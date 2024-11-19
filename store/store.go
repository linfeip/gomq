package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	logger "github.com/linfeip/gomq/logging"
	"github.com/linfeip/gomq/protocol"
)

type Store interface {
	Init() error
	Enqueue(packet *protocol.CommitLogPacket) error
	EnqueueTo(queue int, packet *protocol.CommitLogPacket) error
	Dequeue(topic string, queue int) (*protocol.CommitLogPacket, error)
}

func NewStore(ctx context.Context, storePath string) Store {
	return &store{
		ctx:            ctx,
		topicQueues:    make(map[string]*TopicQueues),
		storePath:      storePath,
		topicsFilename: filepath.Join(storePath, "topics.json"),
	}
}

type store struct {
	ctx            context.Context
	topicQueues    map[string]*TopicQueues
	rw             sync.RWMutex
	storePath      string
	topicsFilename string
}

func (t *store) Init() error {
	var err error
	if err = os.MkdirAll(t.storePath, 0755); err != nil {
		return err
	}

	// 读取topics info数据
	topicFile, err := os.OpenFile(t.topicsFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	topicInfo := &TopicInfo{}
	data, err := io.ReadAll(topicFile)
	if len(data) > 0 {
		err = json.Unmarshal(data, topicInfo)
		if err != nil {
			return err
		}
	}

	// 初始化topicQueues
	for topic, queuesInfo := range topicInfo.Topics {
		queues, err := NewTopicQueues(queuesInfo)
		if err != nil {
			return err
		}
		t.topicQueues[topic] = queues
	}

	go t.workLoop()

	return nil
}

func (t *store) Enqueue(packet *protocol.CommitLogPacket) error {
	queues, err := t.getQueues(packet.Topic)
	if err != nil {
		return err
	}
	return queues.Enqueue(packet)
}

func (t *store) EnqueueTo(queue int, packet *protocol.CommitLogPacket) error {
	queues, err := t.getQueues(packet.Topic)
	if err != nil {
		return err
	}
	return queues.EnqueueTo(queue, packet)
}

func (t *store) Dequeue(topic string, queue int) (*protocol.CommitLogPacket, error) {
	queues, err := t.getQueues(topic)
	if err != nil {
		return nil, err
	}
	return queues.Dequeue(queue)
}

func (t *store) getQueues(topic string) (*TopicQueues, error) {
	t.rw.RLock()
	if queues, ok := t.topicQueues[topic]; ok {
		t.rw.RUnlock()
		return queues, nil
	}
	t.rw.RUnlock()

	// double check
	t.rw.Lock()
	defer t.rw.Unlock()

	if queues, ok := t.topicQueues[topic]; ok {
		return queues, nil
	}

	queues, err := NewTopicQueues(t.defaultQueuesInfo(topic))
	if err != nil {
		return nil, err
	}
	t.topicQueues[topic] = queues
	return queues, nil
}

func (t *store) defaultQueuesInfo(topic string) []*TopicQueueInfo {
	var queues = make([]*TopicQueueInfo, 0, topicQueueNum)
	for i := 0; i < topicQueueNum; i++ {
		q := &TopicQueueInfo{
			Topic:     topic,
			StorePath: t.storePath,
			Queue:     uint32(i),
		}
		q.QueueFileInfos = append(q.QueueFileInfos, &TopicQueueFileInfo{
			Size:     uint64(topicQueueFileSize),
			Filename: filepath.Join(t.storePath, "topics", fmt.Sprintf("%s-%d-%d", topic, i, 0)),
		})
		queues = append(queues, q)
	}
	return queues
}

func (t *store) workLoop() {
	after := time.After(time.Second * 3)
	for {
		select {
		case <-t.ctx.Done():
			// 退出的时候进行一次info落盘
			err := t.FlushTopicsInfo()
			if err != nil {
				logger.Errorf("flush topics info to file error: %v", err)
			}
			return
		case <-after:
			err := t.FlushTopicsInfo()
			if err != nil {
				logger.Errorf("flush topics info to file error: %v", err)
				return
			}
			after = time.After(time.Second * 3)
		}
	}
}

func (t *store) FlushTopicsInfo() error {
	t.rw.RLock()
	topicsInfo := &TopicInfo{
		Topics: make(map[string][]*TopicQueueInfo, len(t.topicQueues)),
	}
	for topic, queues := range t.topicQueues {
		topicsInfo.Topics[topic] = queues.QueuesInfo()
	}
	t.rw.RUnlock()

	data, err := json.Marshal(topicsInfo)
	if err != nil {
		return err
	}

	err = os.WriteFile(t.topicsFilename, data, 0666)
	if err != nil {
		return err
	}
	return nil
}
