package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/linfeip/gomq/logging"
	"github.com/linfeip/gomq/mmapfile"
	"github.com/linfeip/gomq/protocol"
	"google.golang.org/protobuf/proto"
)

const topicQueueNum = 8

var topicQueueFileSize = 128 * 1024 * 1024 // 128M

// TopicInfo 记录topics数据, 每个topic下有多个queue, 每个queue有多个切割文件
type TopicInfo struct {
	Topics map[string][]*TopicQueueInfo `json:"topics"`
}

type TopicQueueFileInfo struct {
	ReadIndex  uint64 `json:"readIndex"`
	WriteIndex uint64 `json:"writeIndex"`
	Filename   string `json:"filename"`
	Index      uint32 `json:"index"`
	Size       uint64 `json:"size"`
}

type TopicQueueInfo struct {
	Topic           string                `json:"topic"`
	StorePath       string                `json:"storePath"`
	WriteQueueIndex uint32                `json:"writeQueueIndex"`
	ReadQueueIndex  uint32                `json:"readQueueIndex"`
	Queue           uint32                `json:"queue"`
	QueueFileInfos  []*TopicQueueFileInfo `json:"queueFileInfos"`
}

func NewTopicQueue(info *TopicQueueInfo) (*TopicQueue, error) {
	tq := &TopicQueue{
		storePath:       info.StorePath,
		topic:           info.Topic,
		queue:           info.Queue,
		cond:            sync.NewCond(&sync.Mutex{}),
		buffer:          bytes.NewBuffer(make([]byte, 0, 1024)),
		writeQueueIndex: info.WriteQueueIndex,
		readQueueIndex:  info.ReadQueueIndex,
	}

	// 构建MmapFile
	for _, fileInfo := range info.QueueFileInfos {
		mfile, err := mmapfile.NewMmapFile(fileInfo.Filename, fileInfo.Size)
		if err != nil {
			return nil, err
		}
		mfile.Reset(fileInfo.ReadIndex, fileInfo.WriteIndex)
		tq.files = append(tq.files, mfile)
	}

	tq.readFile = tq.files[info.ReadQueueIndex]
	tq.writeFile = tq.files[info.WriteQueueIndex]

	return tq, nil
}

type TopicQueue struct {
	files           []*mmapfile.MmapFile
	writeFile       *mmapfile.MmapFile
	readFile        *mmapfile.MmapFile
	topic           string
	storePath       string
	queue           uint32
	writeQueueIndex uint32
	readQueueIndex  uint32
	cond            *sync.Cond
	buffer          *bytes.Buffer
}

func (t *TopicQueue) QueueInfo() *TopicQueueInfo {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	info := &TopicQueueInfo{
		Topic:           t.topic,
		Queue:           t.queue,
		WriteQueueIndex: t.writeQueueIndex,
		ReadQueueIndex:  t.readQueueIndex,
		StorePath:       t.storePath,
	}

	for i, f := range t.files {
		info.QueueFileInfos = append(info.QueueFileInfos, &TopicQueueFileInfo{
			ReadIndex:  f.ReadIndex(),
			WriteIndex: f.WriteIndex(),
			Filename:   f.Filename(),
			Index:      uint32(i),
			Size:       f.Size(),
		})
	}

	return info
}

func (t *TopicQueue) Enqueue(packet *protocol.CommitLogPacket) error {
	t.cond.L.Lock()
	defer func() {
		t.buffer.Reset()
		t.cond.L.Unlock()
	}()

	err := EncodeBuffer(t.buffer, packet)
	if err != nil {
		return err
	}

	if t.writeFile.Free() < uint64(t.buffer.Len()) {
		// 空间不足, 创建新queue文件
		filename := filepath.Join(t.storePath, "topics", fmt.Sprintf("%s-%d-%d", t.topic, t.queue, t.writeQueueIndex+1))
		newMmapfile, err := mmapfile.NewMmapFile(filename, uint64(topicQueueFileSize))
		if err != nil {
			return err
		}

		t.writeFile.StopWrite()
		t.writeQueueIndex++
		t.files = append(t.files, newMmapfile)
		t.writeFile = newMmapfile
	}

	_, err = t.writeFile.Write(t.buffer.Bytes())
	if err != nil {
		return err
	}

	t.cond.Broadcast()
	return err
}

func (t *TopicQueue) Dequeue() (*protocol.CommitLogPacket, error) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()

	var totalBytes [4]byte
	var err error

	for {
		err = binary.Read(t.readFile, binary.LittleEndian, &totalBytes)
		if errors.Is(err, io.EOF) {
			if t.readFile.IsStopWrite() {
				// 如果当前文件已经读完, 但是已经读到了最后一个写入的文件, 那么就wait等待最新消息写入
				if t.writeQueueIndex <= t.readQueueIndex {
					t.cond.Wait()
				}
				t.readQueueIndex++
				t.readFile = t.files[t.readQueueIndex]
			} else {
				// 当前这个可读文件还没读完, 就等待这个文件的数据写入
				t.cond.Wait()
			}
		} else {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	total := binary.LittleEndian.Uint32(totalBytes[:])
	defer t.buffer.Reset()
	t.buffer.Grow(int(total))
	_, err = io.CopyN(t.buffer, t.readFile, int64(total-4))
	if err != nil {
		return nil, err
	}

	packet := &protocol.CommitLogPacket{}
	err = proto.Unmarshal(t.buffer.Bytes(), packet)
	return packet, err
}

// 根据Topic + queue进行分组, 每个topic-queue是顺序处理加一把锁, 不同queue之间并发处理

func NewTopicQueues(queuesInfo []*TopicQueueInfo) (*TopicQueues, error) {
	tq := &TopicQueues{}
	var err error
	for i, queueInfo := range queuesInfo {
		tq.queues[i], err = NewTopicQueue(queueInfo)
		if err != nil {
			return nil, err
		}
	}
	return tq, nil
}

type TopicQueues struct {
	queues [topicQueueNum]*TopicQueue
	topic  string
	nextId uint32
}

func (t *TopicQueues) Enqueue(packet *protocol.CommitLogPacket) error {
	queueId := atomic.AddUint32(&t.nextId, 1)
	queue := t.queues[queueId%topicQueueNum]
	return queue.Enqueue(packet)
}

func (t *TopicQueues) EnqueueTo(queue int, packet *protocol.CommitLogPacket) error {
	if queue >= len(t.queues) {
		return errors.New("queue num error")
	}
	q := t.queues[queue]
	return q.Enqueue(packet)
}

func (t *TopicQueues) Dequeue(queue int) (*protocol.CommitLogPacket, error) {
	if queue >= len(t.queues) {
		return nil, errors.New("queue num error")
	}
	q := t.queues[queue]
	return q.Dequeue()
}

func (t *TopicQueues) QueuesInfo() []*TopicQueueInfo {
	queuesInfo := make([]*TopicQueueInfo, 0, len(t.queues))
	for _, q := range t.queues {
		queuesInfo = append(queuesInfo, q.QueueInfo())
	}
	return queuesInfo
}

func NewTopicManager(ctx context.Context, storePath string) *TopicManager {
	return &TopicManager{
		ctx:            ctx,
		topicQueues:    make(map[string]*TopicQueues),
		storePath:      storePath,
		topicsFilename: filepath.Join(storePath, "topics.json"),
	}
}

type TopicManager struct {
	ctx            context.Context
	topicQueues    map[string]*TopicQueues
	rw             sync.RWMutex
	storePath      string
	topicsFilename string
}

func (t *TopicManager) Init() error {
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

func (t *TopicManager) Enqueue(packet *protocol.CommitLogPacket) error {
	queues, err := t.getQueues(packet.Topic)
	if err != nil {
		return err
	}
	return queues.Enqueue(packet)
}

func (t *TopicManager) EnqueueTo(queue int, packet *protocol.CommitLogPacket) error {
	queues, err := t.getQueues(packet.Topic)
	if err != nil {
		return err
	}
	return queues.EnqueueTo(queue, packet)
}

func (t *TopicManager) Dequeue(topic string, queue int) (*protocol.CommitLogPacket, error) {
	queues, err := t.getQueues(topic)
	if err != nil {
		return nil, err
	}
	return queues.Dequeue(queue)
}

func (t *TopicManager) getQueues(topic string) (*TopicQueues, error) {
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

func (t *TopicManager) defaultQueuesInfo(topic string) []*TopicQueueInfo {
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

func (t *TopicManager) workLoop() {
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

func (t *TopicManager) FlushTopicsInfo() error {
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

func EncodeBuffer(buffer *bytes.Buffer, packet *protocol.CommitLogPacket) error {
	packBytes, err := proto.Marshal(packet)
	if err != nil {
		return err
	}
	total := 4 + len(packBytes)
	var totalBytes [4]byte
	binary.LittleEndian.PutUint32(totalBytes[:], uint32(total))

	buffer.Write(totalBytes[:])
	buffer.Write(packBytes)
	return nil
}
