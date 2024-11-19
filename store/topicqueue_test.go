package store

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/linfeip/gomq/protocol"
)

var testMgr Store
var testPack = &protocol.CommitLogPacket{
	Topic:    fmt.Sprintf("testTopic"),
	Body:     []byte("Hello GOMQ"),
	Checksum: crc32.ChecksumIEEE([]byte("Hello GOMQ")),
	MsgId:    "testMsgId",
}

func init() {
	var err error
	testMgr = NewStore(context.Background(), "./test")
	err = testMgr.Init()
	if err != nil {
		panic(err)
	}
}

func TestTopicQueue(t *testing.T) {
	// producer
	go func() {
		for {
			err := testMgr.Enqueue(testPack)
			if err != nil {
				panic(err)
			}
			fmt.Println("product topic message")
			time.Sleep(time.Second)
		}
	}()

	for i := 0; i < topicQueueNum; i++ {
		i := i
		go func() {
			for {
				packet, err := testMgr.Dequeue("testTopic", i)
				if err != nil {
					panic(err)
				}
				fmt.Printf("i: %d consume packet: %s\n", i, packet.Topic)
			}
		}()
	}

	select {}
}

func BenchmarkTopicManager_Enqueue(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := testMgr.EnqueueTo(0, testPack)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTopicManager_Dequeue(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := testMgr.Dequeue(testPack.Topic, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
