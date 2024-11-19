package broker

import (
	"testing"
	"time"

	logger "github.com/linfeip/gomq/logging"
	"github.com/linfeip/gomq/protocol"
)

func TestBroker(t *testing.T) {
	var b0 = NewBroker("node0", "./node0", "localhost:13000")
	err := b0.Open(true)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	var b1 = NewBroker("node1", "./node1", "localhost:13001")
	err = b1.Open(false)
	if err != nil {
		t.Fatal(err)
	}

	err = b0.Join("node1", "localhost:13001")
	if err != nil {
		t.Fatal(err)
	}

	for {
		time.Sleep(time.Second)

		err := b0.EnqueueTo(0, &protocol.CommitLogPacket{
			Topic:    "testTopic",
			Magic:    1,
			Body:     []byte("HelloBroker"),
			MsgId:    "1",
			Checksum: 1,
		})

		if err != nil {
			t.Fatal(err)
		}

		v, err := b0.Dequeue("testTopic", 0)
		if err != nil {
			t.Fatal(err)
		}

		logger.Infof("dequeue packet: %s queue: %d", v.GetTopic(), v.GetQueue())
	}
}
