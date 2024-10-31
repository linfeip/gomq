package endpoint

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/linfeip/gomq/protocol"
	"github.com/linfeip/gomq/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var testTopicMgr = store.NewTopicManager(context.Background(), "./test")
var testServer = NewServer(":9999", testTopicMgr)
var testClient *grpc.ClientConn
var testPubsub protocol.PubsubServiceClient
var testTopicMsg = &protocol.TopicMessage{
	Topic:    "testTopic",
	MsgId:    "testMsgId",
	Body:     []byte("testBody"),
	Checksum: 123,
}

func init() {
	var err error
	err = testServer.Start(context.Background())
	if err != nil {
		panic(err)
	}
	err = testTopicMgr.Init()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	testClient, err = grpc.NewClient(":9999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	testPubsub = protocol.NewPubsubServiceClient(testClient)
}

func TestGrpc(t *testing.T) {
	go func() {
		for {
			_, err := testPubsub.Publish(context.Background(), &protocol.PublishArgs{
				Message: testTopicMsg,
			})
			if err != nil {
				panic(err)
			}

			fmt.Println("provider publish ok")
			time.Sleep(time.Second)
		}
	}()

	for i := 0; i < 8; i++ {
		consumer, err := testPubsub.Consume(context.Background(), &protocol.ConsumeArgs{
			Topic: testTopicMsg.Topic,
			Queue: int32(i),
		})

		if err != nil {
			t.Fatal(err)
		}

		go func() {
			for {
				reply, err := consumer.Recv()
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("consumer consume topic: %s\n", reply.Message.Topic)
			}
		}()
	}

	select {}
}

func BenchmarkPubsub(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := testPubsub.Publish(context.Background(), &protocol.PublishArgs{
				Message: testTopicMsg,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
