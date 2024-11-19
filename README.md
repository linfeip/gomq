# 简介

使用golang实现极简的消息队列, 数据传输用的grpc, 数据根据topic分组, 每个组中有多个queue, 每个queue进行顺序消费, 多个queue并发消费

# 特性

- [ ] 高可用
- [ ] 消费订阅
- [ ] 支持Topic*订阅
- [ ] 主从复制
- [ ] 使用gRPC通信
- [ ] 指标收集 (Prometheus)

# 实现多节点主从复制

```go
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
```

# 压测

普通的进行发布消费

```go

func BenchmarkRocket(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := testProducer.SendOneWay(context.Background(), testMsg)
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkGoMQ(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := testGomqPubsub.Publish(context.Background(), testPublishArgs)
			if err != nil {
				panic(err)
			}
		}
	})
}
```

```go
goos: windows
goarch: amd64
pkg: github.com/linfeip/gomq/example
cpu: Intel(R) Core(TM) i7-7700K CPU @ 4.20GHz
BenchmarkRocket
BenchmarkRocket-8   	   77319	     71759 ns/op	    3587 B/op	      61 allocs/op
BenchmarkGoMQ
BenchmarkGoMQ-8     	  201670	     29778 ns/op	    5184 B/op	      94 allocs/op
PASS
```