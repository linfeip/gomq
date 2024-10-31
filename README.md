# 简介

使用golang实现极简的消息队列, 数据传输用的grpc, 数据根据topic分组, 每个组中有多个queue, 每个queue进行顺序消费, 多个queue并发消费

# 特性

- [ ] 高可用
- [ ] 消费订阅
- [ ] 支持Topic*订阅
- [ ] 主从复制
- [ ] 使用gRPC通信
- [ ] 指标收集 (Prometheus)

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