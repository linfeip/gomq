package endpoint

import (
	"context"
	"net"

	logger "github.com/linfeip/gomq/logging"
	"github.com/linfeip/gomq/protocol"
	"github.com/linfeip/gomq/store"
	"google.golang.org/grpc"
)

func NewServer(addr string, topicMgr *store.TopicManager) *Server {
	return &Server{
		addr:     addr,
		topicMgr: topicMgr,
	}
}

type Server struct {
	addr     string
	topicMgr *store.TopicManager
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	protocol.RegisterPubsubServiceServer(server, &service{topicMgr: s.topicMgr})

	go func() {
		if err := server.Serve(lis); err != nil {
			logger.Fatalf("failed to serve: %v", err)
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
			logger.Infof("grpc server stopped")
		}
	}()

	return nil
}
