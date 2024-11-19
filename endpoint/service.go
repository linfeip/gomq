package endpoint

import (
	"context"
	"errors"

	"github.com/linfeip/gomq/protocol"
	"github.com/linfeip/gomq/store"
)

type service struct {
	protocol.UnimplementedPubsubServiceServer
	topicMgr *store.store
}

func (s *service) Publish(ctx context.Context, args *protocol.PublishArgs) (*protocol.PublishReply, error) {
	msg := args.GetMessage()
	if msg == nil {
		return nil, errors.New("args message is nil")
	}
	packet := &protocol.CommitLogPacket{
		Topic:    msg.Topic,
		Body:     msg.Body,
		MsgId:    msg.MsgId,
		Checksum: msg.Checksum,
	}
	err := s.topicMgr.Enqueue(packet)
	if err != nil {
		return nil, err
	}
	return &protocol.PublishReply{}, nil
}

func (s *service) Consume(args *protocol.ConsumeArgs, server protocol.PubsubService_ConsumeServer) error {
	for {
		select {
		case <-server.Context().Done():
			return nil
		default:
			packet, err := s.topicMgr.Dequeue(args.Topic, int(args.Queue))
			if err != nil {
				return err
			}

			err = server.Send(&protocol.ConsumeReply{
				Message: &protocol.TopicMessage{
					Topic:    packet.Topic,
					Body:     packet.Body,
					MsgId:    packet.MsgId,
					Checksum: packet.Checksum,
				},
			})

			if err != nil {
				return err
			}
		}
	}
}
