package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	logger "github.com/linfeip/gomq/logging"
	"github.com/linfeip/gomq/protocol"
	"github.com/linfeip/gomq/store"
	"google.golang.org/protobuf/proto"
)

func NewBroker(nodeId, raftDir, raftBind string) *Broker {
	ctx := context.Background()
	s := store.NewStore(ctx, raftDir)
	return &Broker{
		ctx:      ctx,
		store:    s,
		nodeId:   nodeId,
		raftDir:  raftDir,
		raftBind: raftBind,
	}
}

// Broker 提供消息存储落地
type Broker struct {
	ctx      context.Context
	raftDir  string
	raftBind string
	nodeId   string
	raft     *raft.Raft
	mu       sync.Mutex
	store    store.Store
}

func (b *Broker) Open(enableSingle bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(b.nodeId)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", b.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(b.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	retainSnapshotCount := 2
	snapshots, err := raft.NewFileSnapshotStore(b.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(b.raftDir, "raft.db"),
	})
	if err != nil {
		return fmt.Errorf("new bbolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, b, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	b.raft = ra

	if err = b.store.Init(); err != nil {
		logger.Errorf("store init error: %v", err)
		return err
	}

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

func (b *Broker) Join(nodeId string, addr string) error {
	logger.Debugf("received join request for remote node %s at %s", nodeId, addr)

	cf := b.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		logger.Errorf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range cf.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeId) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeId) {
				logger.Errorf("node %s at %s already member of cluster, ignoring join request", nodeId, addr)
				return nil
			}

			future := b.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
			}
		}
	}

	f := b.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	logger.Infof("node %s at %s joined successfully", nodeId, addr)

	return nil
}

func (b *Broker) Enqueue(packet *protocol.CommitLogPacket) error {
	if b.raft.State() != raft.Leader {
		return errors.New("not leader")
	}

	c := &protocol.Command{
		Op:     protocol.CommandOp_Enqueue,
		Packet: packet,
	}

	data, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := b.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (b *Broker) EnqueueTo(queue int, packet *protocol.CommitLogPacket) error {
	if b.raft.State() != raft.Leader {
		return errors.New("not leader")
	}

	packet.Queue = int32(queue)
	c := &protocol.Command{
		Op:     protocol.CommandOp_EnqueueTo,
		Packet: packet,
	}

	data, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := b.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (b *Broker) Dequeue(topic string, queue int) (*protocol.CommitLogPacket, error) {
	if b.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}

	c := &protocol.Command{
		Op: protocol.CommandOp_Dequeue,
		DequeueArgs: &protocol.DequeueArgs{
			Queue: int32(queue),
			Topic: topic,
		},
	}

	data, err := proto.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := b.raft.Apply(data, 10*time.Second)
	if f.Error() != nil {
		return nil, f.Error()
	}
	return f.Response().(*protocol.CommitLogPacket), nil
}

func (b *Broker) Apply(log *raft.Log) interface{} {
	var c protocol.Command
	if err := proto.Unmarshal(log.Data, &c); err != nil {
		panic(err)
	}

	switch c.Op {
	case protocol.CommandOp_Enqueue:
		if err := b.store.Enqueue(c.GetPacket()); err != nil {
			panic(err)
		}
		return nil
	case protocol.CommandOp_EnqueueTo:
		if err := b.store.EnqueueTo(int(c.GetPacket().GetQueue()), c.GetPacket()); err != nil {
			panic(err)
		}
		return nil
	case protocol.CommandOp_Dequeue:
		args := c.GetDequeueArgs()
		v, err := b.store.Dequeue(args.GetTopic(), int(args.GetQueue()))
		if err != nil {
			panic(err)
		}
		return v
	default:
		panic("unknown command")
	}
}

func (b *Broker) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (b *Broker) Restore(snapshot io.ReadCloser) error {
	return nil
}
