package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	api "Distribuiti/progLog/06_CoordinateWithConsensus/api/v1"
	raft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNotLeader is returned when a write is sent to a follower. Clients can
	// use the leader address from Agent.Leader to retry the request.
	ErrNotLeader  = errors.New("raft node is not the leader")
	ErrNoResponse = errors.New("raft apply returned no response")
)

const (
	defaultApplyTimeout = 10 * time.Second
	defaultSnapshotKeep = 2
	maxSnapshotRecord   = 64 << 20
)

// Config contains the files and network identity needed by a Raft node.
// RaftBindAddr must be reachable by the other nodes in the cluster.
type DistributedConfig struct {
	DataDir      string
	NodeID       string
	RaftBindAddr string
	Bootstrap    bool

	// RaftConfig is optional. It is primarily useful to shorten timeouts in
	// tests; LocalID and Logger are always set by NewDistributedLog.
	RaftConfig   *raft.Config
	ApplyTimeout time.Duration
}

// DistributedLog submits appends to Raft and uses localLog as its state
// machine. Reads are served locally because committed entries are applied in
// order on every member.
type DistributedLog struct {
	localLog  *Log
	raft      *raft.Raft
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport

	applyTimeout time.Duration
	closeOnce    sync.Once
	closeErr     error
}

func NewDistributedLog(config DistributedConfig) (*DistributedLog, error) {
	if config.DataDir == "" {
		return nil, errors.New("data directory is required")
	}
	if config.NodeID == "" {
		return nil, errors.New("node ID is required")
	}
	if config.RaftBindAddr == "" {
		return nil, errors.New("raft bind address is required")
	}
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, err
	}
	localDir := filepath.Join(config.DataDir, "log")
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return nil, err
	}
	localLog, err := NewLog(localDir, Config{})
	if err != nil {
		return nil, fmt.Errorf("open local log: %w", err)
	}

	raftDir := filepath.Join(config.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		_ = localLog.Close()
		return nil, err
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		_ = localLog.Close()
		return nil, fmt.Errorf("open raft store: %w", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(raftDir, "snapshots"),
		defaultSnapshotKeep,
		io.Discard,
	)
	if err != nil {
		_ = store.Close()
		_ = localLog.Close()
		return nil, fmt.Errorf("open snapshot store: %w", err)
	}
	transport, err := raft.NewTCPTransport(
		config.RaftBindAddr,
		nil,
		3,
		10*time.Second,
		io.Discard,
	)
	if err != nil {
		_ = store.Close()
		_ = localLog.Close()
		return nil, fmt.Errorf("open raft transport: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	if config.RaftConfig != nil {
		*raftConfig = *config.RaftConfig
	}
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	fsm := &fsm{localLog: localLog}
	r, err := raft.NewRaft(
		raftConfig,
		fsm,
		store,
		store,
		snapshotStore,
		transport,
	)
	if err != nil {
		_ = transport.Close()
		_ = store.Close()
		_ = localLog.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	d := &DistributedLog{
		localLog:     localLog,
		raft:         r,
		store:        store,
		transport:    transport,
		applyTimeout: config.ApplyTimeout,
	}
	if d.applyTimeout <= 0 {
		d.applyTimeout = defaultApplyTimeout
	}
	if config.Bootstrap {
		future := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(config.NodeID),
			Address: transport.LocalAddr(),
		}}})
		if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			_ = d.Close()
			return nil, fmt.Errorf("bootstrap raft: %w", err)
		}
	}
	return d, nil
}

// Append commits a record on a Raft majority before returning its local offset.
func (d *DistributedLog) Append(record *api.Record) (uint64, error) {
	if record == nil {
		return 0, errors.New("record is required")
	}
	if d.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}
	data, err := proto.Marshal(&api.Record{Value: append([]byte(nil), record.Value...)})
	if err != nil {
		return 0, err
	}
	future := d.raft.Apply(data, d.applyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, ErrNotLeader
		}
		return 0, err
	}
	response, ok := future.Response().(applyResponse)
	if !ok {
		return 0, ErrNoResponse
	}
	if response.err != nil {
		return 0, response.err
	}
	return response.offset, nil
}

func (d *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return d.localLog.Read(offset)
}

func (d *DistributedLog) NextOffset() uint64 {
	return d.localLog.NextOffset()
}

func (d *DistributedLog) Leader() string {
	return string(d.raft.Leader())
}

func (d *DistributedLog) State() raft.RaftState {
	return d.raft.State()
}

func (d *DistributedLog) Raft() *raft.Raft {
	return d.raft
}

func (d *DistributedLog) RaftAddr() string {
	return string(d.transport.LocalAddr())
}

func (d *DistributedLog) AddVoter(id, address string, previousIndex uint64) error {
	if id == "" || address == "" {
		return errors.New("voter ID and address are required")
	}
	return d.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), previousIndex, d.applyTimeout).Error()
}

func (d *DistributedLog) RemoveServer(id string, previousIndex uint64) error {
	return d.raft.RemoveServer(raft.ServerID(id), previousIndex, d.applyTimeout).Error()
}

func (d *DistributedLog) Close() error {
	d.closeOnce.Do(func() {
		if d.raft != nil {
			d.closeErr = d.raft.Shutdown().Error()
		}
		if d.transport != nil {
			if err := d.transport.Close(); d.closeErr == nil {
				d.closeErr = err
			}
		}
		if d.store != nil {
			if err := d.store.Close(); d.closeErr == nil {
				d.closeErr = err
			}
		}
		if d.localLog != nil {
			if err := d.localLog.Close(); d.closeErr == nil {
				d.closeErr = err
			}
		}
	})
	return d.closeErr
}

type applyResponse struct {
	offset uint64
	err    error
}

type fsm struct {
	localLog *Log
}

func (f *fsm) Apply(entry *raft.Log) interface{} {
	record := &api.Record{}
	if err := proto.Unmarshal(entry.Data, record); err != nil {
		return applyResponse{err: err}
	}
	offset, err := f.localLog.Append(record)
	return applyResponse{offset: offset, err: err}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	next := f.localLog.NextOffset()
	lowest, err := f.localLog.LowestOffset()
	if err != nil {
		return nil, err
	}
	records := make([]*api.Record, 0, next-lowest)
	for offset := lowest; offset < next; offset++ {
		record, err := f.localLog.Read(offset)
		if err != nil {
			return nil, err
		}
		records = append(records, proto.Clone(record).(*api.Record))
	}
	return &snapshot{records: records}, nil
}

func (f *fsm) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	if err := f.localLog.Reset(); err != nil {
		return err
	}
	var count uint64
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		var size uint64
		if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
			return err
		}
		if size > maxSnapshotRecord {
			return fmt.Errorf("snapshot record is too large: %d bytes", size)
		}
		data := make([]byte, size)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		record := &api.Record{}
		if err := proto.Unmarshal(data, record); err != nil {
			return err
		}
		if _, err := f.localLog.Append(record); err != nil {
			return err
		}
	}
	return nil
}

type snapshot struct {
	records []*api.Record
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	writeErr := func(err error) error {
		_ = sink.Cancel()
		return err
	}
	if err := binary.Write(sink, binary.BigEndian, uint64(len(s.records))); err != nil {
		return writeErr(err)
	}
	for _, record := range s.records {
		data, err := proto.Marshal(record)
		if err != nil {
			return writeErr(err)
		}
		if len(data) > maxSnapshotRecord {
			return writeErr(fmt.Errorf("snapshot record is too large: %d bytes", len(data)))
		}
		if err := binary.Write(sink, binary.BigEndian, uint64(len(data))); err != nil {
			return writeErr(err)
		}
		if _, err := sink.Write(data); err != nil {
			return writeErr(err)
		}
	}
	if err := sink.Close(); err != nil {
		return err
	}
	return nil
}

func (*snapshot) Release() {}
