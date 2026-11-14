package agent

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"Distribuiti/progLog/06_CoordinateWithConsensus/internal/log"
	"Distribuiti/progLog/06_CoordinateWithConsensus/internal/server"
	raft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type Config struct {
	NodeID string
	// NodeName is retained as a convenient alias for callers of chapter 05.
	NodeName     string
	DataDir      string
	BindAddr     string
	RPCPort      int
	RaftPort     int
	RPCBindAddr  string
	RaftBindAddr string
	Bootstrap    bool

	RaftConfig   *raft.Config
	ApplyTimeout time.Duration
}

type Agent struct {
	Config

	commitLog *log.DistributedLog
	grpc      *grpc.Server
	listener  net.Listener

	closeOnce sync.Once
	closeErr  error
}

func New(config Config) (*Agent, error) {
	if config.NodeID == "" {
		config.NodeID = config.NodeName
	}
	if config.NodeID == "" {
		return nil, errors.New("node ID is required")
	}
	if config.DataDir == "" {
		return nil, errors.New("data directory is required")
	}
	host := config.BindAddr
	if host == "" {
		host = "127.0.0.1"
	} else if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	if config.RPCBindAddr == "" && config.RPCPort != 0 {
		config.RPCBindAddr = fmt.Sprintf("%s:%d", host, config.RPCPort)
	}
	if config.RaftBindAddr == "" && config.RaftPort != 0 {
		config.RaftBindAddr = fmt.Sprintf("%s:%d", host, config.RaftPort)
	}
	if config.RPCBindAddr == "" {
		config.RPCBindAddr = host + ":0"
	}
	if config.RaftBindAddr == "" {
		return nil, errors.New("raft bind address is required")
	}
	commitLog, err := log.NewDistributedLog(log.DistributedConfig{
		DataDir:      config.DataDir,
		NodeID:       config.NodeID,
		RaftBindAddr: config.RaftBindAddr,
		Bootstrap:    config.Bootstrap,
		RaftConfig:   config.RaftConfig,
		ApplyTimeout: config.ApplyTimeout,
	})
	if err != nil {
		return nil, err
	}
	a := &Agent{Config: config, commitLog: commitLog}
	a.grpc, err = server.NewGRPCServer(&server.Config{CommitLog: commitLog})
	if err != nil {
		_ = commitLog.Close()
		return nil, err
	}
	a.listener, err = net.Listen("tcp", config.RPCBindAddr)
	if err != nil {
		_ = commitLog.Close()
		return nil, fmt.Errorf("listen for RPC: %w", err)
	}
	go func() {
		_ = a.grpc.Serve(a.listener)
	}()
	return a, nil
}

func (a *Agent) RPCAddr() string {
	if a.listener == nil {
		return ""
	}
	return a.listener.Addr().String()
}

func (a *Agent) RaftAddr() string {
	return a.commitLog.RaftAddr()
}

func (a *Agent) Leader() string {
	return a.commitLog.Leader()
}

func (a *Agent) State() raft.RaftState {
	return a.commitLog.State()
}

func (a *Agent) WaitForLeader(timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if leader := a.Leader(); leader != "" {
			return nil
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for leader")
		case <-ticker.C:
		}
	}
}

func (a *Agent) AddVoter(id, address string) error {
	return a.commitLog.AddVoter(id, address, 0)
}

func (a *Agent) CommitLog() *log.DistributedLog {
	return a.commitLog
}

func (a *Agent) Shutdown() error {
	a.closeOnce.Do(func() {
		if a.grpc != nil {
			a.grpc.Stop()
		}
		if a.listener != nil {
			_ = a.listener.Close()
		}
		if a.commitLog != nil {
			a.closeErr = a.commitLog.Close()
		}
	})
	return a.closeErr
}
