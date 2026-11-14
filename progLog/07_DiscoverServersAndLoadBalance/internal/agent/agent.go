package agent

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	api "Distribuiti/progLog/07_DiscoverServersAndLoadBalance/api/v1"
	"Distribuiti/progLog/07_DiscoverServersAndLoadBalance/internal/log"
	"Distribuiti/progLog/07_DiscoverServersAndLoadBalance/internal/server"
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
	// RPCAddresses maps Raft node IDs to their gRPC addresses. The local
	// address is added automatically; remote addresses are supplied when
	// adding voters.
	RPCAddresses map[string]string

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
	rpcMu     sync.RWMutex
	rpcAddrs  map[string]string
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
	a := &Agent{Config: config, commitLog: commitLog, rpcAddrs: make(map[string]string)}
	for id, address := range config.RPCAddresses {
		a.rpcAddrs[id] = address
	}
	a.rpcAddrs[config.NodeID] = ""
	a.grpc, err = server.NewGRPCServer(&server.Config{
		CommitLog: commitLog,
		Discovery: a.discoveryResponse,
	})
	if err != nil {
		_ = commitLog.Close()
		return nil, err
	}
	a.listener, err = net.Listen("tcp", config.RPCBindAddr)
	if err != nil {
		_ = commitLog.Close()
		return nil, fmt.Errorf("listen for RPC: %w", err)
	}
	a.rpcMu.Lock()
	a.rpcAddrs[config.NodeID] = a.listener.Addr().String()
	a.rpcMu.Unlock()
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

// AddVoterWithRPC adds a Raft voter and records its client-facing address for
// discovery. AddVoter remains available for callers that only need Raft.
func (a *Agent) AddVoterWithRPC(id, raftAddress, rpcAddress string) error {
	if err := a.commitLog.AddVoter(id, raftAddress, 0); err != nil {
		return err
	}
	a.rpcMu.Lock()
	a.rpcAddrs[id] = rpcAddress
	a.rpcMu.Unlock()
	return nil
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

func (a *Agent) discoveryResponse() *api.GetServersResponse {
	configuration := a.commitLog.Raft().GetConfiguration()
	if err := configuration.Error(); err != nil {
		return nil
	}
	leaderAddress := a.commitLog.Leader()
	response := &api.GetServersResponse{LeaderAddress: leaderAddress}
	a.rpcMu.RLock()
	defer a.rpcMu.RUnlock()
	for _, member := range configuration.Configuration().Servers {
		id := string(member.ID)
		rpcAddress := a.rpcAddrs[id]
		if rpcAddress == "" {
			// A node added by an older caller may not have a client address.
			// It is still useful to expose its Raft identity.
			rpcAddress = string(member.Address)
		}
		isLeader := string(member.Address) == leaderAddress
		if isLeader {
			response.LeaderID = id
		}
		response.Servers = append(response.Servers, &api.Server{
			ID:          id,
			RaftAddress: string(member.Address),
			RPCAddress:  rpcAddress,
			Leader:      isLeader,
			Voter:       member.Suffrage == raft.Voter,
		})
	}
	return response
}
