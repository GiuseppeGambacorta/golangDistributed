package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	"Distribuiti/progLog/08_DeployLocally/internal/log"
	"Distribuiti/progLog/08_DeployLocally/internal/server"
	raft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type Config struct {
	NodeID string `json:"node_id"`
	// NodeName is retained as a convenient alias for callers of chapter 05.
	NodeName          string `json:"node_name"`
	DataDir           string `json:"data_dir"`
	BindAddr          string `json:"bind_addr"`
	RPCPort           int    `json:"rpc_port"`
	RaftPort          int    `json:"raft_port"`
	RPCBindAddr       string `json:"rpc_bind_addr"`
	RaftBindAddr      string `json:"raft_bind_addr"`
	RPCAdvertiseAddr  string `json:"rpc_advertise_addr"`
	RaftAdvertiseAddr string `json:"raft_advertise_addr"`
	HTTPBindAddr      string `json:"http_bind_addr"`
	Bootstrap         bool   `json:"bootstrap"`
	// RPCAddresses maps Raft node IDs to their gRPC addresses. The local
	// address is added automatically; remote addresses are supplied when
	// adding voters.
	RPCAddresses map[string]string `json:"rpc_addresses"`

	RaftConfig   *raft.Config  `json:"-"`
	ApplyTimeout time.Duration `json:"apply_timeout"`

	JoinID       string `json:"join_id"`
	JoinRaftAddr string `json:"join_raft_addr"`
	JoinRPCAddr  string `json:"join_rpc_addr"`
	JoinAddress  string `json:"join_address"`
}

type Agent struct {
	Config

	commitLog *log.DistributedLog
	grpc      *grpc.Server
	listener  net.Listener
	http      *http.Server
	httpAddr  string

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
		DataDir:           config.DataDir,
		NodeID:            config.NodeID,
		RaftBindAddr:      config.RaftBindAddr,
		RaftAdvertiseAddr: config.RaftAdvertiseAddr,
		Bootstrap:         config.Bootstrap,
		RaftConfig:        config.RaftConfig,
		ApplyTimeout:      config.ApplyTimeout,
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
	a.rpcAddrs[config.NodeID] = config.RPCAdvertiseAddr
	if a.rpcAddrs[config.NodeID] == "" {
		a.rpcAddrs[config.NodeID] = a.listener.Addr().String()
	}
	a.rpcMu.Unlock()
	go func() {
		_ = a.grpc.Serve(a.listener)
	}()
	if config.HTTPBindAddr != "" {
		httpListener, listenErr := net.Listen("tcp", config.HTTPBindAddr)
		if listenErr != nil {
			_ = a.Shutdown()
			return nil, fmt.Errorf("listen for HTTP: %w", listenErr)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", a.health)
		mux.HandleFunc("/readyz", a.ready)
		mux.HandleFunc("/raftz", a.raftStatus)
		mux.HandleFunc("/join", a.join)
		a.http = &http.Server{Handler: mux}
		a.httpAddr = httpListener.Addr().String()
		go func() { _ = a.http.Serve(httpListener) }()
	}
	return a, nil
}

func (a *Agent) RPCAddr() string {
	if a.RPCAdvertiseAddr != "" {
		return a.RPCAdvertiseAddr
	}
	if a.listener == nil {
		return ""
	}
	return a.listener.Addr().String()
}

func (a *Agent) HTTPAddr() string { return a.httpAddr }

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
		if a.http != nil {
			_ = a.http.Close()
		}
		if a.commitLog != nil {
			a.closeErr = a.commitLog.Close()
		}

	})
	return a.closeErr
}

type joinRequest struct {
	ID          string `json:"id"`
	RaftAddress string `json:"raft_address"`
	RPCAddress  string `json:"rpc_address"`
}

func (a *Agent) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (a *Agent) ready(w http.ResponseWriter, _ *http.Request) {
	if a.Leader() == "" {
		http.Error(w, "leader is not known", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

type raftStatusResponse struct {
	NodeID        string             `json:"node_id"`
	State         string             `json:"state"`
	LeaderAddress string             `json:"leader_address,omitempty"`
	LeaderID      string             `json:"leader_id,omitempty"`
	RaftAddress   string             `json:"raft_address"`
	Servers       []raftServerStatus `json:"servers"`
	Stats         map[string]string  `json:"stats"`
}

type raftServerStatus struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

func (a *Agent) raftStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	configuration := a.commitLog.Raft().GetConfiguration()
	if err := configuration.Error(); err != nil {
		http.Error(w, "raft configuration unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	leaderAddress, leaderID := a.commitLog.Raft().LeaderWithID()
	response := raftStatusResponse{
		NodeID:        a.NodeID,
		State:         a.State().String(),
		LeaderAddress: string(leaderAddress),
		LeaderID:      string(leaderID),
		RaftAddress:   a.RaftAddr(),
		Servers:       make([]raftServerStatus, 0, len(configuration.Configuration().Servers)),
		Stats:         a.commitLog.Raft().Stats(),
	}
	for _, server := range configuration.Configuration().Servers {
		response.Servers = append(response.Servers, raftServerStatus{
			ID:       string(server.ID),
			Address:  string(server.Address),
			Suffrage: server.Suffrage.String(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		return
	}
}

func (a *Agent) join(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var request joinRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid join request", http.StatusBadRequest)
		return
	}
	if err := a.AddVoterWithRPC(request.ID, request.RaftAddress, request.RPCAddress); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, log.ErrNotLeader) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// Join asks a leader's HTTP admin endpoint to add this node to the Raft
// configuration. It is intentionally explicit so Kubernetes DNS addresses
// are never inferred from ephemeral listener addresses.
func (a *Agent) Join(ctx context.Context) error {
	if a.JoinAddress == "" || a.JoinID == "" || a.JoinRaftAddr == "" || a.JoinRPCAddr == "" {
		return errors.New("join address, ID, raft address, and RPC address are required")
	}
	payload, err := json.Marshal(joinRequest{ID: a.JoinID, RaftAddress: a.JoinRaftAddr, RPCAddress: a.JoinRPCAddr})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.JoinAddress, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("join request failed (%s): %s", response.Status, bytes.TrimSpace(message))
	}
	return nil
}

// LoadConfigFile reads the JSON configuration accepted by cmd/agent.
func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	return config, nil
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
