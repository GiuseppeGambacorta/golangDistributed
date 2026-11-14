package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/auth"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/discovery"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/log"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config wires the four pieces of a node. ServerTLSConfig and PeerTLSConfig
// are optional, making local tests and development possible without certs.
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	listener   net.Listener
	localConn  *grpc.ClientConn
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdownLock sync.Mutex
}

// New starts the local log, gRPC server, and Serf membership agent.
func New(config Config) (*Agent, error) {
	a := &Agent{Config: config}
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	for _, setup := range []func() error{
		a.setupLog, a.setupServer, a.setupMembership,
	} {
		if err := setup(); err != nil {
			_ = a.Shutdown()
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.DataDir, log.Config{})
	return err
}

func (a *Agent) setupServer() error {
	var authorizer server.Authorizer
	if a.ACLModelFile != "" || a.ACLPolicyFile != "" {
		loaded, err := auth.New(a.ACLModelFile, a.ACLPolicyFile)
		if err != nil {
			return err
		}
		authorizer = loaded
	}
	options := make([]grpc.ServerOption, 0, 1)
	if a.ServerTLSConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(a.ServerTLSConfig)))
	}
	var err error
	a.server, err = server.NewGRPCServer(&server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}, options...)
	if err != nil {
		return err
	}
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	a.listener, err = net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if serveErr := a.server.Serve(a.listener); serveErr != nil {
			_ = a.Shutdown()
		}
	}()
	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	options := make([]grpc.DialOption, 0, 1)
	if a.PeerTLSConfig != nil {
		options = append(options, grpc.WithTransportCredentials(
			credentials.NewTLS(a.PeerTLSConfig),
		))
	} else {
		options = append(options, grpc.WithInsecure())
	}
	conn, err := grpc.Dial(rpcAddr, options...)
	if err != nil {
		return err
	}
	// The self connection remains open for the lifetime of the agent.
	a.localConn = conn
	localClient := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions:   options,
		LocalServer:   localClient,
		StartOffset:   a.log.NextOffset,
		CurrentOffset: a.log.NextOffset,
	}
	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName:       a.NodeName,
		BindAddr:       a.BindAddr,
		Tags:           map[string]string{"rpc_addr": rpcAddr},
		StartJoinAddrs: a.StartJoinAddrs,
	})
	if err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}

// Shutdown is idempotent and closes membership before the replication and RPC
// resources, so no new pull streams are started while the server is stopping.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	if a.shutdown {
		a.shutdownLock.Unlock()
		return nil
	}
	a.shutdown = true
	a.shutdownLock.Unlock()

	var firstErr error
	if a.membership != nil {
		firstErr = a.membership.Leave()
	}
	if a.replicator != nil {
		if err := a.replicator.Close(); firstErr == nil {
			firstErr = err
		}
		if a.localConn != nil {
			_ = a.localConn.Close()
		}
	}
	if a.server != nil {
		stopped := make(chan struct{})
		go func() {
			a.server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			a.server.Stop()
		}
	}
	if a.listener != nil {
		_ = a.listener.Close()
	}
	if a.log != nil {
		if err := a.log.Close(); firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
