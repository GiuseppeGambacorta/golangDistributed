package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	"Distribuiti/progLog/08_DeployLocally/internal/agent"
	"Distribuiti/progLog/08_DeployLocally/internal/client"
	raft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSingleNodeBecomesLeaderAndCommits(t *testing.T) {
	node := newNode(t, "node-0", true)
	defer node.Shutdown()

	require.NoError(t, node.WaitForLeader(5*time.Second))
	require.Equal(t, raft.Leader, node.State())

	client, closeClient := newClient(t, node.RPCAddr())
	defer closeClient()
	response, err := client.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("committed")},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), response.Offset)
	read, err := client.Consume(context.Background(), &api.ConsumeRequest{Offset: response.Offset})
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), read.Record.Value)
}

func TestCommittedAppendIsAppliedOnSecondNode(t *testing.T) {
	leader := newNode(t, "node-0", true)
	defer leader.Shutdown()
	require.NoError(t, leader.WaitForLeader(5*time.Second))

	follower := newNode(t, "node-1", false)
	defer follower.Shutdown()
	require.NoError(t, leader.AddVoter("node-1", follower.RaftAddr()))
	require.NoError(t, follower.WaitForLeader(5*time.Second))

	client, closeClient := newClient(t, leader.RPCAddr())
	defer closeClient()
	response, err := client.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("replicated")},
	})
	require.NoError(t, err)

	followerClient, closeFollowerClient := newClient(t, follower.RPCAddr())
	defer closeFollowerClient()
	require.Eventually(t, func() bool {
		read, readErr := followerClient.Consume(
			context.Background(),
			&api.ConsumeRequest{Offset: response.Offset},
		)
		return readErr == nil && string(read.Record.Value) == "replicated"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestDiscoveryAndClientSideRouting(t *testing.T) {
	node := newNode(t, "node-0", true)
	defer node.Shutdown()
	require.NoError(t, node.WaitForLeader(5*time.Second))

	discoveryConn, err := grpc.Dial(node.RPCAddr(), grpc.WithInsecure())
	require.NoError(t, err)
	defer discoveryConn.Close()
	discovered, err := api.NewDiscoveryClient(discoveryConn).GetServers(
		context.Background(), &api.GetServersRequest{},
	)
	require.NoError(t, err)
	require.Len(t, discovered.Servers, 1)
	require.Equal(t, "node-0", discovered.LeaderID)
	require.True(t, discovered.Servers[0].Leader)
	require.Equal(t, node.RPCAddr(), discovered.Servers[0].RPCAddress)

	routedConn, err := client.Dial(context.Background(), node.RPCAddr(), grpc.WithInsecure())
	require.NoError(t, err)
	defer routedConn.Close()
	routed := api.NewLogClient(routedConn)
	response, err := routed.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("routed")},
	})
	require.NoError(t, err)
	read, err := routed.Consume(context.Background(), &api.ConsumeRequest{Offset: response.Offset})
	require.NoError(t, err)
	require.Equal(t, []byte("routed"), read.Record.Value)
}

func TestAdvertisedRaftAddressKeepsStableDNSName(t *testing.T) {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 150 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 10 * time.Millisecond
	node, err := agent.New(agent.Config{
		NodeID:            "node-dns",
		DataDir:           t.TempDir(),
		RPCBindAddr:       "127.0.0.1:0",
		RaftBindAddr:      "127.0.0.1:0",
		RaftAdvertiseAddr: "node-dns.log-headless.default.svc.cluster.local:7400",
		Bootstrap:         true,
		RaftConfig:        config,
	})
	require.NoError(t, err)
	defer node.Shutdown()
	require.Equal(t, "node-dns.log-headless.default.svc.cluster.local:7400", node.RaftAddr())
}

func TestRaftStatusEndpoint(t *testing.T) {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 150 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 10 * time.Millisecond
	node, err := agent.New(agent.Config{
		NodeID:       "node-http",
		DataDir:      t.TempDir(),
		RPCBindAddr:  "127.0.0.1:0",
		RaftBindAddr: "127.0.0.1:0",
		HTTPBindAddr: "127.0.0.1:0",
		Bootstrap:    true,
		RaftConfig:   config,
	})
	require.NoError(t, err)
	defer node.Shutdown()
	require.NoError(t, node.WaitForLeader(5*time.Second))

	response, err := http.Get("http://" + node.HTTPAddr() + "/raftz")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var status struct {
		NodeID string `json:"node_id"`
		State  string `json:"state"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&status))
	require.Equal(t, "node-http", status.NodeID)
	require.Equal(t, "Leader", status.State)
}

func newNode(t *testing.T, id string, bootstrap bool) *agent.Agent {
	t.Helper()
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 150 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 10 * time.Millisecond
	node, err := agent.New(agent.Config{
		NodeID:       id,
		DataDir:      t.TempDir(),
		RPCBindAddr:  "127.0.0.1:0",
		RaftBindAddr: "127.0.0.1:0",
		Bootstrap:    bootstrap,
		RaftConfig:   config,
	})
	require.NoError(t, err)
	return node
}

func newClient(t *testing.T, address string) (api.LogClient, func()) {
	t.Helper()
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	require.NoError(t, err)
	return api.NewLogClient(conn), func() { _ = conn.Close() }
}
