package agent_test

import (
	"context"
	"net"
	"testing"
	"time"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/agent"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestAgentsDiscoverAndReplicateWithoutTLS(t *testing.T) {
	firstMembership := freeAddr(t)
	first, err := agent.New(agent.Config{
		NodeName: "agent-0",
		BindAddr: firstMembership,
		RPCPort:  freePort(t),
		DataDir:  t.TempDir(),
	})
	require.NoError(t, err)
	defer first.Shutdown()

	firstAddr, err := first.RPCAddr()
	require.NoError(t, err)
	second, err := agent.New(agent.Config{
		NodeName:       "agent-1",
		BindAddr:       freeAddr(t),
		RPCPort:        freePort(t),
		DataDir:        t.TempDir(),
		StartJoinAddrs: []string{firstMembership},
	})
	require.NoError(t, err)
	defer second.Shutdown()

	conn, err := grpc.Dial(firstAddr, grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()
	client := api.NewLogClient(conn)
	response, err := client.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("replicated")},
	})
	require.NoError(t, err)

	secondAddr, err := second.RPCAddr()
	require.NoError(t, err)
	secondConn, err := grpc.Dial(secondAddr, grpc.WithInsecure())
	require.NoError(t, err)
	defer secondConn.Close()
	secondClient := api.NewLogClient(secondConn)
	require.Eventually(t, func() bool {
		got, readErr := secondClient.Consume(context.Background(),
			&api.ConsumeRequest{Offset: response.Offset})
		return readErr == nil && string(got.Record.Value) == "replicated"
	}, 2*time.Second, 10*time.Millisecond)
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}
