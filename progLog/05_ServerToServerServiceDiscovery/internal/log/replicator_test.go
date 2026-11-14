package log_test

import (
	"net"
	"testing"
	"time"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"
	chapterlog "Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/log"
	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestReplicatorPullsRecordsAndStops(t *testing.T) {
	source := newTestLog(t)
	target := newTestLog(t)
	sourceServer := startLogServer(t, source)
	targetServer := startLogServer(t, target)

	_, err := source.Append(&api.Record{Value: []byte("from-peer")})
	require.NoError(t, err)

	targetConn, err := grpc.Dial(targetServer, grpc.WithInsecure())
	require.NoError(t, err)
	defer targetConn.Close()
	targetClient := api.NewLogClient(targetConn)

	replicator := &chapterlog.Replicator{LocalServer: targetClient}
	require.NoError(t, replicator.Join("source", sourceServer))
	t.Cleanup(func() { require.NoError(t, replicator.Close()) })

	require.Eventually(t, func() bool {
		record, readErr := target.Read(0)
		return readErr == nil && string(record.Value) == "from-peer"
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, replicator.Leave("source"))
}

func newTestLog(t *testing.T) *chapterlog.Log {
	t.Helper()
	log, err := chapterlog.NewLog(t.TempDir(), chapterlog.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	return log
}

func startLogServer(t *testing.T, log *chapterlog.Log) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer, err := server.NewGRPCServer(&server.Config{CommitLog: log})
	require.NoError(t, err)
	go grpcServer.Serve(listener)
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}
