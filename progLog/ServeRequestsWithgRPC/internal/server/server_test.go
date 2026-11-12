package server

import (
	"context"
	"net"
	"testing"

	api "Distribuiti/progLog/ServeRequestsWithgRPC/api/v1"
	log "Distribuiti/progLog/ServeRequestsWithgRPC/internal/log"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	scenarios := map[string]func(*testing.T, api.LogClient, *Config){
		"produce and consume succeeds":        testProduceConsume,
		"produce and consume stream succeeds": testProduceConsumeStream,
		"consume past boundary fails":         testConsumePastBoundary,
	}

	for scenario, test := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			test(t, client, config)
		})
	}
}

func setupTest(t *testing.T, configure func(*Config)) (api.LogClient, *Config, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	connection, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)

	directory := t.TempDir()
	commitLog, err := log.NewLog(directory, log.Config{})
	require.NoError(t, err)

	config := &Config{CommitLog: commitLog}
	if configure != nil {
		configure(config)
	}

	grpcServer, err := NewGRPCServer(config)
	require.NoError(t, err)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	client := api.NewLogClient(connection)
	return client, config, func() {
		grpcServer.Stop()
		_ = connection.Close()
		_ = listener.Close()
		_ = commitLog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produced, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	consumed, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produced.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consumed.Record.Value)
	require.Equal(t, want.Offset, consumed.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produced, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})
	require.NoError(t, err)

	consumed, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produced.Offset + 1})
	require.Nil(t, consumed)
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	produceStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)
	for offset, record := range records {
		require.NoError(t, produceStream.Send(&api.ProduceRequest{Record: record}))
		response, err := produceStream.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(offset), response.Offset)
	}

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)
	for offset, record := range records {
		response, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, &api.Record{Value: record.Value, Offset: uint64(offset)}, response.Record)
	}
	_ = consumeStream.CloseSend()
}
