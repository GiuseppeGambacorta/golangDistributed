package server

import (
	"context"
	"flag"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	api "Distribuiti/progLog/04_ObserveYourSystems/api/v1"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var debug = flag.Bool("debug", false, "Enable observability output.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServerProducesObservableRPC(t *testing.T) {
	client, teardown := setupTest(t)
	defer teardown()

	response, err := client.Produce(context.Background(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), response.Offset)
}

func setupTest(t *testing.T) (api.LogClient, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	config := &Config{
		CommitLog:  &memoryLog{},
		Authorizer: allowAllAuthorizer{},
	}
	grpcServer, err := NewGRPCServer(config)
	require.NoError(t, err)

	connection, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		metricsPath := metricsFile.Name()
		require.NoError(t, metricsFile.Close())

		tracesFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		tracesPath := tracesFile.Name()
		require.NoError(t, tracesFile.Close())

		t.Logf("metrics log file: %s", metricsPath)
		t.Logf("traces log file: %s", tracesPath)
		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsPath,
			TracesLogFile:     tracesPath,
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, telemetryExporter.Start())
	}

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return api.NewLogClient(connection), func() {
		grpcServer.Stop()
		_ = connection.Close()
		_ = listener.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(string, string, string) error {
	return nil
}

type memoryLog struct {
	mu      sync.Mutex
	records []*api.Record
}

func (log *memoryLog) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	record = &api.Record{Value: append([]byte(nil), record.Value...), Offset: uint64(len(log.records))}
	log.records = append(log.records, record)
	return record.Offset, nil
}

func (log *memoryLog) Read(offset uint64) (*api.Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if offset >= uint64(len(log.records)) {
		return nil, api.ErrOffsetOutOfRange{Offset: offset}
	}
	record := log.records[offset]
	return &api.Record{Value: append([]byte(nil), record.Value...), Offset: record.Offset}, nil
}
