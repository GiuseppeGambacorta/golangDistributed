package log

import (
	"context"
	"fmt"
	"sync"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Replicator pulls a peer's ConsumeStream and appends the received records to
// LocalServer. It intentionally uses the same public gRPC API as clients.
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	// StartOffset, when set, is evaluated for every new peer. Agents use it to
	// start after their local high-water mark; a zero value preserves the
	// chapter's simple "replicate from the beginning" behavior.
	StartOffset func() uint64
	// CurrentOffset is used to ignore records already represented locally.
	// This prevents the normal bidirectional membership notifications from
	// endlessly copying the same record around the cluster.
	CurrentOffset func() uint64

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
	wg      sync.WaitGroup
}

// Join starts replication from addr unless that peer is already being pulled.
func (r *Replicator) Join(name, addr string) error {
	if name == "" || addr == "" {
		return fmt.Errorf("peer name and address are required")
	}
	if r.LocalServer == nil {
		return fmt.Errorf("local server client is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		return nil
	}
	stop := make(chan struct{})
	r.servers[name] = stop
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.replicate(addr, stop)
	}()
	return nil
}

// Leave stops pulling from name. It is idempotent.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if stop, ok := r.servers[name]; ok {
		close(stop)
		delete(r.servers, name)
	}
	return nil
}

// Close stops all peer streams and waits for them to exit.
func (r *Replicator) Close() error {
	r.mu.Lock()
	r.init()
	if !r.closed {
		r.closed = true
		close(r.close)
		for name, stop := range r.servers {
			close(stop)
			delete(r.servers, name)
		}
	}
	r.mu.Unlock()
	r.wg.Wait()
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

type receivedRecord struct {
	record *api.Record
	err    error
}

func (r *Replicator) replicate(addr string, leave <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := r.DialOptions
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithInsecure()}
	}
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer conn.Close()

	client := api.NewLogClient(conn)
	offset := uint64(0)
	if r.StartOffset != nil {
		offset = r.StartOffset()
	}
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: offset})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan receivedRecord, 1)
	go func() {
		for {
			record, recvErr := stream.Recv()
			var value *api.Record
			if record != nil {
				value = record.Record
			}
			select {
			case records <- receivedRecord{record: value, err: recvErr}:
			case <-ctx.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case received := <-records:
			if received.err != nil {
				if ctx.Err() == nil {
					r.logError(received.err, "failed to receive", addr)
				}
				return
			}
			if received.record == nil {
				r.logError(fmt.Errorf("empty record"), "failed to receive", addr)
				return
			}
			if r.CurrentOffset != nil &&
				received.record.Offset < r.CurrentOffset() {
				continue
			}
			if _, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: received.record,
			}); err != nil {
				if ctx.Err() == nil {
					r.logError(err, "failed to produce", addr)
				}
				return
			}
		}
	}
}

func (r *Replicator) logError(err error, message, addr string) {
	r.mu.Lock()
	r.init()
	logger := r.logger
	r.mu.Unlock()
	logger.Error(message, zap.String("addr", addr), zap.Error(err))
}
