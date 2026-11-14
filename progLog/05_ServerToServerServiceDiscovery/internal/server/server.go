package server

import (
	"context"
	"io"
	"time"

	api "Distribuiti/progLog/05_ServerToServerServiceDiscovery/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CommitLog is the small persistence surface required by the RPC service.
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

// Authorizer is optional. A nil authorizer is useful for local development and
// tests without certificates; a configured one protects client RPCs as in the
// security chapter.
type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

var _ api.LogServer = (*grpcServer)(nil)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

// NewGRPCServer builds a server but does not bind a listener.
func NewGRPCServer(config *Config, options ...grpc.ServerOption) (*grpc.Server, error) {
	service, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(options...)
	api.RegisterLogServer(server, service)
	return server, nil
}

func newgrpcServer(config *Config) (*grpcServer, error) {
	if config == nil || config.CommitLog == nil {
		return nil, status.Error(codes.InvalidArgument, "commit log is required")
	}
	return &grpcServer{Config: config}, nil
}

// newGRPCServer is retained as a spelling-compatible helper for the previous
// chapter's tests and examples.
func newGRPCServer(config *Config) (*grpcServer, error) {
	return newgrpcServer(config)
}

func (s *grpcServer) Produce(ctx context.Context, request *api.ProduceRequest) (*api.ProduceResponse, error) {
	if request == nil || request.Record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	if err := s.authorize(ctx, produceAction); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(request.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, request *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, consumeAction); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(request.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		response, err := s.Produce(stream.Context(), request)
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(request *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := s.Consume(stream.Context(), request)
		if err == nil {
			if err := stream.Send(response); err != nil {
				return err
			}
			request.Offset++
			continue
		}
		if _, ok := err.(api.ErrOffsetOutOfRange); !ok {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *grpcServer) authorize(ctx context.Context, action string) error {
	if s.Authorizer == nil {
		return nil
	}
	return s.Authorizer.Authorize(subject(ctx), objectWildcard, action)
}

func subject(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 ||
		len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ""
	}
	return tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
}

// AllowAllAuthorizer is convenient when a caller wants to make the policy
// choice explicit while still running without TLS.
type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) Authorize(string, string, string) error { return nil }
