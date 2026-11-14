package server

import (
	"context"
	"io"
	"sync"
	"time"

	api "Distribuiti/progLog/04_ObserveYourSystems/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

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

var (
	registerViewsOnce sync.Once
	registerViewsErr  error
)

func NewGRPCServer(config *Config, grpcOptions ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOptions := []grpc_zap.Option{
		grpc_zap.WithDurationField(func(duration time.Duration) zapcore.Field {
			return zap.Int64("grpc.time_ns", duration.Nanoseconds())
		}),
	}

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	registerViewsOnce.Do(func() {
		registerViewsErr = view.Register(ocgrpc.DefaultServerViews...)
	})
	if registerViewsErr != nil {
		return nil, registerViewsErr
	}

	grpcOptions = append(grpcOptions,
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(logger, zapOptions...),
			grpc_auth.StreamServerInterceptor(authenticate),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOptions...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	grpcServer := grpc.NewServer(grpcOptions...)
	service, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(grpcServer, service)
	return grpcServer, nil
}

func newGRPCServer(config *Config) (*grpcServer, error) {
	if config == nil || config.CommitLog == nil || config.Authorizer == nil {
		return nil, status.Error(codes.InvalidArgument, "commit log and authorizer are required")
	}
	return &grpcServer{Config: config}, nil
}

func (server *grpcServer) Produce(ctx context.Context, request *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := server.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}
	offset, err := server.CommitLog.Append(request.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (server *grpcServer) Consume(ctx context.Context, request *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := server.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
	record, err := server.CommitLog.Read(request.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (server *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		response, err := server.Produce(stream.Context(), request)
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (server *grpcServer) ConsumeStream(request *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		response, err := server.Consume(stream.Context(), request)
		switch err.(type) {
		case nil:
		case api.ErrOffsetOutOfRange:
			continue
		default:
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
		request.Offset++
	}
}

func authenticate(ctx context.Context) (context.Context, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unknown, "could not find peer info")
	}
	if peerInfo.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "client certificate was not verified")
	}
	return context.WithValue(ctx, subjectContextKey{}, tlsInfo.State.VerifiedChains[0][0].Subject.CommonName), nil
}

func subject(ctx context.Context) string {
	value, _ := ctx.Value(subjectContextKey{}).(string)
	return value
}

type subjectContextKey struct{}
