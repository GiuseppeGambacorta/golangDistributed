package server

import (
	"context"
	"io"

	api "Distribuiti/progLog/03_SecureYourServices/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
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

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config, options ...grpc.ServerOption) (*grpc.Server, error) {
	options = append(options,
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
	)
	grpcServer := grpc.NewServer(options...)
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
	clientCertificate := tlsInfo.State.VerifiedChains[0][0]
	return context.WithValue(ctx, subjectContextKey{}, clientCertificate.Subject.CommonName), nil
}

func subject(ctx context.Context) string {
	value, _ := ctx.Value(subjectContextKey{}).(string)
	return value
}

type subjectContextKey struct{}
