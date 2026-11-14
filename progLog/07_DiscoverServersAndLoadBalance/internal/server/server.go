package server

import (
	"context"
	"errors"
	"io"
	"time"

	api "Distribuiti/progLog/07_DiscoverServersAndLoadBalance/api/v1"
	"Distribuiti/progLog/07_DiscoverServersAndLoadBalance/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
	Discovery func() *api.GetServersResponse
}

type grpcServer struct {
	api.UnimplementedLogServer
	api.UnimplementedDiscoveryServer
	commitLog CommitLog
	discovery func() *api.GetServersResponse
}

func NewGRPCServer(config *Config, options ...grpc.ServerOption) (*grpc.Server, error) {
	if config == nil || config.CommitLog == nil {
		return nil, status.Error(codes.InvalidArgument, "commit log is required")
	}
	server := grpc.NewServer(options...)
	implementation := &grpcServer{commitLog: config.CommitLog, discovery: config.Discovery}
	api.RegisterLogServer(server, implementation)
	if config.Discovery != nil {
		api.RegisterDiscoveryServer(server, implementation)
	}
	return server, nil
}

func (s *grpcServer) GetServers(context.Context, *api.GetServersRequest) (*api.GetServersResponse, error) {
	if s.discovery == nil {
		return nil, status.Error(codes.Unimplemented, "discovery is not configured")
	}
	response := s.discovery()
	if response == nil {
		return nil, status.Error(codes.Unavailable, "discovery is not ready")
	}
	return response, nil
}

func (s *grpcServer) Produce(_ context.Context, request *api.ProduceRequest) (*api.ProduceResponse, error) {
	if request == nil || request.Record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	offset, err := s.commitLog.Append(request.Record)
	if err != nil {
		return nil, appendError(err)
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(_ context.Context, request *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	record, err := s.commitLog.Read(request.Offset)
	if err != nil {
		if _, ok := err.(api.ErrOffsetOutOfRange); ok {
			return nil, err
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
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

func appendError(err error) error {
	if errors.Is(err, log.ErrNotLeader) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
