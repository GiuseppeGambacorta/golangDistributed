package server

import (
	"context"
	"errors"
	"io"
	"time"

	api "Distribuiti/progLog/06_CoordinateWithConsensus/api/v1"
	"Distribuiti/progLog/06_CoordinateWithConsensus/internal/log"
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
}

type grpcServer struct {
	api.UnimplementedLogServer
	commitLog CommitLog
}

func NewGRPCServer(config *Config, options ...grpc.ServerOption) (*grpc.Server, error) {
	if config == nil || config.CommitLog == nil {
		return nil, status.Error(codes.InvalidArgument, "commit log is required")
	}
	server := grpc.NewServer(options...)
	api.RegisterLogServer(server, &grpcServer{commitLog: config.CommitLog})
	return server, nil
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
