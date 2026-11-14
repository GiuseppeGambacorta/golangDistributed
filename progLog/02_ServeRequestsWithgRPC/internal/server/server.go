package server

import (
	"context"
	"io"

	api "Distribuiti/progLog/02_ServeRequestsWithgRPC/api/v1"

	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	grpcServer := grpc.NewServer()
	server, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(grpcServer, server)
	return grpcServer, nil
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (*grpcServer, error) {
	return &grpcServer{Config: config}, nil
}

func (server *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := server.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (server *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := server.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (server *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		response, err := server.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (server *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		response, err := server.Consume(stream.Context(), req)
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
		req.Offset++
	}
}
