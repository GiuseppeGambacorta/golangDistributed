package logv1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (err ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	grpcStatus := status.New(codes.OutOfRange, fmt.Sprintf("offset out of range: %d", err.Offset))
	detail := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: fmt.Sprintf("The requested offset is outside the log's range: %d", err.Offset),
	}
	withDetails, detailErr := grpcStatus.WithDetails(detail)
	if detailErr != nil {
		return grpcStatus
	}
	return withDetails
}

func (err ErrOffsetOutOfRange) Error() string {
	return err.GRPCStatus().Err().Error()
}
