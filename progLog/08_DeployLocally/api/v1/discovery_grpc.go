package logv1

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// DiscoveryClient is the client API for membership discovery.
type DiscoveryClient interface {
	GetServers(context.Context, *GetServersRequest, ...grpc.CallOption) (*GetServersResponse, error)
}

type discoveryClient struct{ cc grpc.ClientConnInterface }

func NewDiscoveryClient(cc grpc.ClientConnInterface) DiscoveryClient {
	return &discoveryClient{cc: cc}
}

func (c *discoveryClient) GetServers(ctx context.Context, in *GetServersRequest, opts ...grpc.CallOption) (*GetServersResponse, error) {
	out := new(GetServersResponse)
	if err := c.cc.Invoke(ctx, "/log.v1.Discovery/GetServers", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type DiscoveryServer interface {
	GetServers(context.Context, *GetServersRequest) (*GetServersResponse, error)
	mustEmbedUnimplementedDiscoveryServer()
}

type UnimplementedDiscoveryServer struct{}

func (UnimplementedDiscoveryServer) GetServers(context.Context, *GetServersRequest) (*GetServersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetServers not implemented")
}
func (UnimplementedDiscoveryServer) mustEmbedUnimplementedDiscoveryServer() {}

type UnsafeDiscoveryServer interface {
	mustEmbedUnimplementedDiscoveryServer()
}

func RegisterDiscoveryServer(s *grpc.Server, srv DiscoveryServer) {
	s.RegisterService(&_Discovery_serviceDesc, srv)
}

func _Discovery_GetServers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetServersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).GetServers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/log.v1.Discovery/GetServers"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).GetServers(ctx, req.(*GetServersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Discovery_serviceDesc = grpc.ServiceDesc{
	ServiceName: "log.v1.Discovery",
	HandlerType: (*DiscoveryServer)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "GetServers", Handler: _Discovery_GetServers_Handler}},
	Streams:     []grpc.StreamDesc{},
	Metadata:    "api/v1/discovery.proto",
}
