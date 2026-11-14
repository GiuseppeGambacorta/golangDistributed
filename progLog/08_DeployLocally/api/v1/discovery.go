package logv1

import "fmt"

// GetServersRequest asks a node for the current Raft membership.
type GetServersRequest struct{}

func (*GetServersRequest) Reset()         {}
func (*GetServersRequest) String() string { return "{}" }
func (*GetServersRequest) ProtoMessage()  {}

// Server describes a Raft member and the address at which its gRPC API is
// reachable. RPCAddress is intentionally separate from RaftAddress.
type Server struct {
	ID          string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	RaftAddress string `protobuf:"bytes,2,opt,name=raft_address,json=raftAddress,proto3" json:"raft_address,omitempty"`
	RPCAddress  string `protobuf:"bytes,3,opt,name=rpc_address,json=rpcAddress,proto3" json:"rpc_address,omitempty"`
	Leader      bool   `protobuf:"varint,4,opt,name=leader,proto3" json:"leader,omitempty"`
	Voter       bool   `protobuf:"varint,5,opt,name=voter,proto3" json:"voter,omitempty"`
}

func (x *Server) Reset()         { *x = Server{} }
func (x *Server) String() string { return fmt.Sprintf("%s (%s)", x.ID, x.RPCAddress) }
func (*Server) ProtoMessage()    {}
func (x *Server) GetID() string {
	if x != nil {
		return x.ID
	}
	return ""
}
func (x *Server) GetRaftAddress() string {
	if x != nil {
		return x.RaftAddress
	}
	return ""
}
func (x *Server) GetRPCAddress() string {
	if x != nil {
		return x.RPCAddress
	}
	return ""
}
func (x *Server) GetLeader() bool { return x != nil && x.Leader }
func (x *Server) GetVoter() bool  { return x != nil && x.Voter }

// GetServersResponse contains a point-in-time view of membership.
type GetServersResponse struct {
	Servers       []*Server `protobuf:"bytes,1,rep,name=servers,proto3" json:"servers,omitempty"`
	LeaderID      string    `protobuf:"bytes,2,opt,name=leader_id,json=leaderId,proto3" json:"leader_id,omitempty"`
	LeaderAddress string    `protobuf:"bytes,3,opt,name=leader_address,json=leaderAddress,proto3" json:"leader_address,omitempty"`
}

func (x *GetServersResponse) Reset()         { *x = GetServersResponse{} }
func (x *GetServersResponse) String() string { return fmt.Sprintf("leader=%s", x.LeaderID) }
func (*GetServersResponse) ProtoMessage()    {}
func (x *GetServersResponse) GetServers() []*Server {
	if x != nil {
		return x.Servers
	}
	return nil
}
func (x *GetServersResponse) GetLeaderID() string {
	if x != nil {
		return x.LeaderID
	}
	return ""
}
func (x *GetServersResponse) GetLeaderAddress() string {
	if x != nil {
		return x.LeaderAddress
	}
	return ""
}
