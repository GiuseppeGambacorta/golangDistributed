package client

import (
	"context"
	"testing"

	api "Distribuiti/progLog/07_DiscoverServersAndLoadBalance/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type fakeSubConn struct{ address resolver.Address }

func (*fakeSubConn) UpdateAddresses([]resolver.Address) {}
func (*fakeSubConn) Connect()                           {}

func TestRoutingPickerRoutesWritesToLeaderAndReadsRoundRobinFollowers(t *testing.T) {
	leader := &fakeSubConn{}
	followerA := &fakeSubConn{}
	followerB := &fakeSubConn{}
	picker := newRoutingPicker([]routingSubConn{
		{sc: leader, state: connectivity.Ready, address: resolver.Address{
			Addr: "leader", Attributes: attributes.New(roleKey{}, true),
		}},
		{sc: followerA, state: connectivity.Ready, address: resolver.Address{
			Addr: "follower-a", Attributes: attributes.New(roleKey{}, false),
		}},
		{sc: followerB, state: connectivity.Ready, address: resolver.Address{
			Addr: "follower-b", Attributes: attributes.New(roleKey{}, false),
		}},
	})

	for i := 0; i < 3; i++ {
		result, err := picker.Pick(balancer.PickInfo{FullMethodName: "/log.v1.Log/Produce"})
		require.NoError(t, err)
		require.Same(t, leader, result.SubConn)
	}
	first, err := picker.Pick(balancer.PickInfo{FullMethodName: "/log.v1.Log/Consume"})
	require.NoError(t, err)
	second, err := picker.Pick(balancer.PickInfo{FullMethodName: "/log.v1.Log/Consume"})
	require.NoError(t, err)
	require.NotSame(t, first.SubConn, second.SubConn)
}

type fakeDiscovery struct {
	response *api.GetServersResponse
}

func (d *fakeDiscovery) GetServers(context.Context, *api.GetServersRequest, ...grpc.CallOption) (*api.GetServersResponse, error) {
	return d.response, nil
}

type fakeClientConn struct {
	resolver.ClientConn
	state   resolver.State
	err     error
	updates chan struct{}
}

func (c *fakeClientConn) UpdateState(state resolver.State) {
	c.state = state
	select {
	case c.updates <- struct{}{}:
	default:
	}
}
func (c *fakeClientConn) ReportError(err error) { c.err = err }
func (c *fakeClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}

func TestResolverPublishesDiscoveredAddresses(t *testing.T) {
	cc := &fakeClientConn{updates: make(chan struct{}, 1)}
	r := &discoveryResolver{
		cc: cc,
		discovery: &fakeDiscovery{response: &api.GetServersResponse{Servers: []*api.Server{
			{ID: "leader", RPCAddress: "127.0.0.1:8400", Leader: true},
			{ID: "follower", RPCAddress: "127.0.0.1:8401"},
		}}},
		interval: defaultPoll,
	}
	r.resolve()
	require.NoError(t, cc.err)
	require.Len(t, cc.state.Addresses, 2)
	require.Equal(t, "127.0.0.1:8400", cc.state.Addresses[0].Addr)
	require.Equal(t, true, cc.state.Addresses[0].Attributes.Value(roleKey{}))
	require.Equal(t, false, cc.state.Addresses[1].Attributes.Value(roleKey{}))
}
