// Package client contains the client-side discovery and routing policy.
package client

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const (
	Scheme      = "raft"
	PolicyName  = "raft_routing"
	defaultPoll = time.Second
)

type roleKey struct{}
type idKey struct{}

func init() {
	resolver.Register(&ResolverBuilder{})
	balancer.Register(&routingBuilder{})
}

// Dial creates a ClientConn whose target is the discovery endpoint. The
// resolver polls that endpoint and the routing policy sends writes to the
// current leader and reads to followers.
func Dial(ctx context.Context, discoveryAddress string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if strings.TrimSpace(discoveryAddress) == "" {
		return nil, fmt.Errorf("discovery address is required")
	}
	target := Scheme + ":///" + url.PathEscape(discoveryAddress)
	options := append([]grpc.DialOption{}, opts...)
	options = append(options, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"`+PolicyName+`"}`))
	return grpc.DialContext(ctx, target, options...)
}

// ResolverBuilder is the gRPC resolver builder. PollInterval may be adjusted
// by applications that register their own builder in tests or deployments.
type ResolverBuilder struct {
	PollInterval time.Duration
}

func (*ResolverBuilder) Scheme() string { return Scheme }

func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, options resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint
	if endpoint == "" {
		endpoint = target.Authority
	}
	endpoint, err := url.PathUnescape(endpoint)
	if err != nil || endpoint == "" {
		return nil, fmt.Errorf("invalid discovery endpoint %q", target.Endpoint)
	}
	interval := b.PollInterval
	if interval <= 0 {
		interval = defaultPoll
	}
	ctx, cancel := context.WithCancel(context.Background())
	dialOptions := make([]grpc.DialOption, 0, 1)
	if options.DialCreds != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(options.DialCreds))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}
	conn, err := grpc.DialContext(ctx, endpoint, dialOptions...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dial discovery endpoint: %w", err)
	}
	r := &discoveryResolver{
		cc:        cc,
		discovery: api.NewDiscoveryClient(conn),
		conn:      conn,
		cancel:    cancel,
		interval:  interval,
		done:      make(chan struct{}),
	}
	go r.poll()
	return r, nil
}

type discoveryResolver struct {
	cc        resolver.ClientConn
	discovery api.DiscoveryClient
	conn      *grpc.ClientConn
	cancel    context.CancelFunc
	interval  time.Duration
	done      chan struct{}
	closeOnce sync.Once
}

func (r *discoveryResolver) ResolveNow(resolver.ResolveNowOptions) { r.resolve() }

func (r *discoveryResolver) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		_ = r.conn.Close()
		close(r.done)
	})
}

func (r *discoveryResolver) poll() {
	r.resolve()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.resolve()
		case <-r.done:
			return
		}
	}
}

func (r *discoveryResolver) resolve() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*r.interval)
	defer cancel()
	response, err := r.discovery.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.cc.ReportError(err)
		return
	}
	addresses := make([]resolver.Address, 0, len(response.Servers))
	for _, server := range response.Servers {
		if server == nil || server.RPCAddress == "" {
			continue
		}
		addresses = append(addresses, resolver.Address{
			Addr: server.RPCAddress,
			Attributes: attributes.New(
				roleKey{}, server.Leader,
				idKey{}, server.ID,
			),
		})
	}
	if len(addresses) == 0 {
		r.cc.ReportError(fmt.Errorf("discovery returned no usable servers"))
		return
	}
	r.cc.UpdateState(resolver.State{Addresses: addresses})
}

type routingBuilder struct{}

func (*routingBuilder) Name() string { return PolicyName }

func (*routingBuilder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	return &routingBalancer{cc: cc, subConns: make(map[string]*routingSubConn)}
}

type routingSubConn struct {
	sc      balancer.SubConn
	address resolver.Address
	state   connectivity.State
}

type routingBalancer struct {
	cc       balancer.ClientConn
	subConns map[string]*routingSubConn
}

func (b *routingBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	next := make(map[string]resolver.Address, len(state.ResolverState.Addresses))
	for _, address := range state.ResolverState.Addresses {
		next[address.Addr] = address
	}
	for address, sub := range b.subConns {
		if _, ok := next[address]; !ok {
			b.cc.RemoveSubConn(sub.sc)
			delete(b.subConns, address)
		}
	}
	for address, resolved := range next {
		if sub, ok := b.subConns[address]; ok {
			sub.address = resolved
			continue
		}
		sc, err := b.cc.NewSubConn([]resolver.Address{resolved}, balancer.NewSubConnOptions{})
		if err != nil {
			continue
		}
		b.subConns[address] = &routingSubConn{sc: sc, address: resolved, state: connectivity.Idle}
		sc.Connect()
	}
	b.updatePicker()
	return nil
}

func (b *routingBalancer) ResolverError(error) { b.updatePicker() }

func (b *routingBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	for _, sub := range b.subConns {
		if sub.sc == sc {
			sub.state = state.ConnectivityState
			break
		}
	}
	b.updatePicker()
}

func (b *routingBalancer) Close() {
	for address, sub := range b.subConns {
		b.cc.RemoveSubConn(sub.sc)
		delete(b.subConns, address)
	}
}

func (b *routingBalancer) updatePicker() {
	ready := make([]routingSubConn, 0, len(b.subConns))
	state := connectivity.TransientFailure
	for _, sub := range b.subConns {
		if sub.state == connectivity.Ready {
			ready = append(ready, *sub)
			state = connectivity.Ready
		} else if sub.state == connectivity.Connecting && state != connectivity.Ready {
			state = connectivity.Connecting
		}
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: state,
		Picker:            newRoutingPicker(ready),
	})
}

type routingPicker struct {
	leader    []balancer.SubConn
	followers []balancer.SubConn
	next      uint32
	mu        sync.Mutex
}

func newRoutingPicker(ready []routingSubConn) balancer.Picker {
	p := &routingPicker{}
	for _, sub := range ready {
		if value := sub.address.Attributes.Value(roleKey{}); value == true {
			p.leader = append(p.leader, sub.sc)
		} else {
			p.followers = append(p.followers, sub.sc)
		}
	}
	return p
}

func (p *routingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	leaderCall := strings.HasSuffix(info.FullMethodName, "/Produce") ||
		strings.HasSuffix(info.FullMethodName, "/ProduceStream")
	if leaderCall {
		if len(p.leader) == 0 {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{SubConn: p.leader[0]}, nil
	}
	pool := p.followers
	if len(pool) == 0 {
		pool = p.leader // A one-node cluster has no followers.
	}
	if len(pool) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	sc := pool[p.next%uint32(len(pool))]
	p.next++
	return balancer.PickResult{SubConn: sc}, nil
}
