// Package discovery wraps Serf membership and translates membership events into
// replication handler calls.
package discovery

import (
	"fmt"
	"net"
	"sync"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Handler is notified when a peer joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// Config contains the Serf configuration used by Membership.
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// Membership owns a Serf instance. BindAddr is the Serf/memberlist address;
// the RPC address is conventionally carried in the "rpc_addr" tag.
type Membership struct {
	Config

	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	done    chan struct{}
	logger  *zap.Logger

	closeOnce sync.Once
	leaveOnce sync.Once
	leaveErr  error
}

// New starts a membership agent and joins StartJoinAddrs, if any.
func New(handler Handler, config Config) (*Membership, error) {
	if handler == nil {
		return nil, fmt.Errorf("membership handler is required")
	}
	if config.NodeName == "" {
		return nil, fmt.Errorf("node name is required")
	}
	addr, err := net.ResolveTCPAddr("tcp", config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve membership address: %w", err)
	}

	m := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
		events:  make(chan serf.Event, 32),
		done:    make(chan struct{}),
	}
	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.NodeName = config.NodeName
	serfConfig.Tags = cloneTags(config.Tags)
	serfConfig.EventCh = m.events
	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	m.serf, err = serf.Create(serfConfig)
	if err != nil {
		return nil, fmt.Errorf("create serf membership: %w", err)
	}

	go m.eventHandler()
	if len(config.StartJoinAddrs) > 0 {
		if _, err := m.serf.Join(config.StartJoinAddrs, true); err != nil {
			_ = m.shutdown()
			return nil, fmt.Errorf("join membership: %w", err)
		}
	}
	return m, nil
}

func cloneTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for key, value := range tags {
		out[key] = value
	}
	return out
}

func (m *Membership) eventHandler() {
	for {
		select {
		case <-m.done:
			return
		case event := <-m.events:
			memberEvent, ok := event.(serf.MemberEvent)
			if !ok {
				continue
			}
			switch event.EventType() {
			case serf.EventMemberJoin:
				for _, member := range memberEvent.Members {
					if !m.isLocal(member) {
						m.handleJoin(member)
					}
				}
			case serf.EventMemberLeave, serf.EventMemberFailed:
				for _, member := range memberEvent.Members {
					if !m.isLocal(member) {
						m.handleLeave(member)
					}
				}
			}
		}
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) handleJoin(member serf.Member) {
	addr := member.Tags["rpc_addr"]
	if addr == "" {
		m.logger.Warn("member has no rpc_addr tag", zap.String("name", member.Name))
		return
	}
	if err := m.handler.Join(member.Name, addr); err != nil {
		m.logger.Error("failed to start peer replication", zap.Error(err),
			zap.String("name", member.Name), zap.String("rpc_addr", addr))
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logger.Error("failed to stop peer replication", zap.Error(err),
			zap.String("name", member.Name))
	}
}

// Members returns a snapshot of the current membership.
func (m *Membership) Members() []serf.Member {
	if m == nil || m.serf == nil {
		return nil
	}
	members := m.serf.Members()
	return append([]serf.Member(nil), members...)
}

// Leave announces this node's departure and then shuts down Serf.
func (m *Membership) Leave() error {
	if m == nil || m.serf == nil {
		return nil
	}
	m.leaveOnce.Do(func() {
		m.leaveErr = m.serf.Leave()
		if m.leaveErr == nil {
			m.leaveErr = m.shutdown()
		} else {
			_ = m.shutdown()
		}
	})
	return m.leaveErr
}

func (m *Membership) shutdown() error {
	var err error
	m.closeOnce.Do(func() {
		close(m.done)
		err = m.serf.Shutdown()
	})
	return err
}
