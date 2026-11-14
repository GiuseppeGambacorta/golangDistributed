package discovery_test

import (
	"net"
	"testing"
	"time"

	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/discovery"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

type membershipHandler struct {
	joins  chan string
	leaves chan string
}

func (h *membershipHandler) Join(name, _ string) error {
	select {
	case h.joins <- name:
	default:
	}
	return nil
}

func (h *membershipHandler) Leave(name string) error {
	select {
	case h.leaves <- name:
	default:
	}
	return nil
}

func TestMembershipDiscoversAndRemovesPeers(t *testing.T) {
	firstAddr := freeAddr(t)
	firstHandler := &membershipHandler{
		joins:  make(chan string, 8),
		leaves: make(chan string, 8),
	}
	first, err := discovery.New(firstHandler, discovery.Config{
		NodeName: "node-0",
		BindAddr: firstAddr,
		Tags:     map[string]string{"rpc_addr": "127.0.0.1:10000"},
	})
	require.NoError(t, err)
	defer first.Leave()

	second, err := discovery.New(&membershipHandler{
		joins: make(chan string, 8), leaves: make(chan string, 8),
	}, discovery.Config{
		NodeName:       "node-1",
		BindAddr:       freeAddr(t),
		StartJoinAddrs: []string{firstAddr},
		Tags:           map[string]string{"rpc_addr": "127.0.0.1:10001"},
	})
	require.NoError(t, err)
	defer second.Leave()

	require.Eventually(t, func() bool {
		return len(first.Members()) == 2 && len(firstHandler.joins) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, second.Leave())
	require.Eventually(t, func() bool {
		for _, member := range first.Members() {
			if member.Name == "node-1" && member.Status == serf.StatusLeft {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case name := <-firstHandler.leaves:
			return name == "node-1"
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}
