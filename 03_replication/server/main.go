package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type Store struct {
	data               map[string]string
	mu                 sync.RWMutex
	wal                *WAL
	replicationManager *ReplicationManager
}

func NewStore(walPath string) (*Store, error) {
	store := &Store{
		data: make(map[string]string),
	}

	if walPath != "" {
		wal, err := NewWAL(walPath)
		if err != nil {
			return nil, err
		}
		store.wal = wal

		err = ReplayWAL(walPath, store)
		if err != nil {
			return nil, err
		}
	}

	return store, nil
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		s.wal.WriteEntry("SET", key, value)
	}

	s.data[key] = value

	if s.replicationManager != nil && s.replicationManager.role == "leader" {
		s.replicationManager.ReplicateToFollowers("SET", key, value)
	}
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		s.wal.WriteEntry("DELETE", key, "")
	}

	delete(s.data, key)

	if s.replicationManager != nil && s.replicationManager.role == "leader" {
		s.replicationManager.ReplicateToFollowers("DELETE", key, "")
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) getAllKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) Close() {
	if s.wal != nil {
		s.wal.Close()
	}
}

func startServer(port, nodeID string, store *Store) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Error starting server on port %s: %v\n", port, err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnectionAsLeader(conn, store)
	}
}

func startNode(nodeID, port, role string, followers []string) {
	fmt.Printf("Starting %s node %s on port %s\n", role, nodeID, port)

	walPath := fmt.Sprintf("wal-%s.log", nodeID)
	store, err := NewStore(walPath)
	if err != nil {
		fmt.Printf("Error creating store for node %s: %v\n", nodeID, err)
		return
	}
	defer store.Close()

	replicationManager := NewReplicationManager(role, store)
	store.replicationManager = replicationManager

	if role == "leader" {
		for _, follower := range followers {
			time.Sleep(1 * time.Second)
			err := replicationManager.AddFollower(follower)
			if err != nil {
				fmt.Printf("Failed to connect to follower %s: %v\n", follower, err)
			}
		}
		startServer(port, nodeID, store)

	} else {
		followerPort := fmt.Sprintf("8%s", port[1:])
		replicationManager.StartFollowerHandler(followerPort)
	}

}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage:")
		fmt.Println("  go run demo.go <node-id> <port> <role> [follower-addr]")
		fmt.Println("Example:")
		fmt.Println("  go run demo.go node1 8080 leader localhost:8081")
		fmt.Println("  go run demo.go node2 8081 follower")
		return
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	role := os.Args[3]

	var followers []string
	if role == "leader" && len(os.Args) > 4 {
		followers = os.Args[4:]
	}

	startNode(nodeID, port, role, followers)
}
