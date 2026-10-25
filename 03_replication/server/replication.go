package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Replica struct {
	address string
	conn    net.Conn
}

type ReplicationManager struct {
	role       string
	replicas   []*Replica
	store      *Store
	leaderAddr string
	mu         sync.RWMutex
}

func NewReplicationManager(role string, store *Store) *ReplicationManager {
	return &ReplicationManager{
		role:  role,
		store: store,
	}
}

func (rm *ReplicationManager) AddReplica(addr string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	replica := &Replica{
		address: addr,
		conn:    conn,
	}

	rm.replicas = append(rm.replicas, replica)
	fmt.Printf("Added replica: %s\n", addr)
	return nil
}

func (rm *ReplicationManager) ReplicateToFollowers(operation, key, value string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	command := fmt.Sprintf("REPLICATE %s %s %s\n", operation, key, value)

	for _, replica := range rm.replicas {
		go func(r *Replica) {
			_, err := r.conn.Write([]byte(command))
			if err != nil {
				fmt.Printf("Failed to replicate to %s: %v\n", r.address, err)
			}
		}(replica)
	}
}

func (rm *ReplicationManager) StartFollower(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Follower error starting:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Follower listening on port %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Follower error accepting connection:", err)
			continue
		}

		go rm.handleReplicationConnection(conn)
	}
}

func (rm *ReplicationManager) handleReplicationConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		parts := strings.Split(command, " ")

		if len(parts) < 3 {
			continue
		}

		if parts[0] == "REPLICATE" {
			operation := parts[1]
			key := parts[2]
			value := ""
			if len(parts) >= 4 {
				value = strings.Join(parts[3:], " ")
			}

			switch operation {
			case "SET":
				rm.store.Set(key, value)
			case "DELETE":
				rm.store.Delete(key)
			}
		}
	}
}
