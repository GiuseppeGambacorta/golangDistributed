package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type address = string

type ReplicationManager struct {
	role       string
	followers  map[address]net.Conn // net.Conn è un'interfaccia, non serve *net.Conn
	store      *Store
	leaderAddr address
	mu         sync.RWMutex
}

func NewReplicationManager(role string, store *Store, leaderAddr address) *ReplicationManager {
	return &ReplicationManager{
		role:       role,
		store:      store,
		leaderAddr: leaderAddr,
		followers:  make(map[address]net.Conn),
	}
}

func (rm *ReplicationManager) findFollowers() {
	localhost := "localhost:"

	for i := 8001; i <= 8100; i++ {
		actualAddress := localhost + strconv.Itoa(i)

		if actualAddress == rm.leaderAddr {
			continue
		}

		rm.mu.RLock()
		_, alreadyTracked := rm.followers[actualAddress]
		rm.mu.RUnlock()

		if alreadyTracked {
			continue
		}

		conn, err := net.DialTimeout("tcp", actualAddress, 150*time.Millisecond)
		if err != nil {
			continue
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			continue
		}

		msg := strings.TrimSpace(string(buf[:n]))
		if msg != "I am a follower" {
			conn.Close()
			continue
		}

		fmt.Printf("Follower %s is up\n", actualAddress)
		rm.mu.Lock()
		rm.followers[actualAddress] = conn
		rm.mu.Unlock()
	}
}

func (rm *ReplicationManager) ReplicateToFollowers(operation, key, value string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	command := fmt.Sprintf("REPLICATE %s %s %s\n", operation, key, value)

	for address, follower := range rm.followers {
		go func(r net.Conn) {
			_, err := r.Write([]byte(command))
			if err != nil {
				fmt.Printf("Failed to replicate to %s: %v, removed\n", address, err)
				delete(rm.followers, address)
			}
		}(follower)
	}
}

func (rm *ReplicationManager) StartFollowerHandler(port string) {
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

		go rm.handleReplicationMessages(conn)
	}
}

func (rm *ReplicationManager) handleReplicationMessages(conn net.Conn) {
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
