package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const serverPassword = "admin"

type WAL struct {
	file    *os.File
	mu      sync.Mutex
	walPath string
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		walPath: path,
	}, nil
}

func (w *WAL) WriteEntry(operation, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := fmt.Sprintf("%s %s %s\n", operation, key, value)
	_, err := w.file.WriteString(entry)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func ReplayWAL(path string, store *Store) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Split(line, " ")

		if len(parts) < 2 {
			continue
		}

		operation := parts[0]
		key := parts[1]

		switch operation {
		case "SET":
			value := ""
			if len(parts) >= 3 {
				value = strings.Join(parts[2:], " ")
			}
			store.mu.Lock()
			store.data[key] = value
			store.mu.Unlock()

		case "DELETE":
			store.mu.Lock()
			delete(store.data, key)
			store.mu.Unlock()
		}
	}

	return scanner.Err()
}

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

func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	authenticated := false

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}

		message := strings.TrimSpace(string(buffer[:n]))
		if message == "" {
			continue
		}

		parts := strings.Fields(message)
		command := strings.ToUpper(parts[0])

		if command == "AUTH" {
			if authenticated {
				conn.Write([]byte("Client Already Authenticated\n"))
				continue
			}

			if len(parts) < 2 {
				conn.Write([]byte("ERROR: AUTH requires a password\n"))
				continue
			}

			if parts[1] == serverPassword {
				authenticated = true
				conn.Write([]byte("OK: Authenticated successfully\n"))
			} else {
				conn.Write([]byte("ERROR: Invalid password\n"))
			}
			continue
		}

		if command == "QUIT" {
			conn.Write([]byte("OK\n"))
			return
		}

		if !authenticated {
			conn.Write([]byte("ERROR: Authentication required. Use 'AUTH <password>'\n"))
			continue
		}

		switch command {

		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: GET requires a key\n"))
				continue
			}
			value, exists := store.Get(parts[1])
			if !exists {
				conn.Write([]byte("ERROR: Key not found\n"))
				continue
			}
			conn.Write([]byte(value + "\n"))

		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: SET requires key and value\n"))
				continue
			}
			key := parts[1]
			val := strings.Join(parts[2:], " ")
			store.Set(key, val)
			conn.Write([]byte("OK\n"))

		case "DELETE":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: DELETE requires a key\n"))
				continue
			}
			store.Delete(parts[1])
			conn.Write([]byte("OK\n"))

		case "EXISTS":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: EXISTS requires a key\n"))
				continue
			}
			_, exists := store.Get(parts[1])
			if !exists {
				conn.Write([]byte("NO\n"))
				continue
			}
			conn.Write([]byte("YES\n"))

		case "KEYS":
			keys := store.getAllKeys()
			if len(keys) == 0 {
				conn.Write([]byte("(empty)\n"))
				continue
			}
			conn.Write([]byte(strings.Join(keys, ",") + "\n"))

		default:
			conn.Write([]byte("ERROR: Unknown command\n"))
		}
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
		go handleConnection(conn, store)
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
			err := replicationManager.AddReplica(follower)
			if err != nil {
				fmt.Printf("Failed to connect to follower %s: %v\n", follower, err)
			}
		}
	} else {
		followerPort := fmt.Sprintf("8%s", port[1:])
		go replicationManager.StartFollower(followerPort)
	}

	startServer(port, nodeID, store)
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
