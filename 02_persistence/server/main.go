package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

const serverPassword = "admin"

// WAL represents a Write-Ahead Log
type WAL struct {
	file    *os.File
	mu      sync.Mutex
	walPath string
}

// NewWAL creates or opens a Write-Ahead Log file
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

// WriteEntry appends an operation to the WAL
func (w *WAL) WriteEntry(operation, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := fmt.Sprintf("%s %s %s\n", operation, key, value)
	_, err := w.file.WriteString(entry)
	if err != nil {
		return err
	}

	// Flush to ensure data is written to disk
	return w.file.Sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	return w.file.Close()
}

// ReplayWAL replays the write-ahead log to rebuild state
func ReplayWAL(path string, store *Store) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL exists yet, that's fine
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Split(line, " ")

		if len(parts) < 2 {
			continue // Skip invalid lines
		}

		operation := parts[0]
		key := parts[1]

		switch operation {
		case "SET":
			value := ""
			if len(parts) >= 3 {
				value = strings.Join(parts[2:], " ")
			}
			// Salvataggio diretto nella mappa interna (senza riscrivere su WAL durante il replay)
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

// Store gestisce la memoria in-RAM protetta da lock e il log WAL su disco
type Store struct {
	data map[string]string
	mu   sync.RWMutex
	wal  *WAL
}

func NewStore(walPath string) (*Store, error) {
	store := &Store{
		data: make(map[string]string),
	}

	// Replay existing WAL if it exists
	if walPath != "" {
		wal, err := NewWAL(walPath)
		if err != nil {
			return nil, err
		}
		store.wal = wal

		// Replay WAL to restore state
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

	// Scrive sul WAL prima di aggiornare lo stato in RAM
	if s.wal != nil {
		s.wal.WriteEntry("SET", key, value)
	}

	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Scrive l'operazione di DELETE sul WAL
	if s.wal != nil {
		s.wal.WriteEntry("DELETE", key, "")
	}

	delete(s.data, key)
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
			fmt.Println("Client disconnesso:", err)
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

func startServer(port string, nodeID string) {
	walPath := fmt.Sprintf("wal-%s.log", nodeID)
	store, err := NewStore(walPath)
	if err != nil {
		fmt.Println("Errore creazione store:", err)
		return
	}
	defer store.Close()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Errore avvio server:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server in ascolto sulla porta %s (Nodo: %s, WAL: %s)\n", port, nodeID, walPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Errore accettazione connessione:", err)
			continue
		}
		go handleConnection(conn, store)
	}
}

func main() {
	// Puoi passare la porta e l'ID del nodo desiderato
	startServer("8080", "node1")
}
