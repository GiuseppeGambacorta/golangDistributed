package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

const serverPassword = "admin"

type Store struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

		// 4. Comandi operativi protetti
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

func startServer(port string) {
	store := NewStore()
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Errore avvio server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server in ascolto sulla porta " + port)

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
	startServer("8080")
}
