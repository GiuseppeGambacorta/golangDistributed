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

const pingInterval = time.Duration(2) * time.Second

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

func (rm *ReplicationManager) Set(key, value string) {
	rm.store.Set(key, value)
	if rm.role == "leader" {
		rm.ReplicateToFollowers("SET", key, value)
	}
}

func (rm *ReplicationManager) Delete(key string) {
	rm.store.Delete(key)
	if rm.role == "leader" {
		rm.ReplicateToFollowers("DELETE", key, "")
	}
}

func (rm *ReplicationManager) Get(key string) (string, bool) {
	return rm.store.Get(key)
}

func (rm *ReplicationManager) Exists(key string) bool {
	_, exists := rm.store.Get(key)
	return exists
}

func (rm *ReplicationManager) Keys() []string {
	return rm.store.getAllKeys()
}

func (rm *ReplicationManager) pingFollower(conn net.Conn) bool {
	conn.SetDeadline(time.Now().Add(150 * time.Millisecond))
	defer conn.SetDeadline(time.Time{})

	if _, err := conn.Write([]byte("PING\n")); err != nil {
		return false
	}

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(resp) != "PONG" {
		return false
	}

	return true
}

func (rm *ReplicationManager) findFollowers() {
	localhost := "localhost:"
	fmt.Printf("Leader %s: starting discovery service every %v\n", rm.leaderAddr, pingInterval)

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Printf("Leader %s: starting discovery service\n", rm.leaderAddr)
		for i := 8001; i <= 8100; i++ {
			actualAddress := localhost + strconv.Itoa(i)

			if actualAddress == rm.leaderAddr {
				continue
			}

			rm.mu.RLock()
			existingConn, alreadyTracked := rm.followers[actualAddress]
			rm.mu.RUnlock()

			if alreadyTracked {
				// Controllo di salute (Heartbeat) sulla connessione esistente
				if !rm.pingFollower(existingConn) {
					fmt.Printf("Follower %s non risponde al ping: rimosso\n", actualAddress)
					rm.mu.Lock()
					existingConn.Close()
					delete(rm.followers, actualAddress)
					rm.mu.Unlock()
				}
				continue
			}

			conn, err := net.DialTimeout("tcp", actualAddress, 100*time.Millisecond)
			if err != nil {
				continue
			}

			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

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

			// 3. Reset deadline per l'uso futuro del socket
			conn.SetDeadline(time.Time{})

			fmt.Printf("Follower %s is up\n", actualAddress)
			rm.mu.Lock()
			rm.followers[actualAddress] = conn
			rm.mu.Unlock()
		}
	}
}

func (rm *ReplicationManager) ReplicateToFollowers(operation, key, value string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	command := fmt.Sprintf("REPLICATE %s %s %s\n", operation, key, value)

	for address, follower := range rm.followers {
		go func(address string, r net.Conn) {
			_, err := r.Write([]byte(command))
			if err != nil {
				fmt.Printf("Failed to replicate to %s: %v, removed\n", address, err)
				rm.mu.Lock()
				defer rm.mu.Unlock()
				delete(rm.followers, address)
			}
		}(address, follower)
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

		_, err = conn.Write([]byte("I am a follower\n"))
		if err != nil {
			fmt.Println("Follower error sending handshake:", err)
			conn.Close()
			continue
		}

		// aspetto lunghezza log, se log < aspetto aggiornamento e rileggo tutto. di la, mi devo fermare

		go rm.handleReplicationMessages(conn)
	}
}

func (rm *ReplicationManager) handleReplicationMessages(conn net.Conn) {
	defer conn.Close()

	timeoutDuration := pingInterval * 2

	conn.SetReadDeadline(time.Now().Add(timeoutDuration))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// 2. Rinnova la scadenza ogni volta che arriva un messaggio
		conn.SetReadDeadline(time.Now().Add(timeoutDuration))

		command := strings.TrimSpace(scanner.Text())
		parts := strings.Split(command, " ")

		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		if parts[0] == "PING" {
			conn.Write([]byte("PONG\n"))
			continue
		}

		if parts[0] == "REPLICATE" && len(parts) >= 3 {
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

	if err := scanner.Err(); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			fmt.Println("Leader dichiarato MORTO: nessun messaggio/PING ricevuto entro il timeout!")
		} else {
			fmt.Printf("Connessione con il leader interrotta: %v\n", err)
		}
	} else {
		fmt.Println("Connessione chiusa dal leader (EOF)")
	}
}
