package main

import (
	"net"
	"strings"
)

const serverPassword = "admin"

func handleConnectionAsLeader(conn net.Conn, replicationManager *ReplicationManager) {
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
			value, exists := replicationManager.Get(parts[1])
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
			replicationManager.Set(key, val)
			conn.Write([]byte("OK\n"))

		case "DELETE":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: DELETE requires a key\n"))
				continue
			}
			replicationManager.Delete(parts[1])
			conn.Write([]byte("OK\n"))

		case "EXISTS":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: EXISTS requires a key\n"))
				continue
			}
			if !replicationManager.Exists(parts[1]) {
				conn.Write([]byte("NO\n"))
				continue
			}
			conn.Write([]byte("YES\n"))

		case "KEYS":
			keys := replicationManager.Keys()
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
