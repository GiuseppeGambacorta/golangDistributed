package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func startServer(port, nodeID string, replicationManager *ReplicationManager) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Error starting server on port %s: %v\n", port, err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %s\n", port)
	go replicationManager.findFollowers()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnectionAsLeader(conn, replicationManager)
	}
}

func startNode(nodeID, port, role string) {
	fmt.Printf("Starting %s node %s on port %s\n", role, nodeID, port)

	walPath := fmt.Sprintf("wal-%s.log", nodeID)
	store, err := NewStore(walPath)
	if err != nil {
		fmt.Printf("Error creating store for node %s: %v\n", nodeID, err)
		return
	}
	defer store.Close()

	replicationManager := NewReplicationManager(role, store, "localhost:8080")

	if role == "leader" {
		startServer(port, nodeID, replicationManager)

	} else {
		followerPort := fmt.Sprintf("8%s", port[1:])
		replicationManager.StartFollowerHandler(followerPort)
	}

}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  go run . node1 leader")
		fmt.Println("  go run . node2 8081")
		return
	}

	nodeID := os.Args[1]
	secondArgoment := os.Args[2]
	if secondArgoment == "leader" {
		startNode(nodeID, "8000", secondArgoment)
	}

	port, err := strconv.Atoi(secondArgoment)
	if err == nil {
		if port >= 8001 && port <= 8100 {
			startNode(nodeID, secondArgoment, secondArgoment)
		} else {
			fmt.Println("Invalid port, must be in 8001 and <= 8100")
			return
		}

	}

}
