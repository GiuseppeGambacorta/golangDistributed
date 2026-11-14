package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Distribuiti/progLog/06_CoordinateWithConsensus/internal/agent"
)

func main() {
	var config agent.Config
	flag.StringVar(&config.NodeID, "node-id", "node-0", "stable Raft node ID")
	flag.StringVar(&config.DataDir, "data", "./data/node-0", "node data directory")
	flag.StringVar(&config.RPCBindAddr, "rpc-addr", "127.0.0.1:8400", "gRPC listen address")
	flag.StringVar(&config.RaftBindAddr, "raft-addr", "127.0.0.1:7400", "Raft transport address")
	flag.BoolVar(&config.Bootstrap, "bootstrap", false, "bootstrap a new one-node cluster")
	flag.Parse()

	node, err := agent.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Shutdown()
	log.Printf("node %s: gRPC=%s raft=%s", config.NodeID, node.RPCAddr(), node.RaftAddr())
	if config.Bootstrap {
		log.Printf("waiting for leader: %v", node.WaitForLeader(15*time.Second))
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}
