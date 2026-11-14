package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"Distribuiti/progLog/08_DeployLocally/internal/agent"
)

func main() {
	config := agent.Config{}
	configPath := configArgument(os.Args[1:])
	if configPath != "" {
		loaded, err := agent.LoadConfigFile(configPath)
		if err != nil {
			log.Fatal(err)
		}
		config = loaded
	}
	flag.String("config", configPath, "JSON configuration file")

	flag.StringVar(&config.NodeID, "node-id", valueOr(config.NodeID, "node-0"), "stable Raft node ID")
	flag.StringVar(&config.DataDir, "data", valueOr(config.DataDir, "./data/node-0"), "persistent node data directory")
	flag.StringVar(&config.RPCBindAddr, "rpc-addr", valueOr(config.RPCBindAddr, "0.0.0.0:8400"), "gRPC listen address")
	flag.StringVar(&config.RPCAdvertiseAddr, "rpc-advertise", config.RPCAdvertiseAddr, "gRPC address published to clients")
	flag.StringVar(&config.RaftBindAddr, "raft-addr", valueOr(config.RaftBindAddr, "0.0.0.0:7400"), "Raft listen address")
	flag.StringVar(&config.RaftAdvertiseAddr, "raft-advertise", config.RaftAdvertiseAddr, "stable Raft address published to peers")
	flag.StringVar(&config.HTTPBindAddr, "http-addr", valueOr(config.HTTPBindAddr, "0.0.0.0:8080"), "health/admin HTTP listen address")
	flag.BoolVar(&config.Bootstrap, "bootstrap", config.Bootstrap, "bootstrap a new one-node cluster")
	flag.StringVar(&config.JoinID, "join-id", config.JoinID, "ID to add when joining an existing cluster")
	flag.StringVar(&config.JoinRaftAddr, "join-raft-addr", config.JoinRaftAddr, "Raft address to add when joining")
	flag.StringVar(&config.JoinRPCAddr, "join-rpc-addr", config.JoinRPCAddr, "gRPC address to add when joining")
	flag.StringVar(&config.JoinAddress, "join-address", config.JoinAddress, "leader HTTP /join endpoint")
	flag.Parse()

	node, err := agent.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Shutdown()
	log.Printf("node %s: gRPC=%s raft=%s http=%s", config.NodeID, node.RPCAddr(), node.RaftAddr(), node.HTTPAddr())

	if config.Bootstrap || config.JoinAddress != "" {
		if err := waitForCluster(node, config.Bootstrap); err != nil {
			log.Fatal(err)
		}
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}

func waitForCluster(node *agent.Agent, bootstrap bool) error {
	if bootstrap {
		return node.WaitForLeader(30 * time.Second)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := node.Join(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out joining cluster: " + err.Error())
		}
		time.Sleep(time.Second)
	}
}

func configArgument(args []string) string {
	for i, arg := range args {
		if arg == "-config" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "-config=") {
			return strings.TrimPrefix(arg, "-config=")
		}
	}
	return ""
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
