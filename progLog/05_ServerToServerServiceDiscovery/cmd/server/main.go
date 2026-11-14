package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"Distribuiti/progLog/05_ServerToServerServiceDiscovery/internal/agent"
)

func main() {
	var config agent.Config
	var joins stringList
	flag.StringVar(&config.NodeName, "name", "node", "Serf node name")
	flag.StringVar(&config.BindAddr, "bind", "127.0.0.1:7946", "Serf bind address")
	flag.IntVar(&config.RPCPort, "rpc", 8400, "gRPC port")
	flag.StringVar(&config.DataDir, "data", "./data", "log data directory")
	flag.Var(&joins, "join", "Serf address to join (repeatable)")
	flag.Parse()
	config.StartJoinAddrs = joins

	a, err := agent.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer a.Shutdown()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}

type stringList []string

func (s *stringList) String() string { return "" }
func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}
