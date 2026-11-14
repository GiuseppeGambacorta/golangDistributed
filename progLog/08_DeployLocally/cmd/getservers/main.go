package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	"google.golang.org/grpc"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8400", "gRPC address of any cluster member")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	conn, err := grpc.Dial(*address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	response, err := api.NewDiscoveryClient(conn).GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		log.Fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(response); err != nil {
		log.Fatal(err)
	}
}
