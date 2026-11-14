package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	api "Distribuiti/progLog/02_ServeRequestsWithgRPC/api/v1"

	"google.golang.org/grpc"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "gRPC server address")
	op := flag.String("op", "produce", "operation: produce or consume")
	value := flag.String("value", "hello world", "record value for produce")
	offset := flag.Uint64("offset", 0, "record offset for consume")
	flag.Parse()

	connection, err := grpc.Dial(*address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	client := api.NewLogClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *op {
	case "produce":
		response, err := client.Produce(ctx, &api.ProduceRequest{
			Record: &api.Record{Value: []byte(*value)},
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(response.Offset)
	case "consume":
		response, err := client.Consume(ctx, &api.ConsumeRequest{Offset: *offset})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("offset=%d value=%s\n", response.Record.Offset, response.Record.Value)
	default:
		log.Fatal("unsupported operation: " + strconv.Quote(*op))
	}
}
