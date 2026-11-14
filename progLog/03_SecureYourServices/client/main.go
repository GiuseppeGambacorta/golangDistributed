package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	api "Distribuiti/progLog/03_SecureYourServices/api/v1"
	"Distribuiti/progLog/03_SecureYourServices/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "gRPC server address")
	op := flag.String("op", "produce", "operation: produce or consume")
	value := flag.String("value", "hello world", "record value for produce")
	offset := flag.Uint64("offset", 0, "record offset for consume")
	caFile := flag.String("ca", config.CAFile, "CA certificate path")
	certFile := flag.String("cert", config.RootClientCertFile, "client certificate path")
	keyFile := flag.String("key", config.RootClientKeyFile, "client key path")
	serverName := flag.String("server-name", "localhost", "TLS server name")
	flag.Parse()

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      *certFile,
		KeyFile:       *keyFile,
		CAFile:        *caFile,
		ServerAddress: *serverName,
	})
	if err != nil {
		log.Fatal(err)
	}
	connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
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
