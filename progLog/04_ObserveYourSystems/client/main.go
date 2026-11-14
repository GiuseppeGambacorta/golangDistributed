package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	api "Distribuiti/progLog/04_ObserveYourSystems/api/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "gRPC server address")
	op := flag.String("op", "produce", "operation: produce or consume")
	value := flag.String("value", "hello world", "record value for produce")
	offset := flag.Uint64("offset", 0, "record offset for consume")
	secure := flag.Bool("tls", false, "use TLS credentials")
	caFile := flag.String("ca", "", "CA certificate path")
	certFile := flag.String("cert", "", "client certificate path")
	keyFile := flag.String("key", "", "client key path")
	serverName := flag.String("server-name", "localhost", "TLS server name")
	flag.Parse()

	dialOptions := []grpc.DialOption{grpc.WithInsecure()}
	if *secure {
		tlsConfig, err := loadTLSConfig(*caFile, *certFile, *keyFile, *serverName)
		if err != nil {
			log.Fatal(err)
		}
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	}
	connection, err := grpc.Dial(*address, dialOptions...)
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

func loadTLSConfig(caFile, certFile, keyFile, serverName string) (*tls.Config, error) {
	certificatePEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(certificatePEM) {
		return nil, fmt.Errorf("failed to parse CA certificate: %q", caFile)
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{certificate},
		ServerName:   serverName,
	}, nil
}
