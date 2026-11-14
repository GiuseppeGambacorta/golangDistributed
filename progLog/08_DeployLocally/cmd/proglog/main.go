// Command proglog is a small operational client for a deployed chapter 08
// cluster.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	"Distribuiti/progLog/08_DeployLocally/internal/client"
	"google.golang.org/grpc"
)

const (
	defaultRPCAddr  = "127.0.0.1:8400"
	defaultHTTPAddr = "127.0.0.1:8080"
	defaultTimeout  = 5 * time.Second
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(os.Stderr, "proglog: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		if len(args) == 0 {
			return tui(nil, stdout, stderr)
		}
		printUsage(stdout)
		return nil
	}
	command := args[0]
	args = args[1:]
	if command == "raft" {
		if len(args) > 0 && args[0] == "status" {
			args = args[1:]
		}
		command = "raft-status"
	}
	switch command {
	case "tui":
		return tui(args, stdout, stderr)
	case "produce":
		return produce(args, stdout, stderr)
	case "consume":
		return consume(args, stdout, stderr)
	case "servers", "discovery":
		return servers(args, stdout, stderr)
	case "health":
		return probe(args, "healthz", stdout, stderr)
	case "ready", "readiness":
		return probe(args, "readyz", stdout, stderr)
	case "raft-status":
		return raftStatus(args, stdout, stderr)
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", command)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: proglog <command> [flags]

Commands:
  tui           open the interactive cluster dashboard (default command)
  produce       append one record (-value or -value-base64)
  consume       read one record by offset
  servers       show cluster membership and leader (alias: discovery)
  health        query the agent liveness endpoint
  ready         query the agent readiness endpoint (alias: readiness)
  raft status   show Raft state and membership as JSON

Each command accepts -timeout. gRPC commands use -addr (default 127.0.0.1:8400);
health, ready, and raft status use the agent HTTP address (default 127.0.0.1:8080).
Use "proglog <command> -h" for command-specific flags.`)
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func produce(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("produce", stderr)
	addr := flags.String("addr", defaultRPCAddr, "gRPC address of any cluster member")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	value := flags.String("value", "", "record value as UTF-8 text")
	valueBase64 := flags.String("value-base64", "", "record value as base64")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("produce does not accept positional arguments")
	}
	if *value != "" && *valueBase64 != "" {
		return errors.New("use only one of -value and -value-base64")
	}
	data := []byte(*value)
	if *valueBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(*valueBase64)
		if err != nil {
			return fmt.Errorf("decode -value-base64: %w", err)
		}
		data = decoded
	}
	conn, closeConn, err := dialCluster(*addr)
	if err != nil {
		return err
	}
	defer closeConn()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	response, err := api.NewLogClient(conn).Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: data},
	})
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return writeJSON(stdout, struct {
		Offset uint64 `json:"offset"`
	}{Offset: response.Offset})
}

func consume(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("consume", stderr)
	addr := flags.String("addr", defaultRPCAddr, "gRPC address of any cluster member")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	offset := flags.Uint64("offset", 0, "log offset to read")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("consume does not accept positional arguments")
	}
	conn, closeConn, err := dialCluster(*addr)
	if err != nil {
		return err
	}
	defer closeConn()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	response, err := api.NewLogClient(conn).Consume(ctx, &api.ConsumeRequest{Offset: *offset})
	if err != nil {
		return fmt.Errorf("consume offset %d: %w", *offset, err)
	}
	if response.Record == nil {
		return errors.New("consume returned an empty record")
	}
	return writeJSON(stdout, struct {
		Record struct {
			Value  []byte `json:"value"`
			Offset uint64 `json:"offset"`
		} `json:"record"`
	}{Record: struct {
		Value  []byte `json:"value"`
		Offset uint64 `json:"offset"`
	}{Value: response.Record.Value, Offset: response.Record.Offset}})
}

func servers(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("servers", stderr)
	addr := flags.String("addr", defaultRPCAddr, "gRPC address of any cluster member")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("servers does not accept positional arguments")
	}
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	response, err := api.NewDiscoveryClient(conn).GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		return fmt.Errorf("discover servers: %w", err)
	}
	return writeJSON(stdout, response)
}

func probe(args []string, path string, stdout, stderr io.Writer) error {
	flags := newFlagSet(path, stderr)
	addr := flags.String("addr", defaultHTTPAddr, "agent HTTP address")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s does not accept positional arguments", path)
	}
	result, err := getHTTP(*addr, path, *timeout)
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func raftStatus(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("raft status", stderr)
	addr := flags.String("addr", defaultHTTPAddr, "agent HTTP address")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("raft status does not accept positional arguments")
	}
	result, err := getHTTP(*addr, "raftz", *timeout)
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func dialCluster(address string) (*grpc.ClientConn, func(), error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil, errors.New("gRPC address is required")
	}
	conn, err := client.Dial(context.Background(), address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", address, err)
	}
	return conn, func() { _ = conn.Close() }, nil
}

type httpResult struct {
	Endpoint string `json:"endpoint"`
	Status   int    `json:"status"`
	OK       bool   `json:"ok"`
	Body     string `json:"body,omitempty"`
}

func getHTTP(address, path string, timeout time.Duration) (any, error) {
	base := address
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	base = strings.TrimRight(base, "/")
	endpoint := base + "/" + strings.TrimLeft(path, "/")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build HTTP request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return nil, fmt.Errorf("read %s response: %w", endpoint, readErr)
	}
	result := httpResult{
		Endpoint: endpoint,
		Status:   response.StatusCode,
		OK:       response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices,
		Body:     strings.TrimSpace(string(body)),
	}
	if !result.OK {
		return result, fmt.Errorf("GET %s returned %s: %s", endpoint, response.Status, result.Body)
	}
	if path == "raftz" {
		var document any
		if err := json.Unmarshal(body, &document); err != nil {
			return result, fmt.Errorf("decode %s response: %w", endpoint, err)
		}
		return document, nil
	}
	return result, nil
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	return nil
}
