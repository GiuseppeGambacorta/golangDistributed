package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	api "Distribuiti/progLog/08_DeployLocally/api/v1"
	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
)

type tuiMode uint8

const (
	tuiView tuiMode = iota
	tuiProduce
	tuiConsume
)

type tuiModel struct {
	rpcAddr, httpAddr string
	timeout           time.Duration
	discovery         *api.GetServersResponse
	raft              any
	health, ready     bool
	message           string
	lastAction        string
	input             string
	mode              tuiMode
	loading           bool
}

type tuiStatusMsg struct {
	discovery *api.GetServersResponse
	raft      any
	health    bool
	ready     bool
	err       error
}

type tuiActionMsg struct {
	text string
	err  error
}

type tuiTickMsg time.Time

func tui(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("tui", stderr)
	rpcAddr := flags.String("addr", defaultRPCAddr, "gRPC address of any cluster member")
	httpAddr := flags.String("http-addr", defaultHTTPAddr, "agent HTTP address")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("tui does not accept positional arguments")
	}
	model := tuiModel{rpcAddr: *rpcAddr, httpAddr: *httpAddr, timeout: *timeout}
	final, err := tea.NewProgram(model, tea.WithAltScreen()).Run()
	if err != nil {
		return fmt.Errorf("run TUI: %w", err)
	}
	if m, ok := final.(tuiModel); ok && m.message != "" {
		_, _ = fmt.Fprintln(stdout, m.message)
	}
	return nil
}

func (m tuiModel) Init() tea.Cmd {
	return tea.Batch(m.refresh(), tuiTick())
}

func tuiTick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tuiTickMsg(t) })
}

func (m tuiModel) refresh() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		conn, err := grpc.Dial(m.rpcAddr, grpc.WithInsecure())
		if err != nil {
			return tuiStatusMsg{err: err}
		}
		defer conn.Close()
		discovery, err := api.NewDiscoveryClient(conn).GetServers(ctx, &api.GetServersRequest{})
		if err != nil {
			return tuiStatusMsg{err: fmt.Errorf("discovery: %w", err)}
		}
		raft, raftErr := getHTTP(m.httpAddr, "raftz", m.timeout)
		_, healthErr := getHTTP(m.httpAddr, "healthz", m.timeout)
		_, readyErr := getHTTP(m.httpAddr, "readyz", m.timeout)
		if raftErr != nil {
			return tuiStatusMsg{discovery: discovery, err: raftErr}
		}
		return tuiStatusMsg{discovery: discovery, raft: raft, health: healthErr == nil, ready: readyErr == nil}
	}
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.key(msg)
	case tuiTickMsg:
		if m.loading {
			return m, tuiTick()
		}
		return m, tea.Batch(m.refresh(), tuiTick())
	case tuiStatusMsg:
		m.loading = false
		if msg.err != nil {
			m.message = "Errore aggiornamento: " + msg.err.Error()
			return m, nil
		}
		m.discovery, m.raft = msg.discovery, msg.raft
		m.health, m.ready = msg.health, msg.ready
		m.message = "Stato aggiornato " + time.Now().Format("15:04:05")
	case tuiActionMsg:
		m.loading = false
		if msg.err != nil {
			m.lastAction = "Errore: " + msg.err.Error()
		} else {
			m.lastAction = msg.text
		}
		return m, m.refresh()
	}
	return m, nil
}

func (m tuiModel) key(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.mode != tuiView {
		switch msg.Type {
		case tea.KeyEsc:
			m.mode, m.input = tuiView, ""
		case tea.KeyBackspace:
			if len(m.input) > 0 {
				m.input = m.input[:len(m.input)-1]
			}
		case tea.KeyEnter:
			value := m.input
			mode := m.mode
			m.mode, m.input, m.loading = tuiView, "", true
			if mode == tuiProduce {
				return m, m.produce(value)
			}
			return m, m.consume(value)
		default:
			m.input += string(msg.Runes)
		}
		return m, nil
	}
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "r":
		m.loading = true
		return m, m.refresh()
	case "p":
		m.mode = tuiProduce
		m.message = "Inserisci il valore e premi INVIO (ESC annulla)"
	case "c":
		m.mode = tuiConsume
		m.message = "Inserisci l'offset e premi INVIO (ESC annulla)"
	}
	return m, nil
}

func (m tuiModel) produce(value string) tea.Cmd {
	return func() tea.Msg {
		conn, err := grpc.Dial(m.rpcAddr, grpc.WithInsecure())
		if err != nil {
			return tuiActionMsg{err: err}
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		response, err := api.NewLogClient(conn).Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte(value)}})
		if err != nil {
			return tuiActionMsg{err: fmt.Errorf("produce: %w", err)}
		}
		return tuiActionMsg{text: fmt.Sprintf("Record scritto all'offset %d", response.Offset)}
	}
}

func (m tuiModel) consume(value string) tea.Cmd {
	return func() tea.Msg {
		offset, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return tuiActionMsg{err: fmt.Errorf("offset non valido: %q", value)}
		}
		conn, err := grpc.Dial(m.rpcAddr, grpc.WithInsecure())
		if err != nil {
			return tuiActionMsg{err: err}
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		response, err := api.NewLogClient(conn).Consume(ctx, &api.ConsumeRequest{Offset: offset})
		if err != nil {
			return tuiActionMsg{err: fmt.Errorf("consume: %w", err)}
		}
		record := response.GetRecord()
		if record == nil {
			return tuiActionMsg{err: fmt.Errorf("offset %d: record vuoto", offset)}
		}
		return tuiActionMsg{text: fmt.Sprintf("Record offset %d: %q", record.Offset, record.Value)}
	}
}

func (m tuiModel) View() string {
	var b strings.Builder
	b.WriteString("PROGLOG CLUSTER TUI\n")
	b.WriteString(fmt.Sprintf("gRPC: %-28s HTTP: %s\n\n", m.rpcAddr, m.httpAddr))
	b.WriteString(fmt.Sprintf("Health: %-8s  Ready: %-8s  Refresh: 2s\n", tuiStatus(m.health), tuiStatus(m.ready)))
	if m.discovery == nil {
		b.WriteString("\nConnessione al cluster in corso...\n")
	} else {
		b.WriteString(fmt.Sprintf("Leader: %s (%s)\n\n", m.discovery.LeaderID, m.discovery.LeaderAddress))
		b.WriteString("MEMBERS\n")
		for _, server := range m.discovery.Servers {
			role := "follower"
			if server.Leader {
				role = "LEADER"
			}
			b.WriteString(fmt.Sprintf("  %-16s %-8s voter=%t rpc=%s\n", server.ID, role, server.Voter, server.RPCAddress))
		}
	}
	if m.raft != nil {
		data, _ := json.Marshal(m.raft)
		b.WriteString("\nRAFT: " + string(data) + "\n")
	}
	if m.mode != tuiView {
		label := "valore"
		if m.mode == tuiConsume {
			label = "offset"
		}
		b.WriteString(fmt.Sprintf("\n%s: %s_", label, m.input))
	} else {
		b.WriteString("\n[p] produce  [c] consume  [r] refresh  [q] esci")
	}
	if m.lastAction != "" {
		b.WriteString("\n\nULTIMA AZIONE: " + m.lastAction)
	}
	if m.message != "" {
		b.WriteString("\n\n" + m.message)
	}
	return b.String()
}

func tuiStatus(value bool) string {
	if value {
		return "OK"
	}
	return "DOWN"
}
