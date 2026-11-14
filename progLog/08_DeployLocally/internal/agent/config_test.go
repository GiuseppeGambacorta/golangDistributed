package agent_test

import (
	"os"
	"path/filepath"
	"testing"

	"Distribuiti/progLog/08_DeployLocally/internal/agent"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"node_id": "node-2",
		"data_dir": "/var/lib/log",
		"rpc_bind_addr": "0.0.0.0:8400",
		"rpc_advertise_addr": "node-2.log-headless:8400",
		"raft_bind_addr": "0.0.0.0:7400",
		"raft_advertise_addr": "node-2.log-headless:7400",
		"http_bind_addr": "0.0.0.0:8080"
	}`), 0600))

	config, err := agent.LoadConfigFile(path)
	require.NoError(t, err)
	require.Equal(t, "node-2.log-headless:7400", config.RaftAdvertiseAddr)
	require.Equal(t, "node-2.log-headless:8400", config.RPCAdvertiseAddr)
}
