// manager_test.go
package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// mockCommandStore implements CommandStoreInterface for testing
type mockCommandStore struct {
	mu       sync.Mutex
	commands map[string]*commonpb.ClientCommand
	configs  map[string]*ClientConfig
	nextID   int

	// control flags for testing error paths
	shouldFailPush       bool
	shouldFailList       bool
	shouldFailDelete     bool
	shouldFailListConfig bool
}

func newMockCommandStore() *mockCommandStore {
	return &mockCommandStore{
		commands: make(map[string]*commonpb.ClientCommand),
		configs:  make(map[string]*ClientConfig),
	}
}

func (m *mockCommandStore) PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (string, error) {
	if m.shouldFailPush {
		return "", errors.New("mock push error")
	}

	// Validate persistent flag usage
	if req.Persistent && req.CommandType != "push_config" {
		return "", fmt.Errorf("only push_config commands can be persistent, got: %s", req.CommandType)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	idPrefix := "cmd"
	if req.Persistent {
		idPrefix = "cfg"
	}
	cmdID := fmt.Sprintf("%s-%d", idPrefix, m.nextID)

	scope := "global"
	if req.TargetClientId != "" {
		scope = fmt.Sprintf("client:%s", req.TargetClientId)
	}

	if req.Persistent {
		// Store as config
		m.configs[cmdID] = &ClientConfig{
			ConfigId:    cmdID,
			ConfigType:  req.CommandType,
			Payload:     req.Payload,
			CreateTime:  time.Now().UnixMilli(),
			TargetScope: scope,
		}
	} else {
		// Store as regular command
		m.commands[cmdID] = &commonpb.ClientCommand{
			CommandId:   cmdID,
			CommandType: req.CommandType,
			Payload:     req.Payload,
			CreateTime:  time.Now().UnixMilli(),
			TargetScope: scope,
		}
	}
	return cmdID, nil
}

func (m *mockCommandStore) ListCommands(ctx context.Context) ([]*commonpb.ClientCommand, error) {
	if m.shouldFailList {
		return nil, errors.New("mock list error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var commands []*commonpb.ClientCommand
	for _, cmd := range m.commands {
		commands = append(commands, cmd)
	}
	return commands, nil
}

func (m *mockCommandStore) DeleteCommand(ctx context.Context, commandID string) error {
	if m.shouldFailDelete {
		return errors.New("mock delete error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Try deleting from both commands and configs
	delete(m.commands, commandID)
	delete(m.configs, commandID)
	return nil
}

func (m *mockCommandStore) DeleteNonPersistentCommand(commandID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.commands[commandID]; ok {
		delete(m.commands, commandID)
		return true
	}
	return false
}

func (m *mockCommandStore) GetCommandInfo(commandID string) (string, []byte, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cmd, ok := m.commands[commandID]; ok {
		return cmd.CommandType, cmd.Payload, false, true
	}
	if cfg, ok := m.configs[commandID]; ok {
		return cfg.ConfigType, cfg.Payload, true, true
	}
	return "", nil, false, false
}

func (m *mockCommandStore) CleanupExpiredCommands(ctx context.Context) {
	// no-op for mock
}

func (m *mockCommandStore) ListConfigs(ctx context.Context) ([]*ClientConfig, string, error) {
	if m.shouldFailListConfig {
		return nil, "", errors.New("mock list config error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var configs []*ClientConfig
	for _, cfg := range m.configs {
		configs = append(configs, cfg)
	}
	// Simple hash calculation for testing
	hash := ""
	if len(configs) > 0 {
		hash = "mock-config-hash"
	}
	return configs, hash, nil
}

func (m *mockCommandStore) ListCommandsWithInfo(ctx context.Context) ([]*CommandInfoData, error) {
	if m.shouldFailList {
		return nil, errors.New("mock list error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*CommandInfoData

	// Add commands (non-persistent)
	for _, cmd := range m.commands {
		result = append(result, &CommandInfoData{
			CommandID:   cmd.CommandId,
			CommandType: cmd.CommandType,
			TargetScope: cmd.TargetScope,
			Persistent:  false,
			CreateTime:  cmd.CreateTime,
			TTLSeconds:  0, // mock doesn't track TTL
		})
	}

	// Add configs (persistent)
	for _, cfg := range m.configs {
		result = append(result, &CommandInfoData{
			CommandID:   cfg.ConfigId,
			CommandType: cfg.ConfigType,
			TargetScope: cfg.TargetScope,
			Persistent:  true,
			CreateTime:  cfg.CreateTime,
			TTLSeconds:  0,
		})
	}

	return result, nil
}

func TestNewTelemetryManager(t *testing.T) {
	mgr := NewTelemetryManager(nil) // nil etcd client for now
	assert.NotNil(t, mgr)
	// sync.Map is value type, always initialized
	assert.NotNil(t, mgr.config)
}

func TestHandleHeartbeat(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "pymilvus",
			SdkVersion: "2.4.0",
			Host:       "192.168.1.100",
		},
		ReportTimestamp: time.Now().UnixMilli(),
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
					SuccessCount: 99,
					ErrorCount:   1,
					AvgLatencyMs: 15.5,
					P99LatencyMs: 45.0,
				},
			},
		},
		ConfigHash:           "",
		LastCommandTimestamp: 0,
	}

	resp, err := mgr.HandleHeartbeat(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.ServerTimestamp > 0)

	// Verify client was registered
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)
	assert.Equal(t, "pymilvus", clients[0].ClientInfo.SdkType)
}

func TestGetClientTelemetry(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register two clients via heartbeat
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "client-1"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 100, SuccessCount: 100}},
		},
	})

	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "java-sdk",
			Host:     "192.168.1.101",
			Reserved: map[string]string{"client_id": "client-2"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 50, SuccessCount: 50}},
		},
	})

	// Query all clients
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 2)
	assert.Equal(t, int64(150), resp.Aggregated.RequestCount)

	// Query by client_id
	resp, err = mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId:       "client-1",
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
}

func TestMatchesScope(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	clientInfo := &commonpb.ClientInfo{
		SdkType: "pymilvus",
		Host:    "192.168.1.100",
	}

	tests := []struct {
		name     string
		scope    string
		clientID string
		expected bool
	}{
		{"global scope matches any client", "global", "any-client", true},
		{"client scope matches same client", "client:client-123", "client-123", true},
		{"client scope does not match different client", "client:client-123", "client-456", false},
		{"unknown scope does not match", "unknown:value", "any-client", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mgr.matchesScope(tt.scope, tt.clientID, clientInfo)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchesScopeDatabase(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	clientInfo := &commonpb.ClientInfo{
		SdkType: "pymilvus",
		Host:    "192.168.1.100",
	}

	t.Run("database scope matches client with access", func(t *testing.T) {
		// Register a client with database access
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "pymilvus",
				Host:    "192.168.1.100",
				Reserved: map[string]string{
					"client_id": "db-client-1",
					"db_name":   "test_database",
				},
			},
		})

		result := mgr.matchesScope("database:test_database", "db-client-1", clientInfo)
		assert.True(t, result)
	})

	t.Run("database scope does not match client without access", func(t *testing.T) {
		// Register a client with different database access
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "pymilvus",
				Host:    "192.168.1.101",
				Reserved: map[string]string{
					"client_id": "db-client-2",
					"db_name":   "other_database",
				},
			},
		})

		result := mgr.matchesScope("database:test_database", "db-client-2", clientInfo)
		assert.False(t, result)
	})

	t.Run("database scope does not match unknown client", func(t *testing.T) {
		result := mgr.matchesScope("database:test_database", "unknown-client", clientInfo)
		assert.False(t, result)
	})

	t.Run("database scope does not match client with nil AccessedDatabases", func(t *testing.T) {
		// Create client but simulate nil AccessedDatabases
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "pymilvus",
				Host:    "192.168.1.102",
				Reserved: map[string]string{
					"client_id": "db-client-3",
				},
			},
		})

		// sync.Map is empty by default, so no databases are accessed
		// The test verifies that a client with no accessed databases doesn't match database scope
		result := mgr.matchesScope("database:test_database", "db-client-3", clientInfo)
		assert.False(t, result)
	})
}

func TestPushCommandWithNilStore(t *testing.T) {
	mgr := NewTelemetryManager(nil) // nil etcd client = nil command store

	ctx := context.Background()
	_, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "test",
		Payload:     []byte("test"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command store not initialized")
}

func TestDeleteCommandWithNilStore(t *testing.T) {
	mgr := NewTelemetryManager(nil) // nil etcd client = nil command store

	ctx := context.Background()
	_, err := mgr.DeleteCommand(ctx, &milvuspb.DeleteClientCommandRequest{
		CommandId: "some-command-id",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command store not initialized")
}

func TestGetCommandsForClientWithNilStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "pymilvus",
			Host:    "192.168.1.100",
		},
	}

	// Should return nil without error when store is nil
	clientID := req.ClientInfo.Reserved["client_id"]
	if clientID == "" {
		clientID = "test-client"
	}
	commands := mgr.getCommandsForClientWithID(clientID, req)
	assert.Nil(t, commands)
}

func TestCleanupInactiveClients(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register a client via heartbeat
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "client-old"},
		},
	})

	// Manually set old heartbeat time to simulate inactive client
	if existing, loaded := mgr.clientMetrics.Load("client-old"); loaded {
		cache := existing.(*ClientMetricsCache)
		cache.LastHeartbeat.Store(time.Now().Add(-35 * time.Minute).UnixNano())
	}

	// Add another active client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "java-sdk",
			Host:     "192.168.1.101",
			Reserved: map[string]string{"client_id": "client-active"},
		},
	})

	// Verify both clients exist before cleanup
	clients := mgr.ListClients("")
	assert.Len(t, clients, 2)

	// Run cleanup
	ctx := context.Background()
	mgr.cleanupInactiveClients(ctx)

	// Only active client should remain
	clients = mgr.ListClients("")
	assert.Len(t, clients, 1)
	assert.Equal(t, "client-active", clients[0].ClientID)
}

func TestStartStop(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Start should not panic
	mgr.Start()

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not panic and should complete
	mgr.Stop()
}

func TestGetClientStatus(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Active client (recent heartbeat)
	activeCache := &ClientMetricsCache{}
	activeCache.LastHeartbeat.Store(time.Now().UnixNano())
	assert.Equal(t, "active", mgr.getClientStatus(activeCache))

	// Inactive client (old heartbeat)
	inactiveCache := &ClientMetricsCache{}
	inactiveCache.LastHeartbeat.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	assert.Equal(t, "inactive", mgr.getClientStatus(inactiveCache))
}

// Integration-style tests for full flow scenarios

func TestFullTelemetryFlow(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Simulate multiple clients sending heartbeats over time
	clients := []struct {
		clientID string
		sdkType  string
		host     string
	}{
		{"client-pymilvus-1", "pymilvus", "192.168.1.100"},
		{"client-java-1", "java-sdk", "192.168.1.101"},
		{"client-node-1", "node-sdk", "192.168.1.102"},
	}

	// Register all clients
	for _, c := range clients {
		resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType:  c.sdkType,
				Host:     c.host,
				Reserved: map[string]string{"client_id": c.clientID},
			},
			ReportTimestamp: time.Now().UnixMilli(),
			Metrics: []*commonpb.OperationMetrics{
				{
					Operation: "Search",
					Global: &commonpb.Metrics{
						RequestCount: 100,
						SuccessCount: 95,
						ErrorCount:   5,
						AvgLatencyMs: 25.0,
						P99LatencyMs: 100.0,
					},
				},
			},
		})
		assert.NoError(t, err)
		assert.NotNil(t, resp.Status)
	}

	// Query all clients
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 3)
	assert.Equal(t, int64(300), resp.Aggregated.RequestCount) // 100 * 3 clients
	assert.Equal(t, int64(285), resp.Aggregated.SuccessCount) // 95 * 3 clients
	assert.Equal(t, int64(15), resp.Aggregated.ErrorCount)    // 5 * 3 clients
}

func TestClientHeartbeatUpdatesExistingClient(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	clientID := "persistent-client"

	// First heartbeat
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "pymilvus",
			SdkVersion: "2.4.0",
			Host:       "192.168.1.100",
			Reserved:   map[string]string{"client_id": clientID},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 50}},
		},
	})
	assert.NoError(t, err)

	// Second heartbeat with updated metrics
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "pymilvus",
			SdkVersion: "2.4.1", // Updated version
			Host:       "192.168.1.100",
			Reserved:   map[string]string{"client_id": clientID},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 100}}, // Updated count
		},
	})
	assert.NoError(t, err)

	// Should still only have one client
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)

	// Metrics should be latest
	assert.Equal(t, int64(100), clients[0].LatestMetrics[0].Global.RequestCount)
	// SDK version should be updated
	assert.Equal(t, "2.4.1", clients[0].ClientInfo.SdkVersion)
}

func TestMultipleOperationTypes(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Client reports multiple operation types
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "multi-op-client"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 100, SuccessCount: 100}},
			{Operation: "Insert", Global: &commonpb.Metrics{RequestCount: 50, SuccessCount: 48, ErrorCount: 2}},
			{Operation: "Query", Global: &commonpb.Metrics{RequestCount: 30, SuccessCount: 30}},
		},
	})
	assert.NoError(t, err)

	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId:       "multi-op-client",
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)

	// Verify aggregated metrics
	assert.Equal(t, int64(180), resp.Aggregated.RequestCount) // 100 + 50 + 30
	assert.Equal(t, int64(178), resp.Aggregated.SuccessCount) // 100 + 48 + 30
	assert.Equal(t, int64(2), resp.Aggregated.ErrorCount)
}

func TestConcurrentHeartbeats(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Simulate concurrent heartbeats from multiple clients
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
					ClientInfo: &commonpb.ClientInfo{
						SdkType:  "pymilvus",
						Host:     "192.168.1.100",
						Reserved: map[string]string{"client_id": fmt.Sprintf("client-%d", idx)},
					},
					Metrics: []*commonpb.OperationMetrics{
						{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 1, SuccessCount: 1}},
					},
				})
				assert.NoError(t, err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have 10 distinct clients
	clients := mgr.ListClients("")
	assert.Len(t, clients, 10)
}

func TestLifecycleWithCleanup(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Start the manager
	mgr.Start()

	// Register some clients
	for i := 0; i < 3; i++ {
		_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType:  "pymilvus",
				Host:     fmt.Sprintf("192.168.1.%d", i),
				Reserved: map[string]string{"client_id": fmt.Sprintf("client-%d", i)},
			},
		})
		assert.NoError(t, err)
	}

	// Verify clients are registered
	assert.Len(t, mgr.ListClients(""), 3)

	// Stop should complete gracefully
	mgr.Stop()
}

func TestGetOrCreateClientID(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Test with reserved client_id
	info1 := &commonpb.ClientInfo{
		Host:     "192.168.1.100",
		Reserved: map[string]string{"client_id": "my-custom-id"},
	}
	assert.Equal(t, "my-custom-id", mgr.getOrCreateClientID(info1))

	// Test with nil reserved map
	info2 := &commonpb.ClientInfo{
		Host: "192.168.1.100",
	}
	id2 := mgr.getOrCreateClientID(info2)
	assert.Contains(t, id2, "192.168.1.100")

	// Test with nil client info
	id3 := mgr.getOrCreateClientID(nil)
	assert.NotEmpty(t, id3)
}

func TestGetDatabaseList(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	cache := &ClientMetricsCache{}
	cache.AccessedDatabases.Store("db1", struct{}{})
	cache.AccessedDatabases.Store("db2", struct{}{})
	cache.AccessedDatabases.Store("db3", struct{}{})

	dbs := mgr.getDatabaseList(cache)
	assert.Len(t, dbs, 3)
	assert.ElementsMatch(t, []string{"db1", "db2", "db3"}, dbs)
}

func TestCleanupExpiredCommandsWithNilStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Should not panic when command store is nil
	ctx := context.Background()
	mgr.cleanupExpiredCommands(ctx)
}

func TestHeartbeatWithNilMetrics(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Heartbeat without metrics
	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "no-metrics-client"},
		},
		Metrics: nil,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// Query should work
	telResp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId:       "no-metrics-client",
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, telResp.Clients, 1)
	assert.Nil(t, telResp.Clients[0].Metrics)
}

func TestGetClientTelemetryWithDatabaseFilter(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register a client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "db-filter-client"},
		},
	})

	// Manually add database access
	if existing, loaded := mgr.clientMetrics.Load("db-filter-client"); loaded {
		cache := existing.(*ClientMetricsCache)
		cache.AccessedDatabases.Store("test_db", struct{}{})
	}

	// Query with matching database
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		Database: "test_db",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)

	// Query with non-matching database
	resp, err = mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		Database: "other_db",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 0)
}

func TestListClientsWithDatabaseFilter(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register clients
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "client-with-db"},
		},
	})

	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "java-sdk",
			Host:     "192.168.1.101",
			Reserved: map[string]string{"client_id": "client-without-db"},
		},
	})

	// Add database access to first client only
	if existing, loaded := mgr.clientMetrics.Load("client-with-db"); loaded {
		cache := existing.(*ClientMetricsCache)
		cache.AccessedDatabases.Store("my_database", struct{}{})
	}

	// List all clients
	all := mgr.ListClients("")
	assert.Len(t, all, 2)

	// List clients with database filter
	filtered := mgr.ListClients("my_database")
	assert.Len(t, filtered, 1)
	assert.Equal(t, "client-with-db", filtered[0].ClientID)

	// List clients with non-existent database
	empty := mgr.ListClients("nonexistent")
	assert.Len(t, empty, 0)
}

func TestGetClientTelemetryWithMetricsAggregation(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register multiple clients with different metrics
	clients := []struct {
		clientID     string
		requestCount int64
		successCount int64
		errorCount   int64
	}{
		{"client-1", 100, 95, 5},
		{"client-2", 200, 190, 10},
		{"client-3", 300, 280, 20},
	}

	for _, c := range clients {
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType:  "pymilvus",
				Host:     "192.168.1.100",
				Reserved: map[string]string{"client_id": c.clientID},
			},
			Metrics: []*commonpb.OperationMetrics{
				{
					Operation: "Search",
					Global: &commonpb.Metrics{
						RequestCount: c.requestCount,
						SuccessCount: c.successCount,
						ErrorCount:   c.errorCount,
					},
				},
			},
		})
	}

	// Query with metrics
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 3)
	// Total: 600 requests, 565 success, 35 errors
	assert.Equal(t, int64(600), resp.Aggregated.RequestCount)
	assert.Equal(t, int64(565), resp.Aggregated.SuccessCount)
	assert.Equal(t, int64(35), resp.Aggregated.ErrorCount)
}

func TestGetClientTelemetryWithoutMetrics(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "test-client"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 100}},
		},
	})

	// Query without metrics
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: false,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
	// Metrics should not be included
	assert.Nil(t, resp.Clients[0].Metrics)
}

func TestGetClientTelemetryAllFilters(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register clients with different databases
	for i := 0; i < 3; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType:  "pymilvus",
				Host:     fmt.Sprintf("192.168.1.%d", i),
				Reserved: map[string]string{"client_id": clientID},
			},
			Metrics: []*commonpb.OperationMetrics{
				{Operation: "Search", Global: &commonpb.Metrics{RequestCount: 10}},
			},
		})
		// Add database for first two clients
		if i < 2 {
			if existing, loaded := mgr.clientMetrics.Load(clientID); loaded {
				cache := existing.(*ClientMetricsCache)
				cache.AccessedDatabases.Store("shared_db", struct{}{})
			}
		}
	}

	// Filter by client_id
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId: "client-1",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)

	// Filter by database
	resp, err = mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		Database: "shared_db",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 2)

	// Filter by non-existent client
	resp, err = mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId: "non-existent",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 0)
}

func TestHeartbeatUpdatesConfigHash(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// First heartbeat
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "hash-test-client"},
		},
		ConfigHash:           "initial-hash",
		LastCommandTimestamp: 12345,
	})
	assert.NoError(t, err)

	// Verify hash and timestamp were stored
	existing, loaded := mgr.clientMetrics.Load("hash-test-client")
	assert.True(t, loaded)
	cache := existing.(*ClientMetricsCache)
	assert.Equal(t, "initial-hash", cache.ConfigHash)
	assert.Equal(t, int64(12345), cache.LastCommandTS)
}

func TestHeartbeatWithEmptyClientInfo(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Heartbeat with nil ClientInfo
	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: nil,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	// Should still work and create an entry with generated ID
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)
}

func TestGetClientTelemetryReturnsStatus(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register active client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "active-client"},
		},
	})

	// Register inactive client (manually set old heartbeat)
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "java-sdk",
			Host:     "192.168.1.101",
			Reserved: map[string]string{"client_id": "inactive-client"},
		},
	})
	if existing, loaded := mgr.clientMetrics.Load("inactive-client"); loaded {
		cache := existing.(*ClientMetricsCache)
		cache.LastHeartbeat.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	}

	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 2)

	// Check status is set correctly
	for _, client := range resp.Clients {
		if client.ClientInfo.Reserved["client_id"] == "active-client" {
			assert.Equal(t, "active", client.Status)
		} else {
			assert.Equal(t, "inactive", client.Status)
		}
	}
}

func TestGetClientTelemetryDatabaseList(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register client and add databases
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "db-list-client"},
		},
	})

	if existing, loaded := mgr.clientMetrics.Load("db-list-client"); loaded {
		cache := existing.(*ClientMetricsCache)
		cache.AccessedDatabases.Store("db1", struct{}{})
		cache.AccessedDatabases.Store("db2", struct{}{})
	}

	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId: "db-list-client",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
	assert.ElementsMatch(t, []string{"db1", "db2"}, resp.Clients[0].Databases)
}

func TestCleanupLoopBehavior(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register a client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "cleanup-test-client"},
		},
	})

	// Manually call cleanup functions to test them
	ctx := context.Background()
	mgr.cleanupInactiveClients(ctx)
	mgr.cleanupExpiredCommands(ctx)

	// Client should still be there (it's recent)
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)
}

func TestGetOrCreateClientIDEdgeCases(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Test with empty Reserved map (not nil, but empty)
	info1 := &commonpb.ClientInfo{
		Host:     "192.168.1.100",
		Reserved: map[string]string{},
	}
	id1 := mgr.getOrCreateClientID(info1)
	assert.Contains(t, id1, "192.168.1.100")

	// Test with Reserved containing empty client_id
	info2 := &commonpb.ClientInfo{
		Host:     "192.168.1.101",
		Reserved: map[string]string{"client_id": ""},
	}
	id2 := mgr.getOrCreateClientID(info2)
	assert.Contains(t, id2, "192.168.1.101")

	// Test with no host and no reserved
	info3 := &commonpb.ClientInfo{}
	id3 := mgr.getOrCreateClientID(info3)
	assert.NotEmpty(t, id3)
}

func TestHeartbeatResponseContainsTimestamp(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	beforeTs := time.Now().UnixMilli()
	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "ts-test-client"},
		},
	})
	afterTs := time.Now().UnixMilli()

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, resp.ServerTimestamp, beforeTs)
	assert.LessOrEqual(t, resp.ServerTimestamp, afterTs)
}

func TestMultipleClientsWithSameHost(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Two clients from the same host but different client IDs
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "app1-client"},
		},
	})
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "app2-client"},
		},
	})

	clients := mgr.ListClients("")
	assert.Len(t, clients, 2)
}

func TestGetClientTelemetryWithNilGlobalMetrics(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Client with operation metrics but nil Global
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "nil-global-client"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global:    nil, // nil global metrics
			},
		},
	})

	// Should not panic and should return zero aggregated metrics
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
	assert.Equal(t, int64(0), resp.Aggregated.RequestCount)
}

// Tests using mock command store

func TestPushCommandWithMockStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Test successful push
	resp, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "set_log_level",
		Payload:     []byte(`{"level":"debug"}`),
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.CommandId)
	assert.NotNil(t, resp.Status)

	// Verify command was stored
	assert.Len(t, mockStore.commands, 1)
}

func TestPushCommandWithMockStoreError(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mockStore.shouldFailPush = true
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Test push failure
	_, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "test",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock push error")
}

func TestDeleteCommandWithMockStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// First push a command
	resp, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "test_cmd",
	})
	assert.NoError(t, err)
	assert.Len(t, mockStore.commands, 1)

	// Now delete it
	delResp, err := mgr.DeleteCommand(ctx, &milvuspb.DeleteClientCommandRequest{
		CommandId: resp.CommandId,
	})
	assert.NoError(t, err)
	assert.NotNil(t, delResp.Status)
	assert.Len(t, mockStore.commands, 0)
}

func TestDeleteCommandWithMockStoreError(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mockStore.shouldFailDelete = true
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Test delete failure
	_, err := mgr.DeleteCommand(ctx, &milvuspb.DeleteClientCommandRequest{
		CommandId: "any-id",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock delete error")
}

func TestGetCommandsForClientWithMockStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push some commands (one-time)
	mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "global_cmd",
	})
	mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "client_cmd",
		TargetClientId: "specific-client",
	})

	// Test heartbeat returns commands
	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "specific-client"},
		},
		ConfigHash:           "",
		LastCommandTimestamp: 0,
	}

	commands := mgr.getCommandsForClientWithID("specific-client", req)
	// Should get both global and client-specific commands
	assert.NotNil(t, commands)
}

func TestGetCommandsForClientListError(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mockStore.shouldFailList = true
	mgr.SetCommandStore(mockStore)

	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "test-client"},
		},
	}

	// Should return nil when list fails
	commands := mgr.getCommandsForClientWithID("test-client", req)
	assert.Nil(t, commands)
}

func TestGetCommandsForClientWithConfigHashChange(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push a persistent config (not a one-time command)
	mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})

	// First heartbeat with empty hash - should get persistent config
	req1 := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "hash-test-client"},
		},
		ConfigHash:           "",
		LastCommandTimestamp: time.Now().UnixMilli(), // high timestamp so no one-time commands match
	}
	commands1 := mgr.getCommandsForClientWithID("hash-test-client", req1)
	assert.NotEmpty(t, commands1)

	// Compute the actual hash from received configs (same way client would)
	var receivedConfigs []*ClientConfig
	for _, cmd := range commands1 {
		if cmd.Persistent {
			receivedConfigs = append(receivedConfigs, &ClientConfig{
				ConfigId:   cmd.CommandId,
				ConfigType: cmd.CommandType,
				Payload:    cmd.Payload,
			})
		}
	}
	computedHash := computeClientConfigHash(receivedConfigs)

	// Second heartbeat with matching hash - should not get config again
	req2 := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "hash-test-client"},
		},
		ConfigHash:           computedHash, // matches computed hash
		LastCommandTimestamp: time.Now().UnixMilli(),
	}
	commands2 := mgr.getCommandsForClientWithID("hash-test-client", req2)
	assert.Empty(t, commands2)
}

func TestGetCommandsForClientWithOneTimeCommand(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push a one-time command
	mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "one_time_cmd",
	})

	// First heartbeat with timestamp 0 - should get the command
	req1 := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "onetime-client"},
		},
		LastCommandTimestamp: 0,
	}
	commands1 := mgr.getCommandsForClientWithID("onetime-client", req1)
	assert.NotEmpty(t, commands1)

	// Second heartbeat with recent timestamp - should not get the command
	req2 := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "onetime-client"},
		},
		LastCommandTimestamp: time.Now().UnixMilli() + 1000, // future timestamp
	}
	commands2 := mgr.getCommandsForClientWithID("onetime-client", req2)
	assert.Empty(t, commands2)
}

func TestHeartbeatReturnsCommands(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push a persistent config (so it returns on heartbeat with empty hash)
	mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})

	// Heartbeat should return the config as command
	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "cmd-test-client"},
		},
		ConfigHash:           "",
		LastCommandTimestamp: time.Now().UnixMilli(), // high ts so no one-time commands
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp.Commands)
	assert.Len(t, resp.Commands, 1)
}

func TestCleanupExpiredCommandsWithMockStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	// Should not panic with mock store
	ctx := context.Background()
	mgr.cleanupExpiredCommands(ctx)
}

func TestSetCommandStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Initially nil
	assert.Nil(t, mgr.commandStore)

	// Set mock store
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	assert.NotNil(t, mgr.commandStore)
}

func TestPushCommandWithTargetClient(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push command with target client
	resp, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "client_specific_cmd",
		TargetClientId: "target-client-123",
	})
	assert.NoError(t, err)

	// Verify scope was set correctly
	cmd := mockStore.commands[resp.CommandId]
	assert.Equal(t, "client:target-client-123", cmd.TargetScope)
}

func TestCommandScopeFiltering(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push configs with different scopes (persistent)
	mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})
	mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		TargetClientId: "client-A",
		Persistent:     true,
	})
	mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		TargetClientId: "client-B",
		Persistent:     true,
	})

	// Client A should get global + client-A specific (but not client-B specific)
	reqA := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "client-A"},
		},
		ConfigHash:           "",
		LastCommandTimestamp: time.Now().UnixMilli(), // high ts so no one-time commands
	}
	commandsA := mgr.getCommandsForClientWithID("client-A", reqA)
	// Should get global and client-A configs (not client-B)
	assert.GreaterOrEqual(t, len(commandsA), 2)
}

// Tests for configurable parameters

func TestDefaultTelemetryConfig(t *testing.T) {
	config := DefaultTelemetryConfig()

	assert.NotNil(t, config)
	assert.Equal(t, 1*time.Minute, config.CleanupInterval)
	assert.Equal(t, 10*time.Minute, config.InactiveClientThreshold)
	assert.Equal(t, 1*time.Minute, config.ClientStatusThreshold)
	assert.Equal(t, 10*time.Second, config.CommandCleanupTimeout)
}

func TestNewTelemetryManagerWithConfig(t *testing.T) {
	t.Run("with custom config", func(t *testing.T) {
		customConfig := &TelemetryConfig{
			CleanupInterval:         1 * time.Minute,
			InactiveClientThreshold: 10 * time.Minute,
			ClientStatusThreshold:   2 * time.Minute,
			CommandCleanupTimeout:   5 * time.Second,
		}

		mgr := NewTelemetryManagerWithConfig(nil, customConfig)
		assert.NotNil(t, mgr)
		assert.Equal(t, customConfig, mgr.config)
	})

	t.Run("with nil config uses default", func(t *testing.T) {
		mgr := NewTelemetryManagerWithConfig(nil, nil)
		assert.NotNil(t, mgr)
		assert.NotNil(t, mgr.config)
		assert.Equal(t, 1*time.Minute, mgr.config.CleanupInterval)
	})
}

func TestSetConfig(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	newConfig := &TelemetryConfig{
		CleanupInterval:         2 * time.Minute,
		InactiveClientThreshold: 15 * time.Minute,
		ClientStatusThreshold:   3 * time.Minute,
		CommandCleanupTimeout:   8 * time.Second,
	}

	mgr.SetConfig(newConfig)
	assert.Equal(t, newConfig, mgr.config)
}

func TestCleanupWithCustomThreshold(t *testing.T) {
	// Create manager with short threshold for testing
	customConfig := &TelemetryConfig{
		CleanupInterval:         100 * time.Millisecond,
		InactiveClientThreshold: 50 * time.Millisecond,
		ClientStatusThreshold:   25 * time.Millisecond,
		CommandCleanupTimeout:   1 * time.Second,
	}
	mgr := NewTelemetryManagerWithConfig(nil, customConfig)

	// Register a client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "threshold-test-client"},
		},
	})

	// Verify client exists
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)

	// Wait for threshold to expire
	time.Sleep(100 * time.Millisecond)

	// Run cleanup
	ctx := context.Background()
	mgr.cleanupInactiveClients(ctx)

	// Client should be removed due to short threshold
	clients = mgr.ListClients("")
	assert.Len(t, clients, 0)
}

func TestClientStatusWithCustomThreshold(t *testing.T) {
	// Create manager with short status threshold
	customConfig := &TelemetryConfig{
		CleanupInterval:         5 * time.Minute,
		InactiveClientThreshold: 30 * time.Minute,
		ClientStatusThreshold:   50 * time.Millisecond, // Very short for testing
		CommandCleanupTimeout:   10 * time.Second,
	}
	mgr := NewTelemetryManagerWithConfig(nil, customConfig)

	// Register a client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "status-test-client"},
		},
	})

	// Client should be active immediately
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId: "status-test-client",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
	assert.Equal(t, "active", resp.Clients[0].Status)

	// Wait for status threshold to expire
	time.Sleep(100 * time.Millisecond)

	// Client should now be inactive
	resp, err = mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId: "status-test-client",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 1)
	assert.Equal(t, "inactive", resp.Clients[0].Status)
}

func TestCleanupLoopWithCustomInterval(t *testing.T) {
	// Create manager with very short cleanup interval
	customConfig := &TelemetryConfig{
		CleanupInterval:         50 * time.Millisecond,
		InactiveClientThreshold: 25 * time.Millisecond, // Very short
		ClientStatusThreshold:   10 * time.Millisecond,
		CommandCleanupTimeout:   1 * time.Second,
	}
	mgr := NewTelemetryManagerWithConfig(nil, customConfig)

	// Register a client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "interval-test-client"},
		},
	})

	// Verify client exists
	clients := mgr.ListClients("")
	assert.Len(t, clients, 1)

	// Start the cleanup loop
	mgr.Start()

	// Wait for cleanup to run (at least one interval + threshold)
	time.Sleep(150 * time.Millisecond)

	// Stop the manager
	mgr.Stop()

	// Client should have been cleaned up by the background loop
	clients = mgr.ListClients("")
	assert.Len(t, clients, 0)
}

func TestMultipleStartStop(t *testing.T) {
	customConfig := &TelemetryConfig{
		CleanupInterval:         1 * time.Hour, // Long interval to avoid actual cleanup
		InactiveClientThreshold: 30 * time.Minute,
		ClientStatusThreshold:   5 * time.Minute,
		CommandCleanupTimeout:   10 * time.Second,
	}

	// Test that we can create, start, and stop multiple managers
	for i := 0; i < 3; i++ {
		mgr := NewTelemetryManagerWithConfig(nil, customConfig)
		mgr.Start()
		time.Sleep(10 * time.Millisecond)
		mgr.Stop()
	}
}

func TestConfigFieldsUsedCorrectly(t *testing.T) {
	// Test that each config field affects the correct behavior
	t.Run("InactiveClientThreshold affects cleanup", func(t *testing.T) {
		// Short threshold - clients get cleaned up quickly
		shortConfig := &TelemetryConfig{
			CleanupInterval:         5 * time.Minute,
			InactiveClientThreshold: 10 * time.Millisecond,
			ClientStatusThreshold:   5 * time.Minute,
			CommandCleanupTimeout:   10 * time.Second,
		}
		mgr := NewTelemetryManagerWithConfig(nil, shortConfig)

		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
		})

		time.Sleep(50 * time.Millisecond)
		ctx := context.Background()
		mgr.cleanupInactiveClients(ctx)
		assert.Len(t, mgr.ListClients(""), 0, "client should be cleaned up with short threshold")
	})

	t.Run("ClientStatusThreshold affects status reporting", func(t *testing.T) {
		// Long threshold - clients stay active longer
		longConfig := &TelemetryConfig{
			CleanupInterval:         5 * time.Minute,
			InactiveClientThreshold: 30 * time.Minute,
			ClientStatusThreshold:   1 * time.Hour, // Very long
			CommandCleanupTimeout:   10 * time.Second,
		}
		mgr := NewTelemetryManagerWithConfig(nil, longConfig)

		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
		})

		// Even after some delay, client should still be "active" with long threshold
		time.Sleep(50 * time.Millisecond)
		resp, _ := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
			ClientId: "test-client",
		})
		assert.Equal(t, "active", resp.Clients[0].Status)
	})
}

// TestProcessCommandReplies tests the processCommandReplies method
func TestProcessCommandReplies(t *testing.T) {
	t.Run("successful_command_reply", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)

		// First send heartbeat to register client
		_, _ = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
		})

		// Send heartbeat with command replies
		replies := []*commonpb.CommandReply{
			{
				CommandId: "cmd-1",
				Success:   true,
			},
			{
				CommandId:    "cmd-2",
				Success:      false,
				ErrorMessage: "execution failed",
			},
		}

		resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
			CommandReplies: replies,
		})

		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("empty_command_replies", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)

		resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client-2"},
			},
			CommandReplies: []*commonpb.CommandReply{},
		})

		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})
}

// TestSetErrorCollector tests the SetErrorCollector method
func TestSetErrorCollector(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Mock error collector
	mockCollector := &mockErrorCollector{}

	mgr.SetErrorCollector(mockCollector)
	assert.NotNil(t, mgr.commandRouter)
}

// TestValidateAndTruncateMetricsEdgeCases tests edge cases for metrics validation
func TestValidateAndTruncateMetricsEdgeCases(t *testing.T) {
	mgr := NewTelemetryManagerWithConfig(nil, &TelemetryConfig{
		MaxOperationTypesPerClient: 2,
	})

	t.Run("truncate_many_operation_types", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "insert",
				Global:    &commonpb.Metrics{RequestCount: 10},
			},
			{
				Operation: "search",
				Global:    &commonpb.Metrics{RequestCount: 20},
			},
			{
				Operation: "delete",
				Global:    &commonpb.Metrics{RequestCount: 30},
			},
		}

		validated := mgr.validateAndTruncateMetrics(metrics)
		assert.Equal(t, 2, len(validated))
	})

	t.Run("filter_zero_request_metrics", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "insert",
				CollectionMetrics: map[string]*commonpb.Metrics{
					"col1": {RequestCount: 10, SuccessCount: 9},
					"col2": {RequestCount: 0, SuccessCount: 0},
					"col3": nil,
				},
			},
		}

		validated := mgr.validateAndTruncateMetrics(metrics)
		assert.Equal(t, 1, len(validated[0].CollectionMetrics))
		assert.Contains(t, validated[0].CollectionMetrics, "col1")
	})
}

// TestHandleHeartbeatWithNilClientInfo tests heartbeat with nil client info
func TestHandleHeartbeatWithNilClientInfo(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: nil,
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

// TestMemoryLimitEnforcement tests that memory limit is properly enforced
func TestMemoryLimitEnforcement(t *testing.T) {
	smallConfig := &TelemetryConfig{
		CleanupInterval:         10 * time.Millisecond,
		InactiveClientThreshold: 100 * time.Millisecond,
		MaxClientsInMemory:      5,
	}

	mgr := NewTelemetryManagerWithConfig(nil, smallConfig)
	mgr.Start()
	defer mgr.Stop()

	// Add clients until we exceed limit
	for i := 0; i < 10; i++ {
		_, _ = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Host: fmt.Sprintf("host-%d", i),
			},
		})
	}

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)

	// Verify count is controlled
	count := 0
	mgr.clientMetrics.Range(func(key, value any) bool {
		count++
		return true
	})

	assert.Less(t, count, 20)
}

// Mock error collector for testing
type mockErrorCollector struct{}

func (m *mockErrorCollector) GetRecentErrors(maxCount int) {
}

// =============================================================================
// Additional Tests for Metrics Aggregation and Command Idempotency
// =============================================================================

// TestLatencyMetricsAggregation verifies that latency metrics (P99, Avg, Max) are
// properly stored and returned for each client. Note: Current implementation
// aggregates only count metrics (RequestCount, SuccessCount, ErrorCount) across clients,
// but latency metrics are preserved per-client in the response.
func TestLatencyMetricsAggregation(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Register clients with different latency metrics
	clients := []struct {
		clientID     string
		avgLatency   float64
		p99Latency   float64
		maxLatency   float64
		requestCount int64
	}{
		{"client-1", 10.5, 50.0, 100.0, 100},
		{"client-2", 20.0, 80.0, 150.0, 200},
		{"client-3", 15.0, 60.0, 120.0, 150},
	}

	for _, c := range clients {
		_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType:  "pymilvus",
				Host:     "192.168.1.100",
				Reserved: map[string]string{"client_id": c.clientID},
			},
			Metrics: []*commonpb.OperationMetrics{
				{
					Operation: "Search",
					Global: &commonpb.Metrics{
						RequestCount: c.requestCount,
						SuccessCount: c.requestCount,
						AvgLatencyMs: c.avgLatency,
						P99LatencyMs: c.p99Latency,
						MaxLatencyMs: c.maxLatency,
					},
				},
			},
		})
		assert.NoError(t, err)
	}

	// Query with metrics included
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 3)

	// Verify count aggregation
	assert.Equal(t, int64(450), resp.Aggregated.RequestCount) // 100 + 200 + 150
	assert.Equal(t, int64(450), resp.Aggregated.SuccessCount)

	// Verify each client has correct latency metrics preserved
	clientMetrics := make(map[string]*commonpb.Metrics)
	for _, client := range resp.Clients {
		clientID := client.ClientInfo.Reserved["client_id"]
		if len(client.Metrics) > 0 && client.Metrics[0].Global != nil {
			clientMetrics[clientID] = client.Metrics[0].Global
		}
	}

	// Verify client-1 latency
	assert.Equal(t, 10.5, clientMetrics["client-1"].AvgLatencyMs)
	assert.Equal(t, 50.0, clientMetrics["client-1"].P99LatencyMs)
	assert.Equal(t, 100.0, clientMetrics["client-1"].MaxLatencyMs)

	// Verify client-2 latency
	assert.Equal(t, 20.0, clientMetrics["client-2"].AvgLatencyMs)
	assert.Equal(t, 80.0, clientMetrics["client-2"].P99LatencyMs)
	assert.Equal(t, 150.0, clientMetrics["client-2"].MaxLatencyMs)

	// Verify client-3 latency
	assert.Equal(t, 15.0, clientMetrics["client-3"].AvgLatencyMs)
	assert.Equal(t, 60.0, clientMetrics["client-3"].P99LatencyMs)
	assert.Equal(t, 120.0, clientMetrics["client-3"].MaxLatencyMs)
}

// TestCollectionLevelMetricsAggregation verifies that collection-level metrics
// are properly stored and returned per client.
func TestCollectionLevelMetricsAggregation(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// Client 1: metrics for collections A and B
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": "client-1"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
					SuccessCount: 95,
					ErrorCount:   5,
				},
				CollectionMetrics: map[string]*commonpb.Metrics{
					"collection_A": {
						RequestCount: 60,
						SuccessCount: 58,
						ErrorCount:   2,
						AvgLatencyMs: 10.0,
						P99LatencyMs: 50.0,
						MaxLatencyMs: 100.0,
					},
					"collection_B": {
						RequestCount: 40,
						SuccessCount: 37,
						ErrorCount:   3,
						AvgLatencyMs: 15.0,
						P99LatencyMs: 70.0,
						MaxLatencyMs: 150.0,
					},
				},
			},
		},
	})
	assert.NoError(t, err)

	// Client 2: metrics for collections A and C
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "java-sdk",
			Host:     "192.168.1.101",
			Reserved: map[string]string{"client_id": "client-2"},
		},
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 200,
					SuccessCount: 190,
					ErrorCount:   10,
				},
				CollectionMetrics: map[string]*commonpb.Metrics{
					"collection_A": {
						RequestCount: 120,
						SuccessCount: 115,
						ErrorCount:   5,
						AvgLatencyMs: 12.0,
						P99LatencyMs: 55.0,
						MaxLatencyMs: 110.0,
					},
					"collection_C": {
						RequestCount: 80,
						SuccessCount: 75,
						ErrorCount:   5,
						AvgLatencyMs: 20.0,
						P99LatencyMs: 90.0,
						MaxLatencyMs: 200.0,
					},
				},
			},
		},
	})
	assert.NoError(t, err)

	// Query with metrics
	resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		IncludeMetrics: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Clients, 2)

	// Verify global aggregation
	assert.Equal(t, int64(300), resp.Aggregated.RequestCount) // 100 + 200
	assert.Equal(t, int64(285), resp.Aggregated.SuccessCount) // 95 + 190
	assert.Equal(t, int64(15), resp.Aggregated.ErrorCount)    // 5 + 10

	// Verify collection-level metrics are preserved per client (including latency metrics)
	for _, client := range resp.Clients {
		clientID := client.ClientInfo.Reserved["client_id"]
		assert.NotEmpty(t, client.Metrics)
		assert.NotNil(t, client.Metrics[0].CollectionMetrics)

		if clientID == "client-1" {
			collMetrics := client.Metrics[0].CollectionMetrics
			assert.Len(t, collMetrics, 2)
			assert.Contains(t, collMetrics, "collection_A")
			assert.Contains(t, collMetrics, "collection_B")

			// Verify collection_A metrics including latency
			assert.Equal(t, int64(60), collMetrics["collection_A"].RequestCount)
			assert.Equal(t, int64(58), collMetrics["collection_A"].SuccessCount)
			assert.Equal(t, int64(2), collMetrics["collection_A"].ErrorCount)
			assert.Equal(t, 10.0, collMetrics["collection_A"].AvgLatencyMs)
			assert.Equal(t, 50.0, collMetrics["collection_A"].P99LatencyMs)
			assert.Equal(t, 100.0, collMetrics["collection_A"].MaxLatencyMs)

			// Verify collection_B metrics including latency
			assert.Equal(t, int64(40), collMetrics["collection_B"].RequestCount)
			assert.Equal(t, int64(37), collMetrics["collection_B"].SuccessCount)
			assert.Equal(t, int64(3), collMetrics["collection_B"].ErrorCount)
			assert.Equal(t, 15.0, collMetrics["collection_B"].AvgLatencyMs)
			assert.Equal(t, 70.0, collMetrics["collection_B"].P99LatencyMs)
			assert.Equal(t, 150.0, collMetrics["collection_B"].MaxLatencyMs)
		} else if clientID == "client-2" {
			collMetrics := client.Metrics[0].CollectionMetrics
			assert.Len(t, collMetrics, 2)
			assert.Contains(t, collMetrics, "collection_A")
			assert.Contains(t, collMetrics, "collection_C")

			// Verify collection_A metrics including latency
			assert.Equal(t, int64(120), collMetrics["collection_A"].RequestCount)
			assert.Equal(t, int64(115), collMetrics["collection_A"].SuccessCount)
			assert.Equal(t, int64(5), collMetrics["collection_A"].ErrorCount)
			assert.Equal(t, 12.0, collMetrics["collection_A"].AvgLatencyMs)
			assert.Equal(t, 55.0, collMetrics["collection_A"].P99LatencyMs)
			assert.Equal(t, 110.0, collMetrics["collection_A"].MaxLatencyMs)

			// Verify collection_C metrics including latency
			assert.Equal(t, int64(80), collMetrics["collection_C"].RequestCount)
			assert.Equal(t, int64(75), collMetrics["collection_C"].SuccessCount)
			assert.Equal(t, int64(5), collMetrics["collection_C"].ErrorCount)
			assert.Equal(t, 20.0, collMetrics["collection_C"].AvgLatencyMs)
			assert.Equal(t, 90.0, collMetrics["collection_C"].P99LatencyMs)
			assert.Equal(t, 200.0, collMetrics["collection_C"].MaxLatencyMs)
		}
	}
}

// TestMetricsSnapshotOverwrite verifies that metrics are overwritten (not accumulated)
// when the same client sends multiple heartbeats.
func TestMetricsSnapshotOverwrite(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	clientID := "snapshot-test-client"

	// First heartbeat with initial metrics
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": clientID},
		},
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
					SuccessCount: 95,
					ErrorCount:   5,
					AvgLatencyMs: 10.0,
				},
			},
		},
	})
	assert.NoError(t, err)

	// Verify initial metrics
	resp1, _ := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId:       clientID,
		IncludeMetrics: true,
	})
	assert.Equal(t, int64(100), resp1.Aggregated.RequestCount)

	// Second heartbeat with NEW metrics (should OVERWRITE, not accumulate)
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": clientID},
		},
		Metrics: []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 150, // New value, not 100+150=250
					SuccessCount: 145,
					ErrorCount:   5,
					AvgLatencyMs: 12.0, // New latency
				},
			},
		},
	})
	assert.NoError(t, err)

	// Verify metrics were OVERWRITTEN, not accumulated
	resp2, _ := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
		ClientId:       clientID,
		IncludeMetrics: true,
	})
	assert.Equal(t, int64(150), resp2.Aggregated.RequestCount, "metrics should be overwritten, not accumulated")
	assert.Equal(t, int64(145), resp2.Aggregated.SuccessCount)
	assert.Len(t, resp2.Clients, 1)
	assert.Equal(t, 12.0, resp2.Clients[0].Metrics[0].Global.AvgLatencyMs)
}

// TestCommandDeduplication verifies that:
// 1. Persistent configs are not re-sent when ConfigHash matches
// 2. One-time commands are not re-sent after LastCommandTimestamp
func TestCommandDeduplication(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()
	clientID := "dedup-test-client"

	t.Run("persistent_config_not_resent_when_hash_matches", func(t *testing.T) {
		// Push a persistent config
		_, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"enabled": true}`),
			Persistent:  true,
		})
		assert.NoError(t, err)

		// First heartbeat - empty hash, should receive config
		req1 := &milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": clientID},
			},
			ConfigHash:           "",
			LastCommandTimestamp: time.Now().UnixMilli(),
		}
		commands1 := mgr.getCommandsForClientWithID(clientID, req1)
		assert.NotEmpty(t, commands1, "should receive config on first heartbeat")

		// Compute hash from received configs (same way client would)
		var receivedConfigs []*ClientConfig
		for _, cmd := range commands1 {
			if cmd.Persistent {
				receivedConfigs = append(receivedConfigs, &ClientConfig{
					ConfigId:   cmd.CommandId,
					ConfigType: cmd.CommandType,
					Payload:    cmd.Payload,
				})
			}
		}
		computedHash := computeClientConfigHash(receivedConfigs)

		// Second heartbeat - matching hash, should NOT receive config
		req2 := &milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": clientID},
			},
			ConfigHash:           computedHash, // use computed hash
			LastCommandTimestamp: time.Now().UnixMilli(),
		}
		commands2 := mgr.getCommandsForClientWithID(clientID, req2)
		assert.Empty(t, commands2, "should NOT receive config when hash matches")

		// Third heartbeat - different hash, should receive config again
		req3 := &milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": clientID},
			},
			ConfigHash:           "old-hash",
			LastCommandTimestamp: time.Now().UnixMilli(),
		}
		commands3 := mgr.getCommandsForClientWithID(clientID, req3)
		assert.NotEmpty(t, commands3, "should receive config when hash doesn't match")
	})

	t.Run("one_time_command_not_resent_after_timestamp", func(t *testing.T) {
		// Create fresh store for this test
		mockStore2 := newMockCommandStore()
		mgr2 := NewTelemetryManager(nil)
		mgr2.SetCommandStore(mockStore2)

		// Push a one-time command
		_, err := mgr2.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "show_errors",
			Payload:     []byte(`{}`),
		})
		assert.NoError(t, err)

		// Get the command's create time
		commands, _ := mockStore2.ListCommands(ctx)
		assert.Len(t, commands, 1)
		cmdCreateTime := commands[0].CreateTime

		// Heartbeat with old timestamp - should receive command
		req1 := &milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "client-A"},
			},
			LastCommandTimestamp: cmdCreateTime - 1000, // Before command was created
		}
		cmds1 := mgr2.getCommandsForClientWithID("client-A", req1)
		assert.NotEmpty(t, cmds1, "should receive command when timestamp is old")

		// Heartbeat with recent timestamp - should NOT receive command
		req2 := &milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "client-B"},
			},
			LastCommandTimestamp: cmdCreateTime + 1000, // After command was created
		}
		cmds2 := mgr2.getCommandsForClientWithID("client-B", req2)
		assert.Empty(t, cmds2, "should NOT receive command when timestamp is recent")
	})
}

// TestCommandDeleteAfterAck verifies that one-time commands are deleted
// after the client acknowledges them via CommandReplies.
func TestCommandDeleteAfterAck(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()
	clientID := "ack-test-client"

	// Push a one-time command
	pushResp, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
		Payload:     []byte(`{"max_count": 50}`),
	})
	assert.NoError(t, err)
	commandID := pushResp.CommandId

	// Verify command exists
	commands1, _ := mockStore.ListCommands(ctx)
	assert.Len(t, commands1, 1)
	assert.Equal(t, commandID, commands1[0].CommandId)

	// Client sends heartbeat with ACK (CommandReplies)
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:  "pymilvus",
			Host:     "192.168.1.100",
			Reserved: map[string]string{"client_id": clientID},
		},
		CommandReplies: []*commonpb.CommandReply{
			{
				CommandId: commandID,
				Success:   true,
			},
		},
	})
	assert.NoError(t, err)

	// Verify command was deleted after ACK
	commands2, _ := mockStore.ListCommands(ctx)
	assert.Empty(t, commands2, "one-time command should be deleted after ACK")
}

// TestCommandDeleteAfterAckFailure verifies that commands are still deleted
// even if the client reports execution failure.
func TestCommandDeleteAfterAckFailure(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()
	clientID := "ack-failure-client"

	// Push a one-time command
	pushResp, err := mgr.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
	})
	assert.NoError(t, err)
	commandID := pushResp.CommandId

	// Client reports command execution FAILED
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			Reserved: map[string]string{"client_id": clientID},
		},
		CommandReplies: []*commonpb.CommandReply{
			{
				CommandId:    commandID,
				Success:      false,
				ErrorMessage: "command execution failed",
			},
		},
	})
	assert.NoError(t, err)

	// Command should still be deleted (to avoid infinite retry loops)
	commands, _ := mockStore.ListCommands(ctx)
	assert.Empty(t, commands, "command should be deleted even after failure ACK")
}

// TestPersistentConfigNotDeletedAfterAck verifies that persistent configs
// are NOT deleted after ACK (they should persist).
func TestPersistentConfigNotDeletedAfterAck(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()
	clientID := "persistent-ack-client"

	// Push a persistent config
	_, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"enabled": true}`),
		Persistent:  true,
	})
	assert.NoError(t, err)

	// Verify config exists
	configs1, _, _ := mockStore.ListConfigs(ctx)
	assert.Len(t, configs1, 1)
	configID := configs1[0].ConfigId

	// Client ACKs the config
	_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			Reserved: map[string]string{"client_id": clientID},
		},
		CommandReplies: []*commonpb.CommandReply{
			{
				CommandId: configID,
				Success:   true,
			},
		},
	})
	assert.NoError(t, err)

	// Persistent config should still exist
	configs2, _, _ := mockStore.ListConfigs(ctx)
	assert.Len(t, configs2, 1, "persistent config should NOT be deleted after ACK")
	assert.Equal(t, configID, configs2[0].ConfigId)
}

// TestConcurrentCommandDelivery verifies that commands are delivered correctly
// under concurrent heartbeats from multiple clients.
func TestConcurrentCommandDelivery(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	ctx := context.Background()

	// Push a global persistent config
	_, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"global": true}`),
		Persistent:  true,
	})
	assert.NoError(t, err)

	// Concurrent heartbeats from multiple clients
	var wg sync.WaitGroup
	results := make(chan int, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := fmt.Sprintf("concurrent-client-%d", idx)

			// First heartbeat - should get config
			req := &milvuspb.ClientHeartbeatRequest{
				ClientInfo: &commonpb.ClientInfo{
					Reserved: map[string]string{"client_id": clientID},
				},
				ConfigHash:           "",
				LastCommandTimestamp: time.Now().UnixMilli(),
			}
			commands := mgr.getCommandsForClientWithID(clientID, req)
			results <- len(commands)
		}(i)
	}

	wg.Wait()
	close(results)

	// All clients should have received the config
	totalReceived := 0
	for count := range results {
		if count > 0 {
			totalReceived++
		}
	}
	assert.Equal(t, 10, totalReceived, "all concurrent clients should receive the config")
}

// TestGetClientCommandReplies tests the GetClientCommandReplies function
func TestGetClientCommandReplies(t *testing.T) {
	t.Run("returns nil for non-existent client", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		replies := mgr.GetClientCommandReplies("non-existent-client")
		assert.Nil(t, replies)
	})

	t.Run("returns nil for client with empty replies", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		clientID := "test-client"

		// Register client via heartbeat
		_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": clientID},
			},
		})
		assert.NoError(t, err)

		replies := mgr.GetClientCommandReplies(clientID)
		assert.Nil(t, replies)
	})

	t.Run("returns copies of stored replies", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()
		clientID := "reply-test-client"

		// Push a command
		cmdID, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_command",
			Payload:     []byte(`{"test": true}`),
		})
		assert.NoError(t, err)

		// Client sends heartbeat with command reply
		_, err = mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": clientID},
			},
			CommandReplies: []*commonpb.CommandReply{
				{
					CommandId: cmdID,
					Success:   true,
					Payload:   []byte(`{"result": "ok"}`),
				},
			},
		})
		assert.NoError(t, err)

		// Get replies - should return stored replies
		replies := mgr.GetClientCommandReplies(clientID)
		assert.NotNil(t, replies)
		assert.Len(t, replies, 1)
		assert.Equal(t, cmdID, replies[0].CommandID)
		assert.True(t, replies[0].Success)
	})
}

// TestListAllCommands tests the ListAllCommands function
func TestListAllCommands(t *testing.T) {
	t.Run("returns nil when commandStore is nil", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mgr.SetCommandStore(nil)

		commands, err := mgr.ListAllCommands(context.Background())
		assert.NoError(t, err)
		assert.Nil(t, commands)
	})

	t.Run("returns all commands from store", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()

		// Push multiple commands
		cmdID1, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "command_type_1",
			Payload:     []byte(`{"cmd": 1}`),
		})
		assert.NoError(t, err)

		cmdID2, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType:    "push_config",
			Payload:        []byte(`{"enabled": true}`),
			Persistent:     true,
			TargetClientId: "specific-client",
		})
		assert.NoError(t, err)

		// List all commands
		commands, err := mgr.ListAllCommands(ctx)
		assert.NoError(t, err)
		assert.Len(t, commands, 2)

		// Verify command info
		cmdMap := make(map[string]*CommandInfo)
		for _, cmd := range commands {
			cmdMap[cmd.CommandID] = cmd
		}

		assert.Equal(t, "command_type_1", cmdMap[cmdID1].CommandType)
		assert.Equal(t, "push_config", cmdMap[cmdID2].CommandType)
		assert.True(t, cmdMap[cmdID2].Persistent)
	})

	t.Run("handles ListCommandsWithInfo error", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mockStore.shouldFailList = true
		mgr.SetCommandStore(mockStore)

		_, err := mgr.ListAllCommands(context.Background())
		assert.Error(t, err)
	})
}

// TestCloneClientInfo tests the cloneClientInfo function
func TestCloneClientInfo(t *testing.T) {
	t.Run("returns nil for nil input", func(t *testing.T) {
		result := cloneClientInfo(nil)
		assert.Nil(t, result)
	})

	t.Run("clones client info correctly", func(t *testing.T) {
		original := &commonpb.ClientInfo{
			SdkType:    "GoMilvusClient",
			SdkVersion: "2.0.0",
			User:       "testuser",
			Host:       "localhost",
			Reserved: map[string]string{
				"client_id": "test-123",
				"db_name":   "test_db",
			},
		}

		clone := cloneClientInfo(original)
		assert.NotNil(t, clone)
		assert.Equal(t, original.SdkType, clone.SdkType)
		assert.Equal(t, original.SdkVersion, clone.SdkVersion)
		assert.Equal(t, original.User, clone.User)
		assert.Equal(t, original.Reserved["client_id"], clone.Reserved["client_id"])

		// Verify it's a deep copy
		clone.User = "modified"
		assert.NotEqual(t, original.User, clone.User)
	})
}

// TestCloneOperationMetrics tests the cloneOperationMetrics function
func TestCloneOperationMetrics(t *testing.T) {
	t.Run("returns nil for empty input", func(t *testing.T) {
		result := cloneOperationMetrics(nil)
		assert.Nil(t, result)

		result = cloneOperationMetrics([]*commonpb.OperationMetrics{})
		assert.Nil(t, result)
	})

	t.Run("handles nil metrics in slice", func(t *testing.T) {
		input := []*commonpb.OperationMetrics{
			nil,
			{Operation: "Search"},
			nil,
		}

		result := cloneOperationMetrics(input)
		assert.Len(t, result, 3)
		assert.Nil(t, result[0])
		assert.NotNil(t, result[1])
		assert.Nil(t, result[2])
	})

	t.Run("clones metrics correctly", func(t *testing.T) {
		original := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
					SuccessCount: 99,
					ErrorCount:   1,
					AvgLatencyMs: 5.5,
					P99LatencyMs: 15.0,
				},
			},
			{
				Operation: "Insert",
				Global: &commonpb.Metrics{
					RequestCount: 50,
					SuccessCount: 50,
				},
			},
		}

		clone := cloneOperationMetrics(original)
		assert.Len(t, clone, 2)
		assert.Equal(t, "Search", clone[0].Operation)
		assert.Equal(t, int64(100), clone[0].Global.RequestCount)

		// Verify deep copy
		clone[0].Global.RequestCount = 999
		assert.NotEqual(t, original[0].Global.RequestCount, clone[0].Global.RequestCount)
	})
}

// TestValidateAndTruncateMetricsEdgeCases2 tests additional edge cases
func TestValidateAndTruncateMetricsEdgeCases2(t *testing.T) {
	t.Run("truncates to max operation types", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.MaxOperationTypesPerClient = 2
		mgr := NewTelemetryManagerWithConfig(nil, config)

		metrics := []*commonpb.OperationMetrics{
			{Operation: "Search"},
			{Operation: "Insert"},
			{Operation: "Delete"},
			{Operation: "Query"},
		}

		result := mgr.validateAndTruncateMetrics(metrics)
		assert.Len(t, result, 2)
	})

	t.Run("filters out collections with zero requests", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)

		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				CollectionMetrics: map[string]*commonpb.Metrics{
					"col_with_requests":    {RequestCount: 10},
					"col_without_requests": {RequestCount: 0},
					"nil_col":              nil,
				},
			},
		}

		result := mgr.validateAndTruncateMetrics(metrics)
		assert.Len(t, result[0].CollectionMetrics, 1)
		assert.NotNil(t, result[0].CollectionMetrics["col_with_requests"])
	})

	t.Run("drops collection metrics when payload too large", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.MaxMetricsPerClient = 100 // Very small limit
		mgr := NewTelemetryManagerWithConfig(nil, config)

		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 1000,
					AvgLatencyMs: 5.5,
				},
				CollectionMetrics: map[string]*commonpb.Metrics{
					"col1": {RequestCount: 500},
					"col2": {RequestCount: 500},
				},
			},
		}

		result := mgr.validateAndTruncateMetrics(metrics)
		// Collection metrics should be dropped due to size limit
		assert.NotNil(t, result)
	})

	t.Run("handles very large operation count", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.MaxMetricsPerClient = 500 // Small limit
		mgr := NewTelemetryManagerWithConfig(nil, config)

		// Create many operations
		var metrics []*commonpb.OperationMetrics
		for i := 0; i < 100; i++ {
			metrics = append(metrics, &commonpb.OperationMetrics{
				Operation: fmt.Sprintf("Operation%d", i),
				Global: &commonpb.Metrics{
					RequestCount: int64(i * 10),
					AvgLatencyMs: float64(i),
				},
			})
		}

		result := mgr.validateAndTruncateMetrics(metrics)
		// Should be truncated
		assert.LessOrEqual(t, len(result), config.MaxOperationTypesPerClient)
	})
}

// TestGetDatabaseFromClientInfo tests edge cases
func TestGetDatabaseFromClientInfoEdgeCases(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	t.Run("returns empty for nil info", func(t *testing.T) {
		db := mgr.getDatabaseFromClientInfo(nil)
		assert.Empty(t, db)
	})

	t.Run("returns empty for nil reserved map", func(t *testing.T) {
		db := mgr.getDatabaseFromClientInfo(&commonpb.ClientInfo{
			Reserved: nil,
		})
		assert.Empty(t, db)
	})

	t.Run("returns empty for missing db_name key", func(t *testing.T) {
		db := mgr.getDatabaseFromClientInfo(&commonpb.ClientInfo{
			Reserved: map[string]string{
				"other_key": "value",
			},
		})
		assert.Empty(t, db)
	})

	t.Run("returns db_name when present", func(t *testing.T) {
		db := mgr.getDatabaseFromClientInfo(&commonpb.ClientInfo{
			Reserved: map[string]string{
				"db_name": "test_database",
			},
		})
		assert.Equal(t, "test_database", db)
	})
}

// TestEstimateMetricsSize tests the estimateMetricsSize function
func TestEstimateMetricsSize(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	t.Run("returns 0 for empty metrics", func(t *testing.T) {
		size := mgr.estimateMetricsSize(nil)
		assert.Equal(t, 0, size)

		size = mgr.estimateMetricsSize([]*commonpb.OperationMetrics{})
		assert.Equal(t, 0, size)
	})

	t.Run("handles nil metrics in slice", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			nil,
			{Operation: "Search"},
			nil,
		}
		size := mgr.estimateMetricsSize(metrics)
		assert.True(t, size > 0)
	})

	t.Run("calculates correct size", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
					AvgLatencyMs: 5.5,
				},
			},
		}
		size := mgr.estimateMetricsSize(metrics)
		assert.True(t, size > 0)
	})
}

// TestProcessCommandRepliesEdgeCases tests edge cases in processCommandReplies
func TestProcessCommandRepliesEdgeCases(t *testing.T) {
	t.Run("handles empty replies", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		cache := &ClientMetricsCache{ClientID: "test-client"}
		// Should not panic with empty replies
		mgr.processCommandReplies(cache, nil)
		mgr.processCommandReplies(cache, []*commonpb.CommandReply{})
	})

	t.Run("handles unknown command ID", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		// First register client
		_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
		})
		assert.NoError(t, err)

		// Get client cache
		existing, _ := mgr.clientMetrics.Load("test-client")
		cache := existing.(*ClientMetricsCache)

		// Process reply for unknown command
		mgr.processCommandReplies(cache, []*commonpb.CommandReply{
			{CommandId: "unknown-cmd-id", Success: true},
		})
	})

	t.Run("handles failed reply", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()

		// Push a command
		cmdID, _ := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_command",
		})

		// First register client
		_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				Reserved: map[string]string{"client_id": "test-client"},
			},
		})
		assert.NoError(t, err)

		// Get client cache
		existing, _ := mgr.clientMetrics.Load("test-client")
		cache := existing.(*ClientMetricsCache)

		// Process failed reply
		mgr.processCommandReplies(cache, []*commonpb.CommandReply{
			{
				CommandId:    cmdID,
				Success:      false,
				ErrorMessage: "command failed",
			},
		})
	})
}

// TestLookupCommandInfo tests the lookupCommandInfo function
func TestLookupCommandInfo(t *testing.T) {
	t.Run("returns empty when store is nil", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mgr.SetCommandStore(nil)

		cmdType, payload := mgr.lookupCommandInfo("any-id")
		assert.Empty(t, cmdType)
		assert.Nil(t, payload)
	})

	t.Run("returns empty for non-existent command", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		cmdType, payload := mgr.lookupCommandInfo("non-existent")
		assert.Empty(t, cmdType)
		assert.Nil(t, payload)
	})

	t.Run("returns command info for existing command", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()
		// Use non-persistent command type
		cmdID, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_type",
			Payload:     []byte(`{"test": true}`),
			Persistent:  false,
		})
		assert.NoError(t, err)

		cmdType, payload := mgr.lookupCommandInfo(cmdID)
		assert.Equal(t, "test_type", cmdType)
		assert.NotEmpty(t, payload)
	})

	t.Run("returns command info for persistent config", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()
		// Use push_config for persistent commands
		cmdID, err := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"config": "value"}`),
			Persistent:  true,
		})
		assert.NoError(t, err)

		cmdType, payload := mgr.lookupCommandInfo(cmdID)
		assert.Equal(t, "push_config", cmdType)
		assert.NotEmpty(t, payload)
	})

	t.Run("returns empty for empty command ID", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		cmdType, payload := mgr.lookupCommandInfo("")
		assert.Empty(t, cmdType)
		assert.Nil(t, payload)
	})
}

// TestCleanupRepliedCommands tests the cleanupRepliedCommands function
func TestCleanupRepliedCommands(t *testing.T) {
	t.Run("no-op when store is nil", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mgr.SetCommandStore(nil)

		// Should not panic
		mgr.cleanupRepliedCommands([]string{"test-id"})
	})

	t.Run("does not delete persistent commands", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()
		cmdID, _ := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{}`),
			Persistent:  true,
		})

		// Cleanup should not delete persistent command (DeleteNonPersistentCommand skips persistent)
		mgr.cleanupRepliedCommands([]string{cmdID})

		// Command should still exist
		configs, _, _ := mockStore.ListConfigs(ctx)
		assert.Len(t, configs, 1)
	})

	t.Run("deletes non-persistent commands", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()
		cmdID, _ := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "one_time_command",
			Payload:     []byte(`{}`),
			Persistent:  false,
		})

		// Cleanup should delete non-persistent command
		mgr.cleanupRepliedCommands([]string{cmdID})

		// Command should be deleted
		commands, _ := mockStore.ListCommands(ctx)
		assert.Empty(t, commands)
	})
}

// TestGetClientTelemetryAggregatedMetrics tests aggregated metrics in GetClientTelemetry
func TestGetClientTelemetryAggregatedMetrics(t *testing.T) {
	t.Run("aggregates metrics from multiple clients", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)

		// Create two clients with metrics
		for i, cid := range []string{"agg-client-1", "agg-client-2"} {
			mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
				ClientInfo: &commonpb.ClientInfo{
					SdkType: "test-sdk",
					Reserved: map[string]string{
						"client_id": cid,
					},
				},
				Metrics: []*commonpb.OperationMetrics{
					{
						Operation: "search",
						Global: &commonpb.Metrics{
							RequestCount: int64(100 * (i + 1)),
							SuccessCount: int64(90 * (i + 1)),
							ErrorCount:   int64(10 * (i + 1)),
						},
					},
				},
			})
		}

		resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
			IncludeMetrics: true,
		})
		assert.NoError(t, err)
		// Aggregated should be sum: 100+200=300 requests, 90+180=270 success, 10+20=30 errors
		assert.Equal(t, int64(300), resp.Aggregated.RequestCount)
		assert.Equal(t, int64(270), resp.Aggregated.SuccessCount)
		assert.Equal(t, int64(30), resp.Aggregated.ErrorCount)
	})
}

// TestGetClientTelemetryCommandRepliesInResponse tests command replies in GetClientTelemetry
func TestGetClientTelemetryCommandRepliesInResponse(t *testing.T) {
	t.Run("includes command replies in response", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		ctx := context.Background()

		// Push a command
		cmdResp, _ := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_cmd",
			Payload:     []byte(`{"test": true}`),
		})

		// Client sends heartbeat with command reply
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "reply-client-2",
				},
			},
			CommandReplies: []*commonpb.CommandReply{
				{
					CommandId:    cmdResp,
					Success:      true,
					Payload:      []byte(`{"result": "done"}`),
					ErrorMessage: "",
				},
			},
		})

		// Get telemetry - should include command replies
		resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{
			ClientId: "reply-client-2",
		})
		assert.NoError(t, err)
		assert.Len(t, resp.Clients, 1)
		// Check command_replies is in Reserved
		assert.Contains(t, resp.Clients[0].ClientInfo.Reserved, "command_replies")
	})
}

// TestProcessCommandRepliesFailedCommand tests failed command reply handling
func TestProcessCommandRepliesFailedCommand(t *testing.T) {
	t.Run("logs warning for failed command", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		// Register a client first
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "failing-client-2",
				},
			},
		})

		// Send heartbeat with failed command reply
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "failing-client-2",
				},
			},
			CommandReplies: []*commonpb.CommandReply{
				{
					CommandId:    "cmd-123",
					Success:      false,
					ErrorMessage: "command execution failed: permission denied",
				},
			},
		})

		// Verify the reply was stored
		replies := mgr.GetClientCommandReplies("failing-client-2")
		assert.Len(t, replies, 1)
		assert.False(t, replies[0].Success)
		assert.Equal(t, "command execution failed: permission denied", replies[0].ErrorMsg)
	})
}

// TestProcessCommandRepliesTruncation tests that command replies are truncated to 50
func TestProcessCommandRepliesTruncation(t *testing.T) {
	t.Run("truncates replies to 50", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mgr.SetCommandStore(mockStore)

		// Register client
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "truncate-client",
				},
			},
		})

		// Send 60 command replies (more than 50)
		replies := make([]*commonpb.CommandReply, 60)
		for i := 0; i < 60; i++ {
			replies[i] = &commonpb.CommandReply{
				CommandId: fmt.Sprintf("cmd-%d", i),
				Success:   true,
			}
		}

		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "truncate-client",
				},
			},
			CommandReplies: replies,
		})

		// Verify only 50 are stored (the most recent ones)
		storedReplies := mgr.GetClientCommandReplies("truncate-client")
		assert.Len(t, storedReplies, 50)
		// The first 10 (cmd-0 to cmd-9) should be dropped, keeping cmd-10 to cmd-59
		assert.Equal(t, "cmd-10", storedReplies[0].CommandID)
		assert.Equal(t, "cmd-59", storedReplies[49].CommandID)
	})
}

// TestGetClientTelemetryNilClientInfo tests edge cases with nil ClientInfo
func TestGetClientTelemetryNilClientInfo(t *testing.T) {
	t.Run("handles nil ClientInfo in cache", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)

		// Directly add a client with nil ClientInfo to test the defensive code
		nilCache := &ClientMetricsCache{
			ClientInfo: nil,
		}
		nilCache.LastHeartbeat.Store(time.Now().UnixNano())
		mgr.clientMetrics.Store("test-nil-client", nilCache)

		resp, err := mgr.GetClientTelemetry(&milvuspb.GetClientTelemetryRequest{})
		assert.NoError(t, err)
		assert.Len(t, resp.Clients, 1)
		// Defensive code should create ClientInfo
		assert.NotNil(t, resp.Clients[0].ClientInfo)
		assert.NotNil(t, resp.Clients[0].ClientInfo.Reserved)
		assert.Equal(t, "test-nil-client", resp.Clients[0].ClientInfo.Reserved["client_id"])
	})
}

// TestCloneOperationMetricsNilBranches tests nil handling in cloneOperationMetrics
func TestCloneOperationMetricsNilBranches(t *testing.T) {
	t.Run("returns nil for nil input", func(t *testing.T) {
		result := cloneOperationMetrics(nil)
		assert.Nil(t, result)
	})

	t.Run("returns nil for empty slice", func(t *testing.T) {
		result := cloneOperationMetrics([]*commonpb.OperationMetrics{})
		assert.Nil(t, result)
	})

	t.Run("handles nil element in slice", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "search",
				Global:    &commonpb.Metrics{RequestCount: 100},
			},
			nil, // nil element
			{
				Operation: "insert",
				Global:    &commonpb.Metrics{RequestCount: 50},
			},
		}
		result := cloneOperationMetrics(metrics)
		assert.NotNil(t, result)
		assert.Len(t, result, 3)
		assert.Nil(t, result[1]) // nil should be preserved
	})

	t.Run("handles operations with nil Global", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "search",
				Global:    nil,
				CollectionMetrics: map[string]*commonpb.Metrics{
					"col1": {RequestCount: 10},
				},
			},
		}
		result := cloneOperationMetrics(metrics)
		assert.NotNil(t, result)
		assert.Len(t, result, 1)
		assert.Nil(t, result[0].Global)
		assert.NotNil(t, result[0].CollectionMetrics)
	})

	t.Run("handles operations with nil CollectionMetrics", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "insert",
				Global: &commonpb.Metrics{
					RequestCount: 100,
				},
				CollectionMetrics: nil,
			},
		}
		result := cloneOperationMetrics(metrics)
		assert.NotNil(t, result)
		assert.Len(t, result, 1)
		assert.NotNil(t, result[0].Global)
		assert.Nil(t, result[0].CollectionMetrics)
	})
}

// TestCloneClientInfoNilBranches tests nil handling in cloneClientInfo
func TestCloneClientInfoNilBranches(t *testing.T) {
	t.Run("returns nil for nil input", func(t *testing.T) {
		result := cloneClientInfo(nil)
		assert.Nil(t, result)
	})

	t.Run("handles ClientInfo with nil Reserved", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType:    "test",
			SdkVersion: "1.0",
			Reserved:   nil,
		}
		result := cloneClientInfo(info)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.SdkType)
		assert.Nil(t, result.Reserved)
	})

	t.Run("clones Reserved map", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType: "test",
			Reserved: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}
		result := cloneClientInfo(info)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Reserved)
		assert.Equal(t, "value1", result.Reserved["key1"])

		// Verify it's a deep copy
		result.Reserved["key1"] = "modified"
		assert.Equal(t, "value1", info.Reserved["key1"])
	})
}

// TestGetDatabaseFromClientInfoEdgeCases2 tests more edge cases in getDatabaseFromClientInfo
func TestGetDatabaseFromClientInfoEdgeCases2(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	t.Run("returns empty for nil info", func(t *testing.T) {
		db := mgr.getDatabaseFromClientInfo(nil)
		assert.Equal(t, "", db)
	})

	t.Run("returns empty for nil reserved", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType:  "test",
			Reserved: nil,
		}
		db := mgr.getDatabaseFromClientInfo(info)
		assert.Equal(t, "", db)
	})

	t.Run("returns empty for empty db_name", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType: "test",
			Reserved: map[string]string{
				"db_name": "",
			},
		}
		db := mgr.getDatabaseFromClientInfo(info)
		assert.Equal(t, "", db)
	})

	t.Run("returns database from database key", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType: "test",
			Reserved: map[string]string{
				"database": "alt_database",
			},
		}
		db := mgr.getDatabaseFromClientInfo(info)
		assert.Equal(t, "alt_database", db)
	})

	t.Run("trims whitespace from db_name", func(t *testing.T) {
		info := &commonpb.ClientInfo{
			SdkType: "test",
			Reserved: map[string]string{
				"db_name": "   trimmed_db   ",
			},
		}
		db := mgr.getDatabaseFromClientInfo(info)
		assert.Equal(t, "trimmed_db", db)
	})
}

// TestProcessCommandRepliesWithNilReply tests handling of nil replies in the slice
func TestProcessCommandRepliesWithNilReply(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	// Register client
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "test-sdk",
			Reserved: map[string]string{
				"client_id": "nil-reply-client",
			},
		},
	})

	// Send heartbeat with nil reply in the slice
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "test-sdk",
			Reserved: map[string]string{
				"client_id": "nil-reply-client",
			},
		},
		CommandReplies: []*commonpb.CommandReply{
			{
				CommandId: "cmd-1",
				Success:   true,
			},
			nil, // nil reply should be skipped
			{
				CommandId: "cmd-2",
				Success:   true,
			},
		},
	})

	// Verify only non-nil replies were stored
	replies := mgr.GetClientCommandReplies("nil-reply-client")
	assert.Len(t, replies, 2)
}

// TestGetCommandsForClientWithIDErrorPaths tests error handling in getCommandsForClientWithID
func TestGetCommandsForClientWithIDErrorPaths(t *testing.T) {
	t.Run("handles ListCommands error", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mockStore.shouldFailList = true
		mgr.SetCommandStore(mockStore)

		// First register a client
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "error-test-client",
				},
			},
		})

		// Heartbeat should still succeed but return no commands
		resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "error-test-client",
				},
			},
		})
		assert.NoError(t, err)
		assert.Empty(t, resp.Commands)
	})

	t.Run("handles ListConfigs error", func(t *testing.T) {
		mgr := NewTelemetryManager(nil)
		mockStore := newMockCommandStore()
		mockStore.shouldFailListConfig = true
		mgr.SetCommandStore(mockStore)

		// First register a client
		mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "error-test-client-2",
				},
			},
		})

		// Heartbeat should still succeed but return no commands
		resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "test-sdk",
				Reserved: map[string]string{
					"client_id": "error-test-client-2",
				},
			},
		})
		assert.NoError(t, err)
		assert.Empty(t, resp.Commands)
	})
}

// TestHandleHeartbeatAccessedDatabasesEmpty tests that adding to empty AccessedDatabases works
func TestHandleHeartbeatAccessedDatabasesEmpty(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	// First, register a client (AccessedDatabases starts as empty sync.Map)
	mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "test-sdk",
			Reserved: map[string]string{
				"client_id": "access-db-test",
			},
		},
	})

	// Now send heartbeat with db_name - should work on empty sync.Map
	resp, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "test-sdk",
			Reserved: map[string]string{
				"client_id": "access-db-test",
				"db_name":   "new_database",
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// Verify database was added
	existing, loaded := mgr.clientMetrics.Load("access-db-test")
	assert.True(t, loaded)
	cache := existing.(*ClientMetricsCache)
	_, exists := cache.AccessedDatabases.Load("new_database")
	assert.True(t, exists)
}

// TestNewTelemetryManagerWithConfigNilConfig tests nil config handling
func TestNewTelemetryManagerWithConfigNilConfig(t *testing.T) {
	// Test with nil config - should use default
	mgr := NewTelemetryManagerWithConfig(nil, nil)
	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.config)
	assert.Equal(t, DefaultTelemetryConfig().ClientStatusThreshold, mgr.config.ClientStatusThreshold)
}

// TestCleanupRepliedCommandsNilCommandStore tests cleanup when commandStore is nil
func TestCleanupRepliedCommandsNilCommandStore(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	// commandStore is nil by default

	// Should return early without panic
	mgr.cleanupRepliedCommands([]string{"cmd1", "cmd2"})
}

// TestCleanupRepliedCommandsEmptyIDs tests cleanup with empty string IDs
func TestCleanupRepliedCommandsEmptyIDs(t *testing.T) {
	mockStore := newMockCommandStore()
	mgr := NewTelemetryManager(nil)
	mgr.SetCommandStore(mockStore)

	// Add a command using the mock store directly
	ctx := context.Background()
	cmdID, _ := mockStore.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "test_command",
	})

	// Cleanup with empty strings and valid ID - should skip empty strings
	mgr.cleanupRepliedCommands([]string{"", cmdID, ""})

	// Valid command should be deleted, empty strings skipped
	_, _, _, found := mockStore.GetCommandInfo(cmdID)
	assert.False(t, found)
}

// TestCloneOperationMetricsWithNilElement tests cloning metrics slice with nil element
func TestCloneOperationMetricsWithNilElement(t *testing.T) {
	metrics := []*commonpb.OperationMetrics{
		{
			Operation: "Search",
			Global: &commonpb.Metrics{
				RequestCount: 100,
			},
		},
		nil, // nil element in middle
		{
			Operation: "Insert",
			Global: &commonpb.Metrics{
				RequestCount: 50,
			},
		},
	}

	cloned := cloneOperationMetrics(metrics)

	assert.Len(t, cloned, 3)
	assert.NotNil(t, cloned[0])
	assert.Equal(t, "Search", cloned[0].Operation)
	assert.Nil(t, cloned[1]) // nil element preserved
	assert.NotNil(t, cloned[2])
	assert.Equal(t, "Insert", cloned[2].Operation)
}

// TestCloneClientInfoNil tests cloneClientInfo with nil input
func TestCloneClientInfoNil(t *testing.T) {
	result := cloneClientInfo(nil)
	assert.Nil(t, result)
}

// TestCloneClientInfoValid tests cloneClientInfo with valid input
func TestCloneClientInfoValid(t *testing.T) {
	info := &commonpb.ClientInfo{
		SdkType:    "test-sdk",
		SdkVersion: "1.0.0",
		Host:       "localhost",
		User:       "testuser",
		Reserved: map[string]string{
			"client_id": "test-client-id",
		},
	}

	cloned := cloneClientInfo(info)

	assert.NotNil(t, cloned)
	assert.Equal(t, info.SdkType, cloned.SdkType)
	assert.Equal(t, info.SdkVersion, cloned.SdkVersion)
	assert.Equal(t, info.Host, cloned.Host)
	assert.Equal(t, info.User, cloned.User)
	assert.Equal(t, info.Reserved["client_id"], cloned.Reserved["client_id"])

	// Ensure it's a deep copy
	cloned.Host = "modified"
	assert.NotEqual(t, info.Host, cloned.Host)
}

// TestCloneOperationMetricsEmpty tests cloning empty metrics slice
func TestCloneOperationMetricsEmpty(t *testing.T) {
	result := cloneOperationMetrics([]*commonpb.OperationMetrics{})
	assert.Nil(t, result)

	result = cloneOperationMetrics(nil)
	assert.Nil(t, result)
}

// TestValidateAndTruncateMetricsEdgeCases3 tests additional edge cases for validation
func TestValidateAndTruncateMetricsEdgeCases3(t *testing.T) {
	mgr := NewTelemetryManager(nil)

	t.Run("nil metrics slice", func(t *testing.T) {
		result := mgr.validateAndTruncateMetrics(nil)
		assert.Nil(t, result)
	})

	t.Run("metrics with nil Global", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global:    nil, // nil Global
			},
		}
		result := mgr.validateAndTruncateMetrics(metrics)
		assert.Len(t, result, 1)
	})

	t.Run("metrics with nil CollectionMetrics entry", func(t *testing.T) {
		metrics := []*commonpb.OperationMetrics{
			{
				Operation: "Search",
				Global: &commonpb.Metrics{
					RequestCount: 100,
				},
				CollectionMetrics: map[string]*commonpb.Metrics{
					"col1": nil, // nil entry
					"col2": {RequestCount: 50},
				},
			},
		}
		result := mgr.validateAndTruncateMetrics(metrics)
		assert.NotNil(t, result)
	})
}

// TestGetCommandsForClientWithIDNilStore2 tests getting commands when store is nil
func TestGetCommandsForClientWithIDNilStore2(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	// commandStore is nil

	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType: "test-sdk",
		},
	}
	commands := mgr.getCommandsForClientWithID("test-client", req)

	assert.Empty(t, commands)
}

// TestProcessCommandRepliesNilStore2 tests processing replies when store is nil
func TestProcessCommandRepliesNilStore2(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	// commandStore is nil

	cache := &ClientMetricsCache{ClientID: "test-client"}
	replies := []*commonpb.CommandReply{
		{
			CommandId: "cmd1",
			Success:   true,
		},
	}

	// Should not panic
	mgr.processCommandReplies(cache, replies)
}

// TestStoredCommandReplyJSONSerialization tests that StoredCommandReply serializes
// Payload as a string (not base64 encoded []byte) for proper JSON API responses
func TestStoredCommandReplyJSONSerialization(t *testing.T) {
	t.Run("payload should be string in JSON", func(t *testing.T) {
		reply := &StoredCommandReply{
			CommandID:      "cmd-123",
			CommandType:    "get_config",
			CommandPayload: `{"setting": "value"}`,
			Success:        true,
			Payload:        `{"user_config": {"address": "localhost"}, "pushed_config": {}}`,
			ReceivedAt:     1234567890,
		}

		// Serialize to JSON
		data, err := json.Marshal(reply)
		assert.NoError(t, err)

		// The payload should NOT be base64 encoded
		// It should appear as a raw JSON string in the output
		jsonStr := string(data)
		assert.Contains(t, jsonStr, `"payload":"{\"user_config\"`)
		assert.NotContains(t, jsonStr, "eyJ1c2VyX2NvbmZpZyI") // base64 prefix for {"user_config

		// Verify it can be deserialized back
		var deserialized StoredCommandReply
		err = json.Unmarshal(data, &deserialized)
		assert.NoError(t, err)
		assert.Equal(t, reply.Payload, deserialized.Payload)
		assert.Equal(t, reply.CommandPayload, deserialized.CommandPayload)
	})

	t.Run("payload in command_replies reserved field should be parseable by frontend", func(t *testing.T) {
		// Simulate what the frontend receives
		replies := []*StoredCommandReply{
			{
				CommandID:   "cmd-123",
				CommandType: "get_config",
				Success:     true,
				Payload:     `{"user_config": {"telemetry_enabled": true}, "pushed_config": {}}`,
				ReceivedAt:  1234567890,
			},
		}

		// Serialize to JSON (this is what goes into Reserved["command_replies"])
		data, err := json.Marshal(replies)
		assert.NoError(t, err)

		// Deserialize to verify structure
		var parsed []map[string]interface{}
		err = json.Unmarshal(data, &parsed)
		assert.NoError(t, err)
		assert.Len(t, parsed, 1)

		// The payload should be a string that can be JSON-parsed
		payloadStr, ok := parsed[0]["payload"].(string)
		assert.True(t, ok, "payload should be a string")

		// Parse the payload string as JSON
		var payloadData map[string]interface{}
		err = json.Unmarshal([]byte(payloadStr), &payloadData)
		assert.NoError(t, err)
		assert.Contains(t, payloadData, "user_config")
		assert.Contains(t, payloadData, "pushed_config")
	})
}

// TestProcessCommandRepliesPayloadFormat tests that command reply payloads are stored as strings
func TestProcessCommandRepliesPayloadFormat(t *testing.T) {
	mgr := NewTelemetryManager(nil)
	mockStore := newMockCommandStore()
	mgr.SetCommandStore(mockStore)

	clientID := "test-client"

	// Register client
	_, err := mgr.HandleHeartbeat(&milvuspb.ClientHeartbeatRequest{
		ClientInfo: &commonpb.ClientInfo{
			Reserved: map[string]string{"client_id": clientID},
		},
	})
	assert.NoError(t, err)

	// Get client cache
	existing, _ := mgr.clientMetrics.Load(clientID)
	cache := existing.(*ClientMetricsCache)

	// Process reply with JSON payload
	jsonPayload := `{"user_config": {"address": "localhost:19530"}, "pushed_config": {"sampling_rate": 0.5}}`
	mgr.processCommandReplies(cache, []*commonpb.CommandReply{
		{
			CommandId: "cmd-get-config",
			Success:   true,
			Payload:   []byte(jsonPayload),
		},
	})

	// Get stored replies and verify format
	replies := mgr.GetClientCommandReplies(clientID)
	assert.Len(t, replies, 1)
	assert.Equal(t, "cmd-get-config", replies[0].CommandID)
	assert.Equal(t, jsonPayload, replies[0].Payload) // Should be string, not []byte

	// Verify JSON serialization produces proper format
	data, err := json.Marshal(replies)
	assert.NoError(t, err)

	// Should not contain base64 encoded content
	jsonStr := string(data)
	assert.Contains(t, jsonStr, `"payload":"{\"user_config\"`)
	assert.NotContains(t, jsonStr, "eyJ1c2VyX2NvbmZpZyI") // base64 prefix
}
