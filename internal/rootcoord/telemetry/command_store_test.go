// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// mockKV implements KVInterface for testing
type mockKV struct {
	mu   sync.Mutex
	data map[string]string

	shouldFailPut    bool
	shouldFailGet    bool
	shouldFailDelete bool
}

func newMockKV() *mockKV {
	return &mockKV{
		data: make(map[string]string),
	}
}

func (m *mockKV) Put(ctx context.Context, key, val string) error {
	if m.shouldFailPut {
		return errors.New("mock put error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val
	return nil
}

func (m *mockKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if m.shouldFailGet {
		return nil, errors.New("mock get error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var kvs []*mvccpb.KeyValue
	// Check if it's a prefix query by looking at the key
	for k, v := range m.data {
		if len(k) >= len(key) && k[:len(key)] == key {
			kvs = append(kvs, &mvccpb.KeyValue{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}
	return &clientv3.GetResponse{
		Kvs: kvs,
	}, nil
}

func (m *mockKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error {
	if m.shouldFailDelete {
		return errors.New("mock delete error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// =============================================================================
// Core Functionality Tests
// =============================================================================

func TestNewCommandStore(t *testing.T) {
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/prefix/")

	assert.NotNil(t, store)
	assert.Equal(t, "/test/prefix/configs/", store.configPath)
	assert.NotNil(t, store.cache)
	assert.Empty(t, store.cache.commands)
	assert.Empty(t, store.cache.configs)
}

// =============================================================================
// Command CRUD Tests (In-Memory Only)
// =============================================================================

func TestCommandPushAndRetrieve(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a command
	cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
		Payload:     []byte(`{"max_count": 50}`),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, cmdID)

	// Commands are NOT stored in etcd (in-memory only)
	assert.Empty(t, mockKv.data, "commands should not be persisted to etcd")

	// Retrieve commands
	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)
	assert.Equal(t, cmdID, commands[0].CommandId)
	assert.Equal(t, "show_errors", commands[0].CommandType)
	assert.Equal(t, []byte(`{"max_count": 50}`), commands[0].Payload)
	assert.Equal(t, "global", commands[0].TargetScope)
}

func TestCommandWithTargetClient(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "collection_metrics",
		TargetClientId: "client-abc",
		Payload:        []byte(`{"collections": ["col1"]}`),
	})
	require.NoError(t, err)

	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)
	assert.Equal(t, cmdID, commands[0].CommandId)
	assert.Equal(t, "client:client-abc", commands[0].TargetScope)
}

func TestCommandWithTargetDatabase(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "update_setting",
		TargetDatabase: "test_db",
		Payload:        []byte(`{"setting": "value"}`),
	})
	require.NoError(t, err)

	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)
	assert.Equal(t, cmdID, commands[0].CommandId)
	assert.Equal(t, "database:test_db", commands[0].TargetScope)
}

func TestCommandWithTargetClientTakesPrecedenceOverDatabase(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// When both TargetClientId and TargetDatabase are set, TargetClientId takes precedence
	cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "update_setting",
		TargetClientId: "client-xyz",
		TargetDatabase: "test_db",
		Payload:        []byte(`{"setting": "value"}`),
	})
	require.NoError(t, err)

	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)
	assert.Equal(t, cmdID, commands[0].CommandId)
	assert.Equal(t, "client:client-xyz", commands[0].TargetScope)
}

func TestCommandDelete(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a command
	cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
	})
	require.NoError(t, err)

	// Verify it exists
	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)

	// Delete it
	err = store.DeleteCommand(ctx, cmdID)
	require.NoError(t, err)

	// Verify it's gone
	commands, err = store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Empty(t, commands)
}

func TestCommandTTLExpiration(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a command with 1 second TTL
	expiredID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "expired_cmd",
		TtlSeconds:  1,
	})
	require.NoError(t, err)

	// Manually set create time to 2 seconds ago (expired)
	store.cacheMu.Lock()
	store.cache.commands[expiredID].CreateTime = time.Now().Add(-2 * time.Second).UnixMilli()
	store.cacheMu.Unlock()

	// Push a valid command (no TTL)
	validID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "valid_cmd",
		TtlSeconds:  0,
	})
	require.NoError(t, err)

	// ListCommands should filter out expired command
	commands, err := store.ListCommands(ctx)
	require.NoError(t, err)
	assert.Len(t, commands, 1)
	assert.Equal(t, validID, commands[0].CommandId)
}

func TestCleanupExpiredCommands(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push an expired command
	expiredID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "expired",
		TtlSeconds:  1,
	})
	store.cacheMu.Lock()
	store.cache.commands[expiredID].CreateTime = time.Now().Add(-2 * time.Second).UnixMilli()
	store.cacheMu.Unlock()

	// Push a valid command
	validID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "valid",
		TtlSeconds:  3600,
	})

	// Run cleanup
	store.CleanupExpiredCommands(ctx)

	// Verify only valid command remains
	store.cacheMu.RLock()
	_, expiredExists := store.cache.commands[expiredID]
	_, validExists := store.cache.commands[validID]
	store.cacheMu.RUnlock()

	assert.False(t, expiredExists, "expired command should be removed")
	assert.True(t, validExists, "valid command should remain")
}

// =============================================================================
// Config CRUD Tests (Persisted to etcd)
// =============================================================================

func TestConfigPushAndRetrieve(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a persistent config
	cfgID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"enabled": true}`),
		Persistent:  true,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, cfgID)

	// Configs ARE stored in etcd
	assert.Len(t, mockKv.data, 1, "configs should be persisted to etcd")

	// Retrieve configs
	configs, hash, err := store.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Len(t, configs, 1)
	assert.Equal(t, cfgID, configs[0].ConfigId)
	assert.Equal(t, "push_config", configs[0].ConfigType)
	assert.NotEmpty(t, hash)
}

func TestConfigDelete(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a config
	cfgID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})
	require.NoError(t, err)
	assert.Len(t, mockKv.data, 1)

	// Delete it
	err = store.DeleteCommand(ctx, cfgID)
	require.NoError(t, err)

	// Verify removed from both cache and etcd
	configs, _, err := store.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Empty(t, configs)
	assert.Empty(t, mockKv.data)
}

func TestOnlyPushConfigCanBePersistent(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// push_config can be persistent
	_, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})
	assert.NoError(t, err)

	// Other command types cannot be persistent
	invalidTypes := []string{"show_errors", "collection_metrics", "custom_command"}
	for _, cmdType := range invalidTypes {
		_, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: cmdType,
			Persistent:  true,
		})
		assert.Error(t, err, "command type %s should not be allowed to be persistent", cmdType)
		assert.Contains(t, err.Error(), "only push_config can be persistent")
	}
}

// =============================================================================
// Config Hash Tests
// =============================================================================

func TestConfigHashConsistency(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Empty configs should have empty hash
	_, hash1, err := store.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Empty(t, hash1)

	// Add a config for client1 (scope: client:client1)
	cfgID1, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		Payload:        []byte(`{"a": 1}`),
		Persistent:     true,
		TargetClientId: "client1",
	})

	_, hash2, err := store.ListConfigs(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, hash2)
	assert.Len(t, hash2, 16)

	// Same configs should produce same hash
	_, hash3, _ := store.ListConfigs(ctx)
	assert.Equal(t, hash2, hash3)

	// Add another config for client2 (different scope) - hash should change
	cfgID2, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		Payload:        []byte(`{"b": 2}`),
		Persistent:     true,
		TargetClientId: "client2",
	})

	_, hash4, _ := store.ListConfigs(ctx)
	assert.NotEqual(t, hash2, hash4, "hash should change when config added")

	// Delete a config - hash should change again
	store.DeleteCommand(ctx, cfgID1)
	_, hash5, _ := store.ListConfigs(ctx)
	assert.NotEqual(t, hash4, hash5, "hash should change when config deleted")

	// Cleanup
	store.DeleteCommand(ctx, cfgID2)
}

// =============================================================================
// Restart Behavior Test (Critical!)
// =============================================================================

func TestRestartBehavior_CommandsLostConfigsPersist(t *testing.T) {
	ctx := context.Background()

	// Use shared mockKV to simulate etcd persistence across restarts
	sharedKV := newMockKV()

	// === First "session" before restart ===
	store1 := NewCommandStoreWithKV(sharedKV, "/test/")

	// Push a non-persistent command (in-memory only)
	cmdID, err := store1.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
		Payload:     []byte(`{"max_count": 100}`),
	})
	require.NoError(t, err)

	// Push a persistent config (stored in etcd)
	cfgID, err := store1.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"enabled": true, "sampling_rate": 0.5}`),
		Persistent:  true,
	})
	require.NoError(t, err)

	// Verify both exist before restart
	commands1, _ := store1.ListCommands(ctx)
	assert.Len(t, commands1, 1, "command should exist before restart")
	assert.Equal(t, cmdID, commands1[0].CommandId)

	configs1, hash1, _ := store1.ListConfigs(ctx)
	assert.Len(t, configs1, 1, "config should exist before restart")
	assert.Equal(t, cfgID, configs1[0].ConfigId)
	assert.NotEmpty(t, hash1)

	// Verify etcd state: only config is persisted
	assert.Len(t, sharedKV.data, 1, "only config should be in etcd")

	// === Simulate restart by creating new store with same etcd ===
	store2 := NewCommandStoreWithKV(sharedKV, "/test/")

	// Commands should be GONE (they were in-memory only)
	commands2, err := store2.ListCommands(ctx)
	require.NoError(t, err)
	assert.Empty(t, commands2, "commands should be lost after restart")

	// Configs should PERSIST (loaded from etcd)
	configs2, hash2, err := store2.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Len(t, configs2, 1, "configs should persist after restart")
	assert.Equal(t, cfgID, configs2[0].ConfigId)
	assert.Equal(t, "push_config", configs2[0].ConfigType)
	assert.Equal(t, []byte(`{"enabled": true, "sampling_rate": 0.5}`), configs2[0].Payload)

	// Hash should be the same after restart
	assert.Equal(t, hash1, hash2, "config hash should be consistent after restart")
}

func TestRestartBehavior_MultipleConfigs(t *testing.T) {
	ctx := context.Background()
	sharedKV := newMockKV()

	// First session: create multiple configs with different scopes
	store1 := NewCommandStoreWithKV(sharedKV, "/test/")

	cfgID1, _ := store1.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		Payload:        []byte(`{"setting": "value1"}`),
		Persistent:     true,
		TargetClientId: "client1",
	})
	cfgID2, _ := store1.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		Payload:        []byte(`{"setting": "value2"}`),
		Persistent:     true,
		TargetClientId: "client2",
	})

	_, hash1, _ := store1.ListConfigs(ctx)

	// Simulate restart
	store2 := NewCommandStoreWithKV(sharedKV, "/test/")

	configs2, hash2, err := store2.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Len(t, configs2, 2, "all configs should persist")

	// Verify both configs are present
	configIDs := make(map[string]bool)
	for _, cfg := range configs2 {
		configIDs[cfg.ConfigId] = true
	}
	assert.True(t, configIDs[cfgID1])
	assert.True(t, configIDs[cfgID2])

	// Hash should be consistent
	assert.Equal(t, hash1, hash2)
}

// =============================================================================
// ListCommandsWithInfo Tests
// =============================================================================

func TestListCommandsWithInfo(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a command
	cmdID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "show_errors",
		TtlSeconds:  3600,
	})

	// Push a config
	cfgID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})

	// List all with info
	infos, err := store.ListCommandsWithInfo(ctx)
	require.NoError(t, err)
	assert.Len(t, infos, 2)

	// Verify command info
	var cmdInfo, cfgInfo *CommandInfoData
	for _, info := range infos {
		if info.CommandID == cmdID {
			cmdInfo = info
		} else if info.CommandID == cfgID {
			cfgInfo = info
		}
	}

	require.NotNil(t, cmdInfo)
	assert.Equal(t, "show_errors", cmdInfo.CommandType)
	assert.False(t, cmdInfo.Persistent)
	assert.Equal(t, int64(3600), cmdInfo.TTLSeconds)

	require.NotNil(t, cfgInfo)
	assert.Equal(t, "push_config", cfgInfo.CommandType)
	assert.True(t, cfgInfo.Persistent)
	assert.Equal(t, int64(0), cfgInfo.TTLSeconds)
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestPushConfigError(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	mockKv.shouldFailPut = true
	store := NewCommandStoreWithKV(mockKv, "/test/")

	_, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "IO failed")
}

func TestDeleteConfigError(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	cfgID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Persistent:  true,
	})
	require.NoError(t, err)

	mockKv.shouldFailDelete = true
	err = store.DeleteCommand(ctx, cfgID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "IO failed")
}

func TestLoadCacheWithFailedGet(t *testing.T) {
	mockKv := newMockKV()
	mockKv.shouldFailGet = true

	// Should not panic, just log warning and return empty cache
	store := NewCommandStoreWithKV(mockKv, "/test/")
	assert.NotNil(t, store)
	assert.Empty(t, store.cache.configs)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push some initial data
	for i := 0; i < 5; i++ {
		store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "show_errors",
		})
	}

	// Concurrent reads
	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			commands, err := store.ListCommands(ctx)
			assert.NoError(t, err)
			assert.Len(t, commands, 5)
		}()
	}

	wg.Wait()
}

// =============================================================================
// Payload Preservation Tests
// =============================================================================

func TestPayloadPreservation(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	testPayloads := [][]byte{
		[]byte(`{"key": "value"}`),
		[]byte(`{"unicode": "✓✗✔"}`),
		[]byte(`{"quotes": "\"quoted\""}`),
		{0, 1, 2, 3, 255}, // binary data
		nil,               // nil payload
		[]byte(""),        // empty payload
	}

	// Use different TargetClientId for each to create separate configs
	for i, payload := range testPayloads {
		_, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType:    "push_config",
			Payload:        payload,
			Persistent:     true,
			TargetClientId: fmt.Sprintf("client%d", i),
		})
		require.NoError(t, err, "failed for payload %d", i)
	}

	configs, _, err := store.ListConfigs(ctx)
	require.NoError(t, err)
	assert.Len(t, configs, len(testPayloads))
}

// =============================================================================
// Cache Behavior Tests
// =============================================================================

func TestCacheNotAffectedByDirectEtcdChanges(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	// Push a config through store
	cfgID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"a": 1}`),
		Persistent:  true,
	})

	configs1, hash1, _ := store.ListConfigs(ctx)
	assert.Len(t, configs1, 1)

	// Directly modify etcd (bypassing store/cache)
	newCfg := storedConfig{
		ConfigID:    "cfg-fake",
		ConfigType:  "push_config",
		Payload:     []byte(`{"fake": 1}`),
		CreateTime:  time.Now().UnixMilli(),
		TargetScope: "global",
	}
	data, _ := json.Marshal(newCfg)
	mockKv.data["/test/configs/cfg-fake"] = string(data)

	// Store should NOT see direct etcd changes (cache is authoritative)
	configs2, hash2, _ := store.ListConfigs(ctx)
	assert.Len(t, configs2, 1, "direct etcd changes should not be visible")
	assert.Equal(t, hash1, hash2)

	// Cleanup
	store.DeleteCommand(ctx, cfgID)
}

// =============================================================================
// DeleteNonPersistentCommand Tests
// =============================================================================

func TestDeleteNonPersistentCommand(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	t.Run("deletes existing non-persistent command", func(t *testing.T) {
		cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_cmd",
			Payload:     []byte(`{"test": true}`),
			Persistent:  false,
		})
		require.NoError(t, err)

		// Verify command exists
		commands, _ := store.ListCommands(ctx)
		assert.Len(t, commands, 1)

		// Delete the command
		deleted := store.DeleteNonPersistentCommand(cmdID)
		assert.True(t, deleted)

		// Verify command is gone
		commands, _ = store.ListCommands(ctx)
		assert.Empty(t, commands)
	})

	t.Run("returns false for non-existent command", func(t *testing.T) {
		deleted := store.DeleteNonPersistentCommand("non-existent-id")
		assert.False(t, deleted)
	})

	t.Run("does not delete persistent configs", func(t *testing.T) {
		cfgID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"config": true}`),
			Persistent:  true,
		})
		require.NoError(t, err)

		// Try to delete - should return false (config not in commands map)
		deleted := store.DeleteNonPersistentCommand(cfgID)
		assert.False(t, deleted)

		// Config should still exist
		configs, _, _ := store.ListConfigs(ctx)
		assert.Len(t, configs, 1)

		// Cleanup
		store.DeleteCommand(ctx, cfgID)
	})
}

// =============================================================================
// GetCommandInfo Tests
// =============================================================================

func TestGetCommandInfo(t *testing.T) {
	ctx := context.Background()
	mockKv := newMockKV()
	store := NewCommandStoreWithKV(mockKv, "/test/")

	t.Run("returns info for non-persistent command", func(t *testing.T) {
		cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "show_errors",
			Payload:     []byte(`{"max_count": 10}`),
			Persistent:  false,
		})
		require.NoError(t, err)

		cmdType, payload, persistent, ok := store.GetCommandInfo(cmdID)
		assert.True(t, ok)
		assert.Equal(t, "show_errors", cmdType)
		assert.Equal(t, []byte(`{"max_count": 10}`), payload)
		assert.False(t, persistent)

		// Cleanup
		store.DeleteNonPersistentCommand(cmdID)
	})

	t.Run("returns info for persistent config", func(t *testing.T) {
		cfgID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"setting": "value"}`),
			Persistent:  true,
		})
		require.NoError(t, err)

		cmdType, payload, persistent, ok := store.GetCommandInfo(cfgID)
		assert.True(t, ok)
		assert.Equal(t, "push_config", cmdType)
		assert.Equal(t, []byte(`{"setting": "value"}`), payload)
		assert.True(t, persistent)

		// Cleanup
		store.DeleteCommand(ctx, cfgID)
	})

	t.Run("returns false for non-existent ID", func(t *testing.T) {
		_, _, _, ok := store.GetCommandInfo("non-existent")
		assert.False(t, ok)
	})
}

// =============================================================================
// mergeJSONPayloads Tests
// =============================================================================

func TestMergeJSONPayloads(t *testing.T) {
	t.Run("returns false for empty new payload", func(t *testing.T) {
		_, ok := mergeJSONPayloads([][]byte{[]byte(`{"a": 1}`)}, []byte{})
		assert.False(t, ok)

		_, ok = mergeJSONPayloads([][]byte{[]byte(`{"a": 1}`)}, nil)
		assert.False(t, ok)
	})

	t.Run("returns false for invalid new payload JSON", func(t *testing.T) {
		_, ok := mergeJSONPayloads([][]byte{[]byte(`{"a": 1}`)}, []byte(`invalid json`))
		assert.False(t, ok)
	})

	t.Run("merges valid payloads", func(t *testing.T) {
		existing := [][]byte{
			[]byte(`{"a": 1, "b": 2}`),
			[]byte(`{"c": 3}`),
		}
		newPayload := []byte(`{"d": 4, "a": 10}`) // a is overwritten

		merged, ok := mergeJSONPayloads(existing, newPayload)
		assert.True(t, ok)

		var result map[string]interface{}
		err := json.Unmarshal(merged, &result)
		require.NoError(t, err)

		assert.Equal(t, float64(10), result["a"]) // overwritten
		assert.Equal(t, float64(2), result["b"])
		assert.Equal(t, float64(3), result["c"])
		assert.Equal(t, float64(4), result["d"])
	})

	t.Run("skips invalid existing payloads", func(t *testing.T) {
		existing := [][]byte{
			[]byte(`{"a": 1}`),
			[]byte(`invalid json`), // should be skipped
			[]byte(`{"b": 2}`),
		}
		newPayload := []byte(`{"c": 3}`)

		merged, ok := mergeJSONPayloads(existing, newPayload)
		assert.True(t, ok)

		var result map[string]interface{}
		err := json.Unmarshal(merged, &result)
		require.NoError(t, err)

		assert.Equal(t, float64(1), result["a"])
		assert.Equal(t, float64(2), result["b"])
		assert.Equal(t, float64(3), result["c"])
	})

	t.Run("merges with empty existing", func(t *testing.T) {
		merged, ok := mergeJSONPayloads([][]byte{}, []byte(`{"x": 100}`))
		assert.True(t, ok)

		var result map[string]interface{}
		err := json.Unmarshal(merged, &result)
		require.NoError(t, err)
		assert.Equal(t, float64(100), result["x"])
	})
}

// =============================================================================
// PushCommand Edge Cases
// =============================================================================

func TestPushCommandEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("merges existing configs with same scope", func(t *testing.T) {
		mockKv := newMockKV()
		store := NewCommandStoreWithKV(mockKv, "/test/")

		// Push first config
		cfg1ID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"setting1": "value1"}`),
			Persistent:  true,
		})
		require.NoError(t, err)

		// Push second config with same scope - should replace first
		cfg2ID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"setting2": "value2"}`),
			Persistent:  true,
		})
		require.NoError(t, err)

		// First config should be deleted, only second should exist
		configs, _, _ := store.ListConfigs(ctx)
		assert.Len(t, configs, 1)
		assert.Equal(t, cfg2ID, configs[0].ConfigId)
		assert.NotEqual(t, cfg1ID, cfg2ID)
	})

	t.Run("handles put error for config", func(t *testing.T) {
		mockKv := newMockKV()
		mockKv.shouldFailPut = true
		store := NewCommandStoreWithKV(mockKv, "/test/")

		_, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Payload:     []byte(`{"test": true}`),
			Persistent:  true,
		})
		assert.Error(t, err)
	})

	t.Run("handles nil payload", func(t *testing.T) {
		mockKv := newMockKV()
		store := NewCommandStoreWithKV(mockKv, "/test/")

		cmdID, err := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "test_cmd",
			Payload:     nil,
			Persistent:  false,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, cmdID)

		// Cleanup
		store.DeleteNonPersistentCommand(cmdID)
	})
}

// =============================================================================
// LoadCache Edge Cases
// =============================================================================

func TestLoadCacheEdgeCases(t *testing.T) {
	t.Run("handles get error during load", func(t *testing.T) {
		mockKv := newMockKV()
		mockKv.shouldFailGet = true

		// Should not panic, just skip loading
		store := NewCommandStoreWithKV(mockKv, "/test/")
		assert.NotNil(t, store)

		// Cache should be empty
		configs, _, _ := store.ListConfigs(context.Background())
		assert.Empty(t, configs)
	})

	t.Run("handles invalid JSON in etcd", func(t *testing.T) {
		mockKv := newMockKV()
		// Pre-populate with invalid JSON
		mockKv.data["/test/configs/bad-config"] = "not valid json"

		// Should not panic, just skip invalid entries
		store := NewCommandStoreWithKV(mockKv, "/test/")
		assert.NotNil(t, store)

		// Cache should be empty (invalid entry skipped)
		configs, _, _ := store.ListConfigs(context.Background())
		assert.Empty(t, configs)
	})
}

// =============================================================================
// ListCommandsWithInfo Edge Cases
// =============================================================================

func TestListCommandsWithInfoEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("includes both commands and configs", func(t *testing.T) {
		mockKv := newMockKV()
		store := NewCommandStoreWithKV(mockKv, "/test/")

		// Add a non-persistent command
		cmdID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "one_time_cmd",
			Persistent:  false,
			TtlSeconds:  3600,
		})

		// Add a persistent config
		cfgID, _ := store.PushCommand(ctx, &milvuspb.PushClientCommandRequest{
			CommandType: "push_config",
			Persistent:  true,
		})

		infos, err := store.ListCommandsWithInfo(ctx)
		require.NoError(t, err)
		assert.Len(t, infos, 2)

		// Find command and config in results
		var foundCmd, foundCfg bool
		for _, info := range infos {
			if info.CommandID == cmdID {
				foundCmd = true
				assert.False(t, info.Persistent)
				assert.Equal(t, int64(3600), info.TTLSeconds)
			}
			if info.CommandID == cfgID {
				foundCfg = true
				assert.True(t, info.Persistent)
				assert.Equal(t, int64(0), info.TTLSeconds)
			}
		}
		assert.True(t, foundCmd, "command not found")
		assert.True(t, foundCfg, "config not found")

		// Cleanup
		store.DeleteNonPersistentCommand(cmdID)
		store.DeleteCommand(ctx, cfgID)
	})

	t.Run("skips expired commands", func(t *testing.T) {
		mockKv := newMockKV()
		store := NewCommandStoreWithKV(mockKv, "/test/")

		// Manually add an expired command to the cache
		store.cacheMu.Lock()
		store.cache.commands["expired-cmd"] = &storedCommand{
			CommandID:   "expired-cmd",
			CommandType: "old_cmd",
			CreateTime:  time.Now().Add(-2 * time.Hour).UnixMilli(),
			TTLSeconds:  1, // 1 second TTL, already expired
		}
		store.cacheMu.Unlock()

		infos, err := store.ListCommandsWithInfo(ctx)
		require.NoError(t, err)

		// Expired command should be skipped
		for _, info := range infos {
			assert.NotEqual(t, "expired-cmd", info.CommandID)
		}
	})
}
