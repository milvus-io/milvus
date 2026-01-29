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

package milvusclient

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestDefaultTelemetryConfig(t *testing.T) {
	config := DefaultTelemetryConfig()
	assert.True(t, config.Enabled)
	assert.Equal(t, 30*time.Second, config.HeartbeatInterval)
	assert.Equal(t, 1.0, config.SamplingRate)
	assert.Equal(t, 100, config.ErrorMaxCount)
}

func TestOperationMetricsCollector(t *testing.T) {
	t.Run("basic recording", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record some operations
		collector.Record("test_collection", 1000, true)  // 1ms success
		collector.Record("test_collection", 2000, true)  // 2ms success
		collector.Record("test_collection", 3000, false) // 3ms error

		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(3), metrics.RequestCount)
		assert.Equal(t, int64(2), metrics.SuccessCount)
		assert.Equal(t, int64(1), metrics.ErrorCount)
		assert.Equal(t, 2.0, metrics.AvgLatencyMs) // (1+2+3)/3 = 2ms
		assert.Equal(t, 3.0, metrics.P99LatencyMs) // P99 is now calculated in GetMetrics()
	})

	t.Run("metrics reset after collection", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		collector.Record("", 1000, true)
		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(1), metrics.RequestCount)

		// Second collection should return nil (no new data)
		metrics = collector.GetMetrics()
		assert.Nil(t, metrics)
	})

	t.Run("per-collection metrics", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		collector.Record("collection_a", 1000, true)
		collector.Record("collection_a", 2000, true)
		collector.Record("collection_b", 3000, false)

		collMetrics := collector.GetCollectionMetrics()
		assert.Len(t, collMetrics, 2)

		metricsA := collMetrics["collection_a"]
		assert.NotNil(t, metricsA)
		assert.Equal(t, int64(2), metricsA.RequestCount)
		assert.Equal(t, int64(2), metricsA.SuccessCount)

		metricsB := collMetrics["collection_b"]
		assert.NotNil(t, metricsB)
		assert.Equal(t, int64(1), metricsB.RequestCount)
		assert.Equal(t, int64(1), metricsB.ErrorCount)
	})

	t.Run("p99 calculation in GetMetrics", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record 100 operations with increasing latency
		for i := 1; i <= 100; i++ {
			collector.Record("", int64(i*1000), true) // 1ms to 100ms
		}

		// P99 should be available via GetP99Latency() (for testing)
		p99 := collector.GetP99Latency()
		assert.True(t, p99 >= 99.0)

		// GetMetrics() now calculates P99 atomically before clearing samples
		// This prevents the race condition where sendHeartbeat could read from a cleared buffer
		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.True(t, metrics.P99LatencyMs >= 99.0) // P99 is now calculated in GetMetrics()
	})
}

func TestClientTelemetryManager(t *testing.T) {
	t.Run("creation with default config", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, nil)
		assert.NotNil(t, manager)
		assert.True(t, manager.config.Enabled)
		assert.Equal(t, 30*time.Second, manager.config.HeartbeatInterval)
	})

	t.Run("creation with custom config", func(t *testing.T) {
		config := &TelemetryConfig{
			Enabled:           false,
			HeartbeatInterval: 60 * time.Second,
			SamplingRate:      0.5,
		}
		manager := NewClientTelemetryManager(nil, config)
		assert.NotNil(t, manager)
		assert.False(t, manager.config.Enabled)
		assert.Equal(t, 60*time.Second, manager.config.HeartbeatInterval)
	})

	t.Run("record operation", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.SamplingRate = 1.0 // 100% sampling for testing
		manager := NewClientTelemetryManager(nil, config)

		startTime := time.Now().Add(-100 * time.Millisecond)
		manager.RecordOperation("Search", "test_collection", startTime, nil)

		// Verify collector was created
		manager.mu.RLock()
		collector, ok := manager.collectors["Search"]
		manager.mu.RUnlock()
		assert.True(t, ok)
		assert.NotNil(t, collector)
	})

	t.Run("record operation disabled", func(t *testing.T) {
		config := &TelemetryConfig{
			Enabled: false,
		}
		manager := NewClientTelemetryManager(nil, config)

		startTime := time.Now()
		manager.RecordOperation("Search", "test_collection", startTime, nil)

		// No collector should be created when disabled
		manager.mu.RLock()
		_, ok := manager.collectors["Search"]
		manager.mu.RUnlock()
		assert.False(t, ok)
	})

	t.Run("start and stop", func(t *testing.T) {
		config := &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 100 * time.Millisecond,
			SamplingRate:      1.0,
		}
		manager := NewClientTelemetryManager(nil, config)

		manager.Start()
		time.Sleep(50 * time.Millisecond)
		manager.Stop()

		// Should not panic on double stop
		manager.Stop()
	})

	t.Run("collect metrics", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.SamplingRate = 1.0 // 100% sampling for testing
		manager := NewClientTelemetryManager(nil, config)

		// Record some operations
		startTime := time.Now().Add(-10 * time.Millisecond)
		manager.RecordOperation("Search", "coll1", startTime, nil)
		manager.RecordOperation("Insert", "coll2", startTime, errors.New("error"))

		metrics := manager.collectMetrics()
		assert.Len(t, metrics, 2)

		// Find Search metrics
		var searchMetrics *OperationMetrics
		for _, m := range metrics {
			if m.Operation == "Search" {
				searchMetrics = m
				break
			}
		}
		assert.NotNil(t, searchMetrics)
		assert.Equal(t, int64(1), searchMetrics.Global.SuccessCount)
	})

	t.Run("command handler registration", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		handlerCalled := false
		manager.RegisterCommandHandler("test_cmd", func(cmd *ClientCommand) *CommandReply {
			handlerCalled = true
			return &CommandReply{
				CommandId: cmd.CommandId,
				Success:   true,
			}
		})

		cmd := &ClientCommand{
			CommandId:   "cmd-1",
			CommandType: "test_cmd",
		}
		reply := manager.handleCommand(cmd)
		assert.True(t, handlerCalled)
		assert.True(t, reply.Success)
		assert.Equal(t, "cmd-1", reply.CommandId)
	})

	t.Run("unknown command handler", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId:   "cmd-1",
			CommandType: "unknown_cmd",
		}
		reply := manager.handleCommand(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "unknown command type")
	})

	t.Run("process commands idempotent", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		commands := []*ClientCommand{
			{
				CommandId:   "cmd-1",
				CommandType: "test",
				CreateTime:  1000,
				Persistent:  true,
			},
			{
				CommandId:   "cmd-2",
				CommandType: "test",
				CreateTime:  2000,
				Persistent:  true,
			},
		}

		// Register a handler
		manager.RegisterCommandHandler("test", func(cmd *ClientCommand) *CommandReply {
			return &CommandReply{
				CommandId: cmd.CommandId,
				Success:   true,
			}
		})

		manager.processCommands(commands)

		// Config hash should be updated for persistent commands
		assert.NotEmpty(t, manager.GetConfigHash())

		// Process the same commands again - should be idempotent
		oldHash := manager.GetConfigHash()
		manager.processCommands(commands)
		assert.Equal(t, oldHash, manager.GetConfigHash())
	})

	t.Run("config hash getters/setters", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.SetConfigHash("test-hash")
		assert.Equal(t, "test-hash", manager.GetConfigHash())
	})

	t.Run("pending replies", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.RegisterCommandHandler("test", func(cmd *ClientCommand) *CommandReply {
			return &CommandReply{
				CommandId: cmd.CommandId,
				Success:   true,
			}
		})

		commands := []*ClientCommand{
			{CommandId: "cmd-1", CommandType: "test", CreateTime: 1000},
		}
		manager.processCommands(commands)

		replies := manager.getPendingReplies()
		assert.Len(t, replies, 1)
		assert.Equal(t, "cmd-1", replies[0].CommandId)

		// Second call should return empty
		replies = manager.getPendingReplies()
		assert.Len(t, replies, 0)
	})
}

func TestProcessProtoCommands(t *testing.T) {
	t.Run("idempotent ack and timestamp update", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmds := []*commonpb.ClientCommand{
			{
				CommandId:   "cmd-1",
				CommandType: "show_errors",
				CreateTime:  1000,
				Persistent:  false,
			},
			{
				CommandId:   "cmd-2",
				CommandType: "show_errors",
				CreateTime:  2000,
				Persistent:  false,
			},
		}

		manager.processProtoCommands(cmds)
		manager.pendingRepliesMu.Lock()
		firstReplyCount := len(manager.pendingReplies)
		manager.pendingRepliesMu.Unlock()
		assert.Equal(t, 2, firstReplyCount)
		assert.Equal(t, int64(2000), manager.lastCommandTimestamp.Load())

		// Process same commands again - should add idempotent acks
		manager.processProtoCommands(cmds)
		manager.pendingRepliesMu.Lock()
		secondReplyCount := len(manager.pendingReplies)
		manager.pendingRepliesMu.Unlock()
		assert.Equal(t, 4, secondReplyCount)
		assert.Equal(t, int64(2000), manager.lastCommandTimestamp.Load())
	})

	t.Run("config hash includes payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmds := []*commonpb.ClientCommand{
			{
				CommandId:   "cfg-1",
				CommandType: "push_config",
				CreateTime:  1000,
				Persistent:  true,
				Payload:     []byte(`{"a":1}`),
			},
			{
				CommandId:   "cfg-2",
				CommandType: "push_config",
				CreateTime:  2000,
				Persistent:  true,
				Payload:     []byte(`{"b":2}`),
			},
		}

		manager.processProtoCommands(cmds)
		expected := manager.calculateProtoConfigHash(cmds)
		assert.Equal(t, expected, manager.GetConfigHash())

		// Change payload and ensure hash changes
		cmds[0].Payload = []byte(`{"a":999}`)
		manager.processProtoCommands(cmds)
		updated := manager.calculateProtoConfigHash(cmds)
		assert.Equal(t, updated, manager.GetConfigHash())
		assert.NotEqual(t, expected, updated)
	})
}

func TestProtoReplySnapshotAndClear(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	manager.pendingRepliesMu.Lock()
	manager.pendingReplies = []*commonpb.CommandReply{
		{CommandId: "r1", Success: true},
		{CommandId: "r2", Success: false},
	}
	manager.pendingRepliesMu.Unlock()

	// Snapshot does not clear
	replies := manager.getPendingProtoRepliesSnapshot()
	assert.Len(t, replies, 2)
	replies = manager.getPendingProtoRepliesSnapshot()
	assert.Len(t, replies, 2)

	// Simulate successful heartbeat -> clear sent replies
	manager.clearPendingProtoReplies(2)
	replies = manager.getPendingProtoRepliesSnapshot()
	assert.Len(t, replies, 0)
}

func TestCollectionMetrics(t *testing.T) {
	cm := &CollectionMetrics{}

	// Verify initial state
	assert.Equal(t, int64(0), cm.requestCount)
	assert.Equal(t, int64(0), cm.successCount)
	assert.Equal(t, int64(0), cm.errorCount)
	assert.Equal(t, int64(0), cm.totalLatency)
}

func TestErrorTruncation(t *testing.T) {
	t.Run("errors within 1MB limit", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record small errors
		for i := 0; i < 10; i++ {
			manager.RecordOperation("Search", "col_a", time.Now().Add(-time.Duration(i)*time.Millisecond), errors.New("small error"))
		}

		// Get errors via command
		cmd := &ClientCommand{CommandId: "test-1", CommandType: "show_errors"}
		reply := manager.handleShowErrors(cmd)

		assert.True(t, reply.Success)
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)
	})

	t.Run("errors truncated to fit 1MB limit", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 30 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     500,
		})

		// Create large error messages
		largeMsg := ""
		for i := 0; i < 3000; i++ {
			largeMsg += "This is a very long error message that contains a lot of text to simulate large errors. "
		}

		// Record many large errors
		for i := 0; i < 100; i++ {
			manager.RecordOperation("Insert", "col_b", time.Now().Add(-time.Duration(i)*time.Millisecond), errors.New(largeMsg))
		}

		// Get errors via command
		cmd := &ClientCommand{CommandId: "test-2", CommandType: "show_errors", Payload: []byte(`{"max_count": 500}`)}
		reply := manager.handleShowErrors(cmd)

		assert.True(t, reply.Success)
		// Verify payload is truncated to fit limit
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)
		// Verify we still have some errors
		assert.Greater(t, len(reply.Payload), 0)
	})

	t.Run("single large error is truncated", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create a very large error message
		largeMsg := ""
		for i := 0; i < 50000; i++ {
			largeMsg += "x"
		}

		manager.RecordOperation("Search", "col_c", time.Now(), errors.New(largeMsg))

		// Get errors via command
		cmd := &ClientCommand{CommandId: "test-3", CommandType: "show_errors"}
		reply := manager.handleShowErrors(cmd)

		assert.True(t, reply.Success)
		// Even with one large error, should be under limit
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)
	})
}

func TestConfigHashUpdateTiming(t *testing.T) {
	t.Run("hash updated only after all commands processed", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Initial hash should be empty
		assert.Equal(t, "", manager.GetConfigHash())

		// Process multiple persistent commands
		commands := []*ClientCommand{
			{CommandId: "cfg1", CommandType: "log_level", Payload: []byte(`{"level": "debug"}`), Persistent: true},
			{CommandId: "cfg2", CommandType: "enable_trace", Payload: []byte(`{"enabled": true}`), Persistent: true},
		}

		manager.processCommands(commands)

		// Hash should be updated after all commands processed
		hash := manager.GetConfigHash()
		assert.NotEmpty(t, hash)

		// Hash should include both commands
		// Verify by processing again - hash should remain the same
		manager.processCommands(commands)
		assert.Equal(t, hash, manager.GetConfigHash())
	})

	t.Run("hash not updated if no persistent commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Process only non-persistent commands
		commands := []*ClientCommand{
			{CommandId: "cmd1", CommandType: "show_errors", Persistent: false},
		}

		manager.processCommands(commands)

		// Hash should remain empty
		assert.Equal(t, "", manager.GetConfigHash())
	})

	t.Run("mixed persistent and non-persistent commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Mix of persistent and non-persistent
		commands := []*ClientCommand{
			{CommandId: "cmd1", CommandType: "show_errors", Persistent: false},
			{CommandId: "cfg1", CommandType: "log_level", Payload: []byte(`{"level": "info"}`), Persistent: true},
			{CommandId: "cmd2", CommandType: "clear_metrics", Persistent: false},
		}

		manager.processCommands(commands)

		// Hash should be calculated based on persistent commands only
		hash := manager.GetConfigHash()
		assert.NotEmpty(t, hash)

		// Add another non-persistent command - hash shouldn't change
		manager.processCommands([]*ClientCommand{
			{CommandId: "cmd3", CommandType: "show_metrics", Persistent: false},
		})
		assert.Equal(t, hash, manager.GetConfigHash())
	})
}

func TestPartialDeliveryScenario(t *testing.T) {
	t.Run("simulated crash during processing", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Initial state: no hash
		assert.Equal(t, "", manager.GetConfigHash())

		// Simulate receiving commands but NOT calling processCommands
		// (simulates crash before processing completes)
		commands := []*ClientCommand{
			{CommandId: "cfg1", CommandType: "log_level", Payload: []byte(`{"level": "debug"}`), Persistent: true, CreateTime: 100},
			{CommandId: "cfg2", CommandType: "enable_trace", Payload: []byte(`{"enabled": true}`), Persistent: true, CreateTime: 100},
		}

		// Process only first command (simulate partial processing)
		manager.handleCommand(commands[0])

		// Hash should still be empty (not updated until all commands processed)
		assert.Equal(t, "", manager.GetConfigHash())

		// Now process all commands properly
		manager.processCommands(commands)

		// Hash should now be updated
		expectedHash := manager.calculateConfigHash(commands)
		assert.Equal(t, expectedHash, manager.GetConfigHash())
	})
}

func TestCollectionMetricsWildcard(t *testing.T) {
	t.Run("enable all collections with wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable all collections using "*" wildcard
		cmd := &ClientCommand{
			CommandId:   "test-1",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": true, "collections": ["*"]}`),
		}
		reply := manager.handleCollectionMetrics(cmd)

		assert.True(t, reply.Success)
		assert.Empty(t, reply.ErrorMessage)

		// Verify allCollectionsEnabled is set
		manager.enabledCollectionsMu.RLock()
		allEnabled := manager.allCollectionsEnabled
		manager.enabledCollectionsMu.RUnlock()
		assert.True(t, allEnabled)
	})

	t.Run("disable all collections with wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// First enable some specific collections
		cmd := &ClientCommand{
			CommandId:   "test-1",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": true, "collections": ["col_a", "col_b"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		// Now disable all using wildcard
		cmd = &ClientCommand{
			CommandId:   "test-2",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": false, "collections": ["*"]}`),
		}
		reply := manager.handleCollectionMetrics(cmd)

		assert.True(t, reply.Success)

		// Verify both allCollectionsEnabled and specific collections are cleared
		manager.enabledCollectionsMu.RLock()
		allEnabled := manager.allCollectionsEnabled
		specificCount := len(manager.enabledCollections)
		manager.enabledCollectionsMu.RUnlock()
		assert.False(t, allEnabled)
		assert.Equal(t, 0, specificCount)
	})

	t.Run("wildcard enables metrics for any collection", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable all collections
		cmd := &ClientCommand{
			CommandId:   "test-1",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": true, "collections": ["*"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		// Record operations on various collections
		manager.RecordOperation("Search", "collection_x", time.Now(), nil)
		manager.RecordOperation("Search", "collection_y", time.Now(), nil)
		manager.RecordOperation("Search", "collection_z", time.Now(), nil)

		// Force snapshot creation
		manager.createSnapshot()

		// Get the snapshot and verify all collections have metrics
		snapshot := manager.GetLatestSnapshot()
		assert.NotNil(t, snapshot)

		// Should have Search operation
		var searchMetrics *OperationMetrics
		for _, m := range snapshot.Metrics {
			if m.Operation == "Search" {
				searchMetrics = m
				break
			}
		}
		assert.NotNil(t, searchMetrics)

		// All three collections should have metrics when wildcard is enabled
		assert.Contains(t, searchMetrics.CollectionMetrics, "collection_x")
		assert.Contains(t, searchMetrics.CollectionMetrics, "collection_y")
		assert.Contains(t, searchMetrics.CollectionMetrics, "collection_z")
	})

	t.Run("query status returns all_collections_enabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable all collections
		cmd := &ClientCommand{
			CommandId:   "test-1",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": true, "collections": ["*"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		// Query status (empty payload)
		queryCmd := &ClientCommand{
			CommandId:   "test-2",
			CommandType: "collection_metrics",
			Payload:     nil,
		}
		reply := manager.handleCollectionMetrics(queryCmd)

		assert.True(t, reply.Success)
		assert.NotNil(t, reply.Payload)

		// Parse response
		var response map[string]interface{}
		err := json.Unmarshal(reply.Payload, &response)
		assert.NoError(t, err)
		assert.True(t, response["all_collections_enabled"].(bool))
	})

	t.Run("specific collections still work alongside wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// First enable specific collections
		cmd := &ClientCommand{
			CommandId:   "test-1",
			CommandType: "collection_metrics",
			Payload:     []byte(`{"enabled": true, "collections": ["col_a"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		// Record operation
		manager.RecordOperation("Search", "col_a", time.Now(), nil)
		manager.RecordOperation("Search", "col_b", time.Now(), nil) // Not explicitly enabled

		// Force snapshot
		manager.createSnapshot()

		snapshot := manager.GetLatestSnapshot()
		assert.NotNil(t, snapshot)

		var searchMetrics *OperationMetrics
		for _, m := range snapshot.Metrics {
			if m.Operation == "Search" {
				searchMetrics = m
				break
			}
		}
		assert.NotNil(t, searchMetrics)

		// Only col_a should have metrics (not col_b since allCollectionsEnabled is false)
		assert.Contains(t, searchMetrics.CollectionMetrics, "col_a")
		assert.NotContains(t, searchMetrics.CollectionMetrics, "col_b")
	})
}

func TestSnapshotAggregation(t *testing.T) {
	t.Run("aggregate multiple snapshots", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create multiple snapshots with known metrics
		// Snapshot 1: Search - 100 requests, 10ms avg
		manager.mu.Lock()
		collector1 := NewOperationMetricsCollector()
		manager.collectors["Search"] = collector1
		manager.mu.Unlock()
		for i := 0; i < 100; i++ {
			collector1.Record("", 10000, true) // 10ms each
		}
		manager.createSnapshot()

		// Snapshot 2: Search - 200 requests, 20ms avg
		manager.mu.Lock()
		collector2 := NewOperationMetricsCollector()
		manager.collectors["Search"] = collector2
		manager.mu.Unlock()
		for i := 0; i < 200; i++ {
			collector2.Record("", 20000, true) // 20ms each
		}
		manager.createSnapshot()

		// Get all snapshots
		snapshots := manager.GetMetricsSnapshots()
		assert.Len(t, snapshots, 2)

		// Aggregate them
		result := manager.aggregateSnapshots(snapshots, 0, time.Now().UnixMilli())

		assert.NotNil(t, result)
		assert.Equal(t, 2, result.SnapshotCount)
		assert.NotNil(t, result.Aggregated)

		searchMetrics := result.Aggregated.Metrics["Search"]
		assert.NotNil(t, searchMetrics)

		// Total requests should be 300
		assert.Equal(t, int64(300), searchMetrics.RequestCount)
		assert.Equal(t, int64(300), searchMetrics.SuccessCount)
		assert.Equal(t, int64(0), searchMetrics.ErrorCount)

		// Weighted average: (100*10 + 200*20) / 300 = 5000/300 = 16.67ms
		expectedAvg := (100.0*10.0 + 200.0*20.0) / 300.0
		assert.InDelta(t, expectedAvg, searchMetrics.AvgLatencyMs, 0.01)
	})

	t.Run("aggregate empty snapshots", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Aggregate with no snapshots
		result := manager.aggregateSnapshots(nil, 0, time.Now().UnixMilli())

		assert.NotNil(t, result)
		assert.Equal(t, 0, result.SnapshotCount)
		assert.NotNil(t, result.Aggregated)
		assert.Empty(t, result.Aggregated.Metrics)
	})

	t.Run("show_latency_history with aggregate flag", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create a snapshot
		manager.mu.Lock()
		collector := NewOperationMetricsCollector()
		manager.collectors["Query"] = collector
		manager.mu.Unlock()
		for i := 0; i < 50; i++ {
			collector.Record("", 5000, true) // 5ms each
		}
		manager.createSnapshot()

		// Query with detail=false (aggregated mode)
		now := time.Now()
		oneHourAgo := now.Add(-1 * time.Hour)
		payload := LatencyHistoryPayload{
			StartTime: oneHourAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
			Detail:    false,
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId:   "test-agg",
			CommandType: "show_latency_history",
			Payload:     payloadBytes,
		}

		reply := manager.handleShowLatencyHistory(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)

		// Parse response as aggregated
		var aggResponse AggregatedLatencyHistoryResponse
		err := json.Unmarshal(reply.Payload, &aggResponse)
		assert.NoError(t, err)
		assert.Equal(t, 1, aggResponse.SnapshotCount)
		assert.NotNil(t, aggResponse.Aggregated)
		assert.Contains(t, aggResponse.Aggregated.Metrics, "Query")
	})

	t.Run("show_latency_history without aggregate flag", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create a snapshot
		manager.mu.Lock()
		collector := NewOperationMetricsCollector()
		manager.collectors["Insert"] = collector
		manager.mu.Unlock()
		for i := 0; i < 30; i++ {
			collector.Record("", 3000, true) // 3ms each
		}
		manager.createSnapshot()

		// Query with detail=true (non-aggregated, show all snapshots)
		now := time.Now()
		oneHourAgo := now.Add(-1 * time.Hour)
		payload := LatencyHistoryPayload{
			StartTime: oneHourAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
			Detail:    true,
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId:   "test-non-agg",
			CommandType: "show_latency_history",
			Payload:     payloadBytes,
		}

		reply := manager.handleShowLatencyHistory(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)

		// Parse response as snapshot list
		var listResponse LatencyHistoryResponse
		err := json.Unmarshal(reply.Payload, &listResponse)
		assert.NoError(t, err)
		assert.Equal(t, 1, listResponse.TotalSnapshots)
		assert.Len(t, listResponse.Snapshots, 1)
	})
}

func TestGetMetricsSnapshot(t *testing.T) {
	t.Run("returns snapshot without resetting", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record some operations
		collector.Record("test_collection", 1000, true)
		collector.Record("test_collection", 2000, true)

		// Get snapshot (should not reset)
		snapshot := collector.GetMetricsSnapshot()
		assert.NotNil(t, snapshot)
		assert.Equal(t, int64(2), snapshot.RequestCount)

		// Get snapshot again (should still have data)
		snapshot2 := collector.GetMetricsSnapshot()
		assert.NotNil(t, snapshot2)
		assert.Equal(t, int64(2), snapshot2.RequestCount)

		// Now call GetMetrics which resets
		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)

		// Snapshot should now return nil
		snapshot3 := collector.GetMetricsSnapshot()
		assert.Nil(t, snapshot3)
	})

	t.Run("returns nil when no data", func(t *testing.T) {
		collector := NewOperationMetricsCollector()
		snapshot := collector.GetMetricsSnapshot()
		assert.Nil(t, snapshot)
	})
}

func TestShouldSample(t *testing.T) {
	t.Run("100% sampling rate", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:      true,
			SamplingRate: 1.0,
		})

		// All operations should be sampled
		for i := 0; i < 100; i++ {
			assert.True(t, manager.shouldSample(1.0))
		}
	})

	t.Run("0% sampling rate", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:      true,
			SamplingRate: 0.0,
		})

		// No operations should be sampled
		for i := 0; i < 100; i++ {
			assert.False(t, manager.shouldSample(0.0))
		}
	})

	t.Run("50% sampling rate", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:      true,
			SamplingRate: 0.5,
		})

		// Roughly 50% should be sampled (deterministic based on counter)
		sampled := 0
		for i := 0; i < 10000; i++ {
			if manager.shouldSample(0.5) {
				sampled++
			}
		}
		// Should be approximately 50% (allow 5% margin)
		ratio := float64(sampled) / 10000.0
		assert.InDelta(t, 0.5, ratio, 0.05)
	})

	t.Run("very low sampling rate", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Very low rate should still sample occasionally
		sampled := 0
		for i := 0; i < 100000; i++ {
			if manager.shouldSample(0.001) { // 0.1%
				sampled++
			}
		}
		// Should be approximately 0.1%
		ratio := float64(sampled) / 100000.0
		assert.InDelta(t, 0.001, ratio, 0.001)
	})

	t.Run("threshold zero returns false", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		// A very small rate that results in threshold=0
		assert.False(t, manager.shouldSample(0.00001))
	})
}

func TestIsReady(t *testing.T) {
	t.Run("not ready before start", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		assert.False(t, manager.IsReady())
	})

	t.Run("ready after start", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 100 * time.Millisecond,
		})
		manager.Start()
		defer manager.Stop()

		assert.True(t, manager.IsReady())
	})

	t.Run("ready when disabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled: false,
		})
		manager.Start()
		defer manager.Stop()

		// Should be ready even when disabled
		assert.True(t, manager.IsReady())
	})
}

func TestRecordOperationWithRequestID(t *testing.T) {
	t.Run("records with request ID", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.SamplingRate = 1.0
		manager := NewClientTelemetryManager(nil, config)

		startTime := time.Now().Add(-10 * time.Millisecond)
		manager.RecordOperationWithRequestID("Search", "test_collection", "req-123", startTime, errors.New("test error"))

		// Verify collector was created
		manager.mu.RLock()
		collector, ok := manager.collectors["Search"]
		manager.mu.RUnlock()
		assert.True(t, ok)
		assert.NotNil(t, collector)

		// Verify error was recorded with request ID
		errors := manager.GetRecentErrors(10)
		assert.Len(t, errors, 1)
		assert.Equal(t, "req-123", errors[0].RequestID)
	})

	t.Run("disabled telemetry does nothing", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled: false,
		})

		startTime := time.Now()
		manager.RecordOperationWithRequestID("Search", "test_collection", "req-123", startTime, nil)

		manager.mu.RLock()
		_, ok := manager.collectors["Search"]
		manager.mu.RUnlock()
		assert.False(t, ok)
	})

	t.Run("with collection metrics enabled", func(t *testing.T) {
		config := DefaultTelemetryConfig()
		config.SamplingRate = 1.0
		manager := NewClientTelemetryManager(nil, config)

		// Enable collection metrics for test_collection
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": true, "collections": ["test_collection"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		startTime := time.Now().Add(-5 * time.Millisecond)
		manager.RecordOperationWithRequestID("Query", "test_collection", "req-456", startTime, nil)

		// Force snapshot
		manager.createSnapshot()

		snapshot := manager.GetLatestSnapshot()
		assert.NotNil(t, snapshot)

		var queryMetrics *OperationMetrics
		for _, m := range snapshot.Metrics {
			if m.Operation == "Query" {
				queryMetrics = m
				break
			}
		}
		assert.NotNil(t, queryMetrics)
		assert.Contains(t, queryMetrics.CollectionMetrics, "test_collection")
	})
}

func TestGetRecentErrorsManager(t *testing.T) {
	t.Run("returns recent errors", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record some errors
		for i := 0; i < 5; i++ {
			manager.RecordOperation("Search", "col", time.Now(), errors.Errorf("error %d", i))
		}

		errors := manager.GetRecentErrors(10)
		assert.Len(t, errors, 5)
	})

	t.Run("respects max count", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record many errors
		for i := 0; i < 20; i++ {
			manager.RecordOperation("Insert", "col", time.Now(), errors.Errorf("error %d", i))
		}

		errors := manager.GetRecentErrors(5)
		assert.Len(t, errors, 5)
	})

	t.Run("nil error collector returns nil", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		manager.errorCollector = nil

		errors := manager.GetRecentErrors(10)
		assert.Nil(t, errors)
	})
}

func TestPublicHandleMethods(t *testing.T) {
	t.Run("HandlePushConfigCommand", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"sampling_rate": 0.5}`),
		}
		reply := manager.HandlePushConfigCommand(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		rate := manager.config.SamplingRate
		manager.configMu.RUnlock()
		assert.Equal(t, 0.5, rate)
	})

	t.Run("HandleCollectionMetricsCommand", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": true, "collections": ["col_a"]}`),
		}
		reply := manager.HandleCollectionMetricsCommand(cmd)
		assert.True(t, reply.Success)

		manager.enabledCollectionsMu.RLock()
		enabled := manager.enabledCollections["col_a"]
		manager.enabledCollectionsMu.RUnlock()
		assert.True(t, enabled)
	})

	t.Run("HandleShowErrorsCommand", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record an error
		manager.RecordOperation("Search", "col", time.Now(), errors.New("test error"))

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.HandleShowErrorsCommand(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)
	})
}

func TestHandlePushConfigEdgeCases(t *testing.T) {
	t.Run("invalid payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{invalid json}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "failed to parse")
	})

	t.Run("negative heartbeat interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"heartbeat_interval_ms": -1000}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "must be positive")
	})

	t.Run("zero heartbeat interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"heartbeat_interval_ms": 0}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "must be positive")
	})

	t.Run("sampling rate clamped to min", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"sampling_rate": -0.5}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		rate := manager.config.SamplingRate
		manager.configMu.RUnlock()
		assert.Equal(t, 0.0, rate) // Clamped to 0
	})

	t.Run("sampling rate clamped to max", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"sampling_rate": 1.5}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		rate := manager.config.SamplingRate
		manager.configMu.RUnlock()
		assert.Equal(t, 1.0, rate) // Clamped to 1
	})

	t.Run("enable/disable telemetry", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Disable
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": false}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		enabled := manager.config.Enabled
		manager.configMu.RUnlock()
		assert.False(t, enabled)

		// Re-enable
		cmd = &ClientCommand{
			CommandId: "test-2",
			Payload:   []byte(`{"enabled": true}`),
		}
		reply = manager.handlePushConfig(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		enabled = manager.config.Enabled
		manager.configMu.RUnlock()
		assert.True(t, enabled)
	})

	t.Run("valid heartbeat interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"heartbeat_interval_ms": 60000}`),
		}
		reply := manager.handlePushConfig(cmd)
		assert.True(t, reply.Success)

		manager.configMu.RLock()
		interval := manager.config.HeartbeatInterval
		manager.configMu.RUnlock()
		assert.Equal(t, 60*time.Second, interval)
	})
}

func TestHandleGetConfigEdgeCases(t *testing.T) {
	t.Run("nil client returns empty user config", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.handleGetConfig(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)

		var response GetConfigResponse
		err := json.Unmarshal(reply.Payload, &response)
		assert.NoError(t, err)

		// Should still have telemetry config even with nil client
		assert.Contains(t, response.UserConfig, "telemetry_enabled")
		assert.Contains(t, response.UserConfig, "telemetry_heartbeat_interval_ms")
		assert.Contains(t, response.UserConfig, "telemetry_sampling_rate")
		assert.Contains(t, response.UserConfig, "enabled_collections")

		// Should not have client-specific configs since client is nil
		assert.NotContains(t, response.UserConfig, "address")
	})

	t.Run("enabled collections as list", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Set specific collections
		manager.enabledCollectionsMu.Lock()
		manager.allCollectionsEnabled = false
		manager.enabledCollections = map[string]bool{
			"collection1": true,
			"collection2": true,
		}
		manager.enabledCollectionsMu.Unlock()

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.handleGetConfig(cmd)
		assert.True(t, reply.Success)

		var response GetConfigResponse
		err := json.Unmarshal(reply.Payload, &response)
		assert.NoError(t, err)

		// Should have collections as a list (always []interface{} after JSON unmarshal)
		collections, ok := response.UserConfig["enabled_collections"].([]interface{})
		assert.True(t, ok)
		assert.Len(t, collections, 2)

		// all_collections_enabled should be false
		allEnabled, ok := response.UserConfig["all_collections_enabled"].(bool)
		assert.True(t, ok)
		assert.False(t, allEnabled)
	})

	t.Run("all collections enabled as wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Set all collections enabled
		manager.enabledCollectionsMu.Lock()
		manager.allCollectionsEnabled = true
		manager.enabledCollectionsMu.Unlock()

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.handleGetConfig(cmd)
		assert.True(t, reply.Success)

		var response GetConfigResponse
		err := json.Unmarshal(reply.Payload, &response)
		assert.NoError(t, err)

		// Should have ["*"] for all collections (consistent []interface{} type)
		collections, ok := response.UserConfig["enabled_collections"].([]interface{})
		assert.True(t, ok)
		assert.Len(t, collections, 1)
		assert.Equal(t, "*", collections[0])

		// all_collections_enabled should be true
		allEnabled, ok := response.UserConfig["all_collections_enabled"].(bool)
		assert.True(t, ok)
		assert.True(t, allEnabled)
	})
}

func TestHandleShowLatencyHistoryEdgeCases(t *testing.T) {
	t.Run("missing payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   nil,
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "payload is required")
	})

	t.Run("invalid json payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{invalid}`),
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "failed to parse")
	})

	t.Run("invalid start_time format", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		payload := `{"start_time": "invalid", "end_time": "2024-01-01T00:00:00Z"}`
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(payload),
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "invalid start_time format")
	})

	t.Run("invalid end_time format", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		payload := `{"start_time": "2024-01-01T00:00:00Z", "end_time": "invalid"}`
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(payload),
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "invalid end_time format")
	})

	t.Run("time range exceeds 1 hour", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		now := time.Now()
		twoHoursAgo := now.Add(-2 * time.Hour)
		payload := LatencyHistoryPayload{
			StartTime: twoHoursAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   payloadBytes,
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "cannot exceed 1 hour")
	})

	t.Run("end_time before start_time", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		now := time.Now()
		oneHourAgo := now.Add(-1 * time.Hour)
		payload := LatencyHistoryPayload{
			StartTime: now.Format(time.RFC3339),
			EndTime:   oneHourAgo.Format(time.RFC3339),
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   payloadBytes,
		}
		reply := manager.handleShowLatencyHistory(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "end_time must be after start_time")
	})
}

func TestHandleShowErrorsEdgeCases(t *testing.T) {
	t.Run("invalid payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{invalid}`),
		}
		reply := manager.handleShowErrors(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "failed to parse")
	})

	t.Run("custom max count", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record more errors than requested
		for i := 0; i < 10; i++ {
			manager.RecordOperation("Search", "col", time.Now(), errors.Errorf("error %d", i))
		}

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"max_count": 5}`),
		}
		reply := manager.handleShowErrors(cmd)
		assert.True(t, reply.Success)

		var errors []*ErrorInfo
		err := json.Unmarshal(reply.Payload, &errors)
		assert.NoError(t, err)
		assert.Len(t, errors, 5)
	})

	t.Run("no errors recorded", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.handleShowErrors(cmd)
		assert.True(t, reply.Success)
		// Payload should be nil or empty when no errors
		assert.True(t, len(reply.Payload) == 0 || string(reply.Payload) == "null")
	})
}

func TestClearPendingProtoRepliesEdgeCases(t *testing.T) {
	t.Run("clear zero replies", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.pendingRepliesMu.Lock()
		manager.pendingReplies = []*commonpb.CommandReply{
			{CommandId: "r1", Success: true},
		}
		manager.pendingRepliesMu.Unlock()

		manager.clearPendingProtoReplies(0)

		manager.pendingRepliesMu.Lock()
		count := len(manager.pendingReplies)
		manager.pendingRepliesMu.Unlock()
		assert.Equal(t, 1, count) // Should not clear anything
	})

	t.Run("clear partial replies", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.pendingRepliesMu.Lock()
		manager.pendingReplies = []*commonpb.CommandReply{
			{CommandId: "r1", Success: true},
			{CommandId: "r2", Success: true},
			{CommandId: "r3", Success: true},
		}
		manager.pendingRepliesMu.Unlock()

		manager.clearPendingProtoReplies(2)

		replies := manager.getPendingProtoRepliesSnapshot()
		assert.Len(t, replies, 1)
		assert.Equal(t, "r3", replies[0].CommandId)
	})
}

func TestSnapshotEnabledCollections(t *testing.T) {
	t.Run("all collections enabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.enabledCollectionsMu.Lock()
		manager.allCollectionsEnabled = true
		manager.enabledCollectionsMu.Unlock()

		snapshot, allEnabled := manager.snapshotEnabledCollections()
		assert.Nil(t, snapshot)
		assert.True(t, allEnabled)
	})

	t.Run("no collections enabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		snapshot, allEnabled := manager.snapshotEnabledCollections()
		assert.Nil(t, snapshot)
		assert.False(t, allEnabled)
	})

	t.Run("specific collections enabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		manager.enabledCollectionsMu.Lock()
		manager.enabledCollections["col_a"] = true
		manager.enabledCollections["col_b"] = true
		manager.enabledCollectionsMu.Unlock()

		snapshot, allEnabled := manager.snapshotEnabledCollections()
		assert.NotNil(t, snapshot)
		assert.False(t, allEnabled)
		assert.True(t, snapshot["col_a"])
		assert.True(t, snapshot["col_b"])
	})
}

func TestCalculateP99WithBufferWrap(t *testing.T) {
	t.Run("buffer wrap scenario", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record more than buffer size to trigger wrap
		// Buffer size is 1000
		for i := 0; i < 1500; i++ {
			collector.Record("", int64(i*1000), true) // Latencies from 0 to 1499ms
		}

		// P99 should be calculated correctly from the latest 1000 samples
		// Latest samples are 500-1499ms, so P99 should be around 1489ms
		p99 := collector.GetP99Latency()
		assert.True(t, p99 >= 1400.0, "P99 should be >= 1400ms, got %f", p99)
	})

	t.Run("calculateP99FromSamples with wrap - copies full buffer", func(t *testing.T) {
		samples := make([]int64, 100)
		for i := 0; i < 100; i++ {
			samples[i] = int64(i * 1000) // 0 to 99ms in microseconds
		}

		// totalSamples > bufferSize scenario - triggers the else branch that copies full buffer
		p99 := calculateP99FromSamples(samples, 150, 100)
		assert.True(t, p99 >= 99000.0) // P99 should be around 99ms in microseconds
	})

	t.Run("calculateP99FromSamples zero samples", func(t *testing.T) {
		samples := make([]int64, 100)
		p99 := calculateP99FromSamples(samples, 0, 100)
		assert.Equal(t, 0.0, p99)
	})

	t.Run("calculateP99FromSamples within buffer", func(t *testing.T) {
		samples := make([]int64, 100)
		for i := 0; i < 50; i++ {
			samples[i] = int64(i * 1000)
		}

		// totalSamples < bufferSize - uses the first branch
		p99 := calculateP99FromSamples(samples, 50, 100)
		assert.True(t, p99 >= 49000.0)
	})

	t.Run("calculateP99FromSamples index clamping", func(t *testing.T) {
		// Single sample case - index calculation might exceed length
		samples := []int64{5000}
		p99 := calculateP99FromSamples(samples, 1, 1)
		assert.Equal(t, 5000.0, p99)
	})

	t.Run("calculateP99 with buffer wrap", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Fill beyond buffer capacity to trigger wrap
		for i := 0; i < 1200; i++ {
			collector.Record("", int64((i%100)*1000), true)
		}

		// Get metrics triggers P99 calculation with wrapped buffer
		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.True(t, metrics.P99LatencyMs >= 0)
	})

	t.Run("GetMetrics with wrapped buffer", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record more than buffer capacity
		for i := 0; i < 1100; i++ {
			collector.Record("", int64(i*100), true)
		}

		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(1100), metrics.RequestCount)
		assert.True(t, metrics.P99LatencyMs > 0)
	})
}

func TestCollectProtoMetrics(t *testing.T) {
	t.Run("collects metrics in proto format", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable collection metrics for a specific collection
		manager.enabledCollectionsMu.Lock()
		manager.enabledCollections["test_col"] = true
		manager.enabledCollectionsMu.Unlock()

		// Record some operations
		startTime := time.Now().Add(-10 * time.Millisecond)
		manager.RecordOperation("Search", "test_col", startTime, nil)
		manager.RecordOperation("Search", "test_col", startTime, nil)
		manager.RecordOperation("Insert", "test_col", startTime, errors.New("insert error"))

		metrics := manager.collectProtoMetrics()
		assert.NotEmpty(t, metrics)

		// Find Search metrics
		var searchFound bool
		for _, m := range metrics {
			if m.Operation == "Search" {
				searchFound = true
				assert.Equal(t, int64(2), m.Global.RequestCount)
				assert.Equal(t, int64(2), m.Global.SuccessCount)
				// Collection metrics should be present for enabled collection
				assert.Contains(t, m.CollectionMetrics, "test_col")
			}
		}
		assert.True(t, searchFound)
	})

	t.Run("filters collection metrics by enabled list", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Only enable col_a
		manager.enabledCollectionsMu.Lock()
		manager.enabledCollections["col_a"] = true
		manager.enabledCollectionsMu.Unlock()

		startTime := time.Now().Add(-5 * time.Millisecond)
		manager.RecordOperation("Query", "col_a", startTime, nil)
		manager.RecordOperation("Query", "col_b", startTime, nil) // Not enabled

		metrics := manager.collectProtoMetrics()
		for _, m := range metrics {
			if m.Operation == "Query" {
				// Only col_a should have collection metrics
				assert.Contains(t, m.CollectionMetrics, "col_a")
				assert.NotContains(t, m.CollectionMetrics, "col_b")
			}
		}
	})
}

func TestGetHeartbeatInterval(t *testing.T) {
	t.Run("returns configured interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 45 * time.Second,
		})

		interval := manager.getHeartbeatInterval()
		assert.Equal(t, 45*time.Second, interval)
	})

	t.Run("returns default for zero interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 0,
		})

		interval := manager.getHeartbeatInterval()
		assert.Equal(t, 30*time.Second, interval)
	})

	t.Run("returns default for negative interval", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: -10 * time.Second,
		})

		interval := manager.getHeartbeatInterval()
		assert.Equal(t, 30*time.Second, interval)
	})
}

func TestCalculateProtoConfigHash(t *testing.T) {
	t.Run("empty commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		hash := manager.calculateProtoConfigHash(nil)
		assert.Empty(t, hash)
	})

	t.Run("no persistent commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		commands := []*commonpb.ClientCommand{
			{CommandId: "cmd-1", CommandType: "show_errors", Persistent: false},
		}
		hash := manager.calculateProtoConfigHash(commands)
		assert.Empty(t, hash)
	})

	t.Run("consistent ordering", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Commands in different order should produce same hash
		commands1 := []*commonpb.ClientCommand{
			{CommandId: "a", CommandType: "type_a", Persistent: true, Payload: []byte("payload_a")},
			{CommandId: "b", CommandType: "type_b", Persistent: true, Payload: []byte("payload_b")},
		}
		commands2 := []*commonpb.ClientCommand{
			{CommandId: "b", CommandType: "type_b", Persistent: true, Payload: []byte("payload_b")},
			{CommandId: "a", CommandType: "type_a", Persistent: true, Payload: []byte("payload_a")},
		}

		hash1 := manager.calculateProtoConfigHash(commands1)
		hash2 := manager.calculateProtoConfigHash(commands2)
		assert.Equal(t, hash1, hash2)
	})
}

func TestCalculateConfigHash(t *testing.T) {
	t.Run("empty commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		hash := manager.calculateConfigHash(nil)
		assert.Empty(t, hash)
	})

	t.Run("no persistent commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		commands := []*ClientCommand{
			{CommandId: "cmd-1", CommandType: "show_errors", Persistent: false},
		}
		hash := manager.calculateConfigHash(commands)
		assert.Empty(t, hash)
	})
}

func TestCollectionMetricsEdgeCases(t *testing.T) {
	t.Run("disable specific collections", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// First enable some collections
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": true, "collections": ["col_a", "col_b", "col_c"]}`),
		}
		manager.handleCollectionMetrics(cmd)

		// Now disable specific ones
		cmd = &ClientCommand{
			CommandId: "test-2",
			Payload:   []byte(`{"enabled": false, "collections": ["col_a", "col_b"]}`),
		}
		reply := manager.handleCollectionMetrics(cmd)
		assert.True(t, reply.Success)

		// Only col_c should remain enabled
		manager.enabledCollectionsMu.RLock()
		aEnabled := manager.enabledCollections["col_a"]
		bEnabled := manager.enabledCollections["col_b"]
		cEnabled := manager.enabledCollections["col_c"]
		manager.enabledCollectionsMu.RUnlock()

		assert.False(t, aEnabled)
		assert.False(t, bEnabled)
		assert.True(t, cEnabled)
	})

	t.Run("enable with empty collections returns error", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": true, "collections": []}`),
		}
		reply := manager.handleCollectionMetrics(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "cannot be empty")
	})

	t.Run("disable all with empty collections", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// First enable some
		manager.enabledCollectionsMu.Lock()
		manager.enabledCollections["col_a"] = true
		manager.enabledCollectionsMu.Unlock()

		// Disable all with empty list
		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{"enabled": false, "collections": []}`),
		}
		reply := manager.handleCollectionMetrics(cmd)
		assert.True(t, reply.Success)

		manager.enabledCollectionsMu.RLock()
		count := len(manager.enabledCollections)
		manager.enabledCollectionsMu.RUnlock()
		assert.Equal(t, 0, count)
	})

	t.Run("invalid payload", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   []byte(`{invalid json}`),
		}
		reply := manager.handleCollectionMetrics(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "failed to parse")
	})
}

func TestNewErrorCollectorEdgeCases(t *testing.T) {
	t.Run("negative max count defaults to 100", func(t *testing.T) {
		collector := NewErrorCollector(-5)
		assert.Equal(t, 100, collector.maxCount)
	})

	t.Run("zero max count defaults to 100", func(t *testing.T) {
		collector := NewErrorCollector(0)
		assert.Equal(t, 100, collector.maxCount)
	})
}

func TestGetLatestSnapshotEmpty(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// No snapshots yet
	snapshot := manager.GetLatestSnapshot()
	assert.Nil(t, snapshot)
}

func TestCreateSnapshotDisabled(t *testing.T) {
	manager := NewClientTelemetryManager(nil, &TelemetryConfig{
		Enabled: false,
	})

	// Record some operations (should be ignored due to disabled)
	manager.createSnapshot()

	// No snapshots should be created when disabled
	snapshots := manager.GetMetricsSnapshots()
	assert.Empty(t, snapshots)
}

func TestHandleShowErrorsPayloadEdgeCases(t *testing.T) {
	t.Run("errors json marshal failure recovery", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record normal errors
		for i := 0; i < 5; i++ {
			manager.RecordOperation("Search", "col", time.Now(), errors.Errorf("error %d", i))
		}

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   nil, // No specific max_count
		}
		reply := manager.handleShowErrors(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)
	})

	t.Run("nil error collector", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		manager.errorCollector = nil

		cmd := &ClientCommand{
			CommandId: "test-1",
		}
		reply := manager.handleShowErrors(cmd)
		assert.True(t, reply.Success)
		// Payload should be empty when error collector is nil
	})
}

func TestCollectMetricsEdgeCases(t *testing.T) {
	t.Run("no operations recorded", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		metrics := manager.collectMetrics()
		assert.Empty(t, metrics)
	})

	t.Run("collector returns nil metrics", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create a collector but don't record anything
		manager.mu.Lock()
		manager.collectors["EmptyOp"] = NewOperationMetricsCollector()
		manager.mu.Unlock()

		metrics := manager.collectMetrics()
		// Should be empty because the collector has no data
		assert.Empty(t, metrics)
	})
}

func TestGetCollectionMetricsEdgeCases(t *testing.T) {
	t.Run("collection with no requests", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record operations for some collections
		collector.Record("col_a", 1000, true)
		collector.Record("col_b", 2000, true)

		// Get metrics - should include both collections
		collMetrics := collector.GetCollectionMetrics()
		assert.Len(t, collMetrics, 2)

		// Call again - should be empty (reset)
		collMetrics = collector.GetCollectionMetrics()
		assert.Empty(t, collMetrics)
	})

	t.Run("mixed success and errors", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		collector.Record("col_a", 1000, true)
		collector.Record("col_a", 2000, false)
		collector.Record("col_a", 3000, true)

		collMetrics := collector.GetCollectionMetrics()
		assert.NotNil(t, collMetrics["col_a"])
		assert.Equal(t, int64(3), collMetrics["col_a"].RequestCount)
		assert.Equal(t, int64(2), collMetrics["col_a"].SuccessCount)
		assert.Equal(t, int64(1), collMetrics["col_a"].ErrorCount)
	})
}

func TestCollectProtoMetricsEdgeCases(t *testing.T) {
	t.Run("no collectors", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		metrics := manager.collectProtoMetrics()
		assert.Empty(t, metrics)
	})

	t.Run("collector with no data", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Add an empty collector
		manager.mu.Lock()
		manager.collectors["EmptyOp"] = NewOperationMetricsCollector()
		manager.mu.Unlock()

		metrics := manager.collectProtoMetrics()
		assert.Empty(t, metrics) // Should skip collectors with no data
	})

	t.Run("all collections enabled via wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable all collections
		manager.enabledCollectionsMu.Lock()
		manager.allCollectionsEnabled = true
		manager.enabledCollectionsMu.Unlock()

		// Record operations
		manager.RecordOperation("Search", "any_collection", time.Now().Add(-10*time.Millisecond), nil)

		metrics := manager.collectProtoMetrics()
		assert.NotEmpty(t, metrics)

		// Find Search metrics and verify collection is included
		for _, m := range metrics {
			if m.Operation == "Search" {
				assert.Contains(t, m.CollectionMetrics, "any_collection")
			}
		}
	})
}

func TestMaxLatencyTracking(t *testing.T) {
	t.Run("tracks max latency correctly", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		collector.Record("col", 1000, true)  // 1ms
		collector.Record("col", 5000, true)  // 5ms
		collector.Record("col", 3000, true)  // 3ms
		collector.Record("col", 10000, true) // 10ms - max
		collector.Record("col", 2000, true)  // 2ms

		metrics := collector.GetMetrics()
		assert.Equal(t, 10.0, metrics.MaxLatencyMs)
	})

	t.Run("tracks max latency per collection", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		collector.Record("col_a", 5000, true)  // 5ms
		collector.Record("col_a", 15000, true) // 15ms - max for col_a
		collector.Record("col_b", 20000, true) // 20ms - max for col_b
		collector.Record("col_b", 10000, true) // 10ms

		collMetrics := collector.GetCollectionMetrics()
		assert.Equal(t, 15.0, collMetrics["col_a"].MaxLatencyMs)
		assert.Equal(t, 20.0, collMetrics["col_b"].MaxLatencyMs)
	})
}

func TestBuildClientInfoEdgeCases(t *testing.T) {
	t.Run("with nil client", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		info := manager.buildClientInfo()
		assert.NotNil(t, info)
		assert.Equal(t, "GoMilvusClient", info.SdkType)
		assert.NotEmpty(t, info.Reserved["client_id"])
	})
}

func TestSnapshotTrimming(t *testing.T) {
	t.Run("trims to 120 snapshots", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: time.Millisecond,
			SamplingRate:      1.0,
		})

		// Record an operation first
		manager.RecordOperation("Test", "", time.Now(), nil)

		// Manually add more than 120 snapshots
		for i := 0; i < 130; i++ {
			manager.createSnapshot()
		}

		snapshots := manager.GetMetricsSnapshots()
		assert.Equal(t, 120, len(snapshots))
	})
}

func TestCalculateP99IndexClamping(t *testing.T) {
	t.Run("single sample clamps index correctly", func(t *testing.T) {
		collector := NewOperationMetricsCollector()
		collector.Record("", 5000, true) // Single sample

		p99 := collector.GetP99Latency()
		assert.Equal(t, 5.0, p99) // 5000us = 5ms
	})

	t.Run("two samples clamps index correctly", func(t *testing.T) {
		collector := NewOperationMetricsCollector()
		collector.Record("", 5000, true)  // 5ms
		collector.Record("", 10000, true) // 10ms

		p99 := collector.GetP99Latency()
		// P99 of 2 samples: index = 2*0.99 = 1.98 -> 1
		// But 1 >= len(2), so index = 1, which is valid
		assert.True(t, p99 >= 5.0)
	})
}

func TestGetCollectionMetricsP99Calculation(t *testing.T) {
	t.Run("per-collection P99 calculated correctly", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record 100 operations for accurate P99
		for i := 1; i <= 100; i++ {
			collector.Record("col_a", int64(i*1000), true) // 1-100ms
		}

		collMetrics := collector.GetCollectionMetrics()
		assert.NotNil(t, collMetrics["col_a"])
		// P99 should be around 99ms
		assert.True(t, collMetrics["col_a"].P99LatencyMs >= 99.0)
	})

	t.Run("empty collection after wrap", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record for one collection
		collector.Record("col_a", 1000, true)

		// Get and reset
		collMetrics := collector.GetCollectionMetrics()
		assert.NotNil(t, collMetrics["col_a"])

		// Second call should return empty
		collMetrics = collector.GetCollectionMetrics()
		assert.Empty(t, collMetrics)
	})
}

func TestHandleShowLatencyHistoryResponseSizeLimit(t *testing.T) {
	t.Run("response within size limit", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create a snapshot
		manager.RecordOperation("Search", "", time.Now().Add(-5*time.Millisecond), nil)
		manager.createSnapshot()

		now := time.Now()
		tenMinAgo := now.Add(-10 * time.Minute)
		payload := LatencyHistoryPayload{
			StartTime: tenMinAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
			Detail:    true,
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId: "test-1",
			Payload:   payloadBytes,
		}

		reply := manager.handleShowLatencyHistory(cmd)
		assert.True(t, reply.Success)
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)
	})
}

func TestErrorCollectorRingBuffer(t *testing.T) {
	t.Run("ring buffer wraps correctly", func(t *testing.T) {
		collector := NewErrorCollector(5)

		// Record more than max
		for i := 0; i < 10; i++ {
			collector.RecordError("Search", "col", errors.Errorf("error %d", i).Error(), "")
		}

		// Should only have last 5 errors
		errors := collector.GetRecentErrors(10)
		assert.Len(t, errors, 5)

		// Verify they are the most recent ones
		// Errors 5-9 should be present
		for _, err := range errors {
			assert.Contains(t, err.ErrorMsg, "error")
		}
	})

	t.Run("get fewer than available", func(t *testing.T) {
		collector := NewErrorCollector(10)

		for i := 0; i < 8; i++ {
			collector.RecordError("Insert", "col", errors.Errorf("error %d", i).Error(), "")
		}

		// Request only 3
		errors := collector.GetRecentErrors(3)
		assert.Len(t, errors, 3)
	})
}

func TestSendHeartbeatDisabled(t *testing.T) {
	t.Run("disabled telemetry skips heartbeat", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled: false,
		})

		// This should not panic even with nil client
		manager.sendHeartbeat()
	})

	t.Run("nil client skips heartbeat", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		manager.client = nil

		// This should not panic
		manager.sendHeartbeat()
	})
}

func TestHeartbeatLoopStop(t *testing.T) {
	t.Run("stops cleanly on close", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 10 * time.Millisecond,
		})

		manager.Start()
		time.Sleep(25 * time.Millisecond) // Let a few heartbeats happen
		manager.Stop()

		// Should not hang or panic
		assert.True(t, manager.closed.Load())
	})
}

func TestCollectMetricsMultipleOperations(t *testing.T) {
	t.Run("collects from multiple operation types", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record different operations
		manager.RecordOperation("Search", "col", time.Now().Add(-5*time.Millisecond), nil)
		manager.RecordOperation("Insert", "col", time.Now().Add(-10*time.Millisecond), nil)
		manager.RecordOperation("Query", "col", time.Now().Add(-3*time.Millisecond), errors.New("error"))

		metrics := manager.collectMetrics()
		assert.Len(t, metrics, 3)

		// Verify each operation is present
		ops := make(map[string]bool)
		for _, m := range metrics {
			ops[m.Operation] = true
		}
		assert.True(t, ops["Search"])
		assert.True(t, ops["Insert"])
		assert.True(t, ops["Query"])
	})
}

func TestCalculateP99EdgeCases(t *testing.T) {
	t.Run("exact buffer size samples", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record exactly 1000 samples (buffer size)
		for i := 0; i < 1000; i++ {
			collector.Record("", int64(i*100), true) // 0-99.9ms
		}

		p99 := collector.GetP99Latency()
		// P99 of 1000 samples with values 0-999 (in 100us units)
		// should be around 98.9ms (index 990)
		assert.True(t, p99 >= 98.0, "P99 should be >= 98ms, got %f", p99)
	})

	t.Run("samples just over buffer - triggers wrap branch", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record exactly 1001 samples (buffer size + 1)
		for i := 0; i < 1001; i++ {
			collector.Record("", int64(i*100), true)
		}

		// This triggers the else branch in calculateP99
		p99 := collector.GetP99Latency()
		assert.True(t, p99 > 0)
	})
}

func TestHandleShowErrorsTruncation(t *testing.T) {
	t.Run("iterative truncation with multiple large errors", func(t *testing.T) {
		// Create manager with large error capacity
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 30 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     1000,
		})

		// Create errors with very large messages that will exceed 1MB total
		// Each error with a 50KB message, 25 errors = 1.25MB > 1MB limit
		largeMsg := make([]byte, 50*1024)
		for i := range largeMsg {
			largeMsg[i] = 'x'
		}

		for i := 0; i < 25; i++ {
			manager.RecordOperation("Search", "col", time.Now(), errors.New(string(largeMsg)))
		}

		cmd := &ClientCommand{
			CommandId: "test-trunc",
			Payload:   []byte(`{"max_count": 1000}`),
		}
		reply := manager.handleShowErrors(cmd)

		assert.True(t, reply.Success)
		// Payload should be truncated to under 1MB
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)
		assert.Greater(t, len(reply.Payload), 0)
	})

	t.Run("single error message truncation", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 30 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     10,
		})

		// Create a single error with message larger than 1MB
		veryLargeMsg := make([]byte, 2*1024*1024) // 2MB message
		for i := range veryLargeMsg {
			veryLargeMsg[i] = 'a'
		}

		manager.RecordOperation("Search", "col", time.Now(), errors.New(string(veryLargeMsg)))

		cmd := &ClientCommand{
			CommandId: "test-single-trunc",
			Payload:   []byte(`{"max_count": 1}`),
		}
		reply := manager.handleShowErrors(cmd)

		assert.True(t, reply.Success)
		// Payload should be truncated
		assert.Less(t, len(reply.Payload), maxErrorPayloadSize)

		// Verify the error message was truncated
		var errs []*ErrorInfo
		json.Unmarshal(reply.Payload, &errs)
		if len(errs) > 0 {
			assert.Contains(t, errs[0].ErrorMsg, "...(truncated)")
		}
	})
}

func TestCollectionMetricsZeroRequestSkip(t *testing.T) {
	t.Run("skip collections with zero requests", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Manually add an empty collection metrics entry
		collector.mu.Lock()
		collector.collectionMetrics["empty_col"] = &CollectionMetrics{
			requestCount:   0,
			latencySamples: make([]int64, 1000),
			bufferSize:     1000,
		}
		collector.collectionMetrics["active_col"] = &CollectionMetrics{
			requestCount:   5,
			successCount:   5,
			totalLatency:   5000,
			latencySamples: make([]int64, 1000),
			bufferSize:     1000,
			totalSamples:   5,
		}
		collector.mu.Unlock()

		// GetCollectionMetrics should skip the empty collection
		result := collector.GetCollectionMetrics()

		// Only active_col should be included
		assert.NotContains(t, result, "empty_col")
		assert.Contains(t, result, "active_col")
	})
}

func TestSamplingNotSampled(t *testing.T) {
	t.Run("operation not sampled does not record", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:      true,
			SamplingRate: 0.0, // 0% sampling rate
		})

		// Record should not create collector because sampling rate is 0%
		manager.RecordOperation("Search", "col", time.Now(), nil)

		manager.mu.RLock()
		_, ok := manager.collectors["Search"]
		manager.mu.RUnlock()

		assert.False(t, ok) // No collector created
	})
}

func TestCalculateP99BufferWrapFullCoverage(t *testing.T) {
	t.Run("calculateP99 with totalSamples > bufferSize triggers copy full buffer", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Fill buffer multiple times to ensure totalSamples > bufferSize
		// Buffer size is 1000, record 2000 samples
		for i := 0; i < 2000; i++ {
			collector.Record("", int64((i%50)*1000), true) // Latencies 0-49ms cycling
		}

		// This triggers the else branch: copy(sorted, c.latencySamples)
		metrics := collector.GetMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(2000), metrics.RequestCount)
		// P99 should be calculated correctly
		assert.True(t, metrics.P99LatencyMs >= 0)
	})

	t.Run("calculateP99FromSamples with totalSamples > bufferSize", func(t *testing.T) {
		// Create a buffer and simulate wrapped state
		samples := make([]int64, 50)
		for i := 0; i < 50; i++ {
			samples[i] = int64((i + 1) * 1000) // 1ms to 50ms
		}

		// totalSamples (100) > bufferSize (50) triggers the else branch
		p99 := calculateP99FromSamples(samples, 100, 50)

		// Should calculate P99 from all 50 samples (the buffer)
		// P99 of 1-50ms should be around 49-50ms = 49000-50000 us
		assert.True(t, p99 >= 49000.0, "P99 should be >= 49000us, got %f", p99)
	})
}

func TestHandleShowLatencyHistoryMarshalError(t *testing.T) {
	t.Run("empty snapshots returns valid response", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Don't create any snapshots
		now := time.Now()
		tenMinAgo := now.Add(-10 * time.Minute)
		payload := LatencyHistoryPayload{
			StartTime: tenMinAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
			Detail:    false, // Aggregated mode
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId: "test-empty",
			Payload:   payloadBytes,
		}

		reply := manager.handleShowLatencyHistory(cmd)
		assert.True(t, reply.Success)
		assert.NotEmpty(t, reply.Payload)

		// Should return valid aggregated response with 0 snapshots
		var resp AggregatedLatencyHistoryResponse
		err := json.Unmarshal(reply.Payload, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.SnapshotCount)
	})

	t.Run("detail mode with multiple snapshots", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create multiple snapshots
		for i := 0; i < 5; i++ {
			manager.RecordOperation("Search", "", time.Now().Add(-time.Duration(i)*time.Millisecond), nil)
			manager.createSnapshot()
		}

		now := time.Now()
		tenMinAgo := now.Add(-10 * time.Minute)
		payload := LatencyHistoryPayload{
			StartTime: tenMinAgo.Format(time.RFC3339),
			EndTime:   now.Format(time.RFC3339),
			Detail:    true, // Detail mode
		}
		payloadBytes, _ := json.Marshal(payload)

		cmd := &ClientCommand{
			CommandId: "test-detail",
			Payload:   payloadBytes,
		}

		reply := manager.handleShowLatencyHistory(cmd)
		assert.True(t, reply.Success)

		var resp LatencyHistoryResponse
		err := json.Unmarshal(reply.Payload, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 5, resp.TotalSnapshots)
	})
}

func TestAggregateSnapshotsEdgeCases(t *testing.T) {
	t.Run("snapshot with nil global metrics is skipped", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Create snapshots with actual data
		manager.RecordOperation("Search", "", time.Now(), nil)
		manager.createSnapshot()

		snapshots := manager.GetMetricsSnapshots()

		// Aggregate should work
		result := manager.aggregateSnapshots(snapshots, 0, time.Now().UnixMilli())
		assert.NotNil(t, result)
		assert.True(t, result.SnapshotCount > 0)
	})

	t.Run("multiple operations aggregated correctly", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record multiple operation types
		manager.RecordOperation("Search", "", time.Now().Add(-5*time.Millisecond), nil)
		manager.RecordOperation("Insert", "", time.Now().Add(-10*time.Millisecond), nil)
		manager.createSnapshot()

		manager.RecordOperation("Search", "", time.Now().Add(-3*time.Millisecond), nil)
		manager.RecordOperation("Insert", "", time.Now().Add(-7*time.Millisecond), errors.New("error"))
		manager.createSnapshot()

		snapshots := manager.GetMetricsSnapshots()
		result := manager.aggregateSnapshots(snapshots, 0, time.Now().UnixMilli())

		assert.Equal(t, 2, result.SnapshotCount)
		assert.Contains(t, result.Aggregated.Metrics, "Search")
		assert.Contains(t, result.Aggregated.Metrics, "Insert")

		// Search should have 2 requests total
		assert.Equal(t, int64(2), result.Aggregated.Metrics["Search"].RequestCount)
		// Insert should have 2 requests total (1 success + 1 error)
		assert.Equal(t, int64(2), result.Aggregated.Metrics["Insert"].RequestCount)
	})
}

// TestCalculateP99FromSamplesBufferWrap tests the buffer wrap scenarios
func TestCalculateP99FromSamplesBufferWrap(t *testing.T) {
	t.Run("totalSamples greater than bufferSize copies full buffer", func(t *testing.T) {
		// Create a samples buffer with known values
		bufferSize := 100
		samples := make([]int64, bufferSize)
		for i := 0; i < bufferSize; i++ {
			samples[i] = int64((i + 1) * 1000) // 1ms to 100ms in microseconds
		}

		// When totalSamples > bufferSize, it should copy full buffer
		totalSamples := int64(150) // More than bufferSize to trigger else branch
		p99 := calculateP99FromSamples(samples, totalSamples, bufferSize)

		// P99 should be calculated from the buffer
		assert.True(t, p99 > 0)
		assert.True(t, p99 >= 99000) // 99th percentile of 1000-100000 should be ~99000
	})

	t.Run("totalSamples exactly at bufferSize", func(t *testing.T) {
		bufferSize := 50
		samples := make([]int64, bufferSize)
		for i := 0; i < bufferSize; i++ {
			samples[i] = int64(i * 100)
		}

		p99 := calculateP99FromSamples(samples, int64(bufferSize), bufferSize)
		assert.True(t, p99 > 0)
	})

	t.Run("totalSamples less than bufferSize", func(t *testing.T) {
		bufferSize := 100
		samples := make([]int64, bufferSize)
		// Only fill first 30 samples
		for i := 0; i < 30; i++ {
			samples[i] = int64(i * 1000)
		}

		p99 := calculateP99FromSamples(samples, 30, bufferSize)
		assert.True(t, p99 > 0)
	})

	t.Run("zero totalSamples returns zero", func(t *testing.T) {
		samples := make([]int64, 100)
		p99 := calculateP99FromSamples(samples, 0, 100)
		assert.Equal(t, float64(0), p99)
	})
}

// TestCalculateP99MethodCoverageDirectly tests calculateP99 via GetP99Latency
func TestCalculateP99MethodCoverageDirectly(t *testing.T) {
	t.Run("buffer wrap triggers copy of full latencySamples", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record more than bufferSize samples to trigger buffer wrap
		// bufferSize is 1000 by default
		for i := 0; i < 1500; i++ {
			// Use varied latencies to get meaningful P99
			collector.Record("", int64((i%100+1)*1000), true)
		}

		// GetP99Latency calls calculateP99 internally
		p99 := collector.GetP99Latency()
		assert.True(t, p99 > 0)
	})

	t.Run("buffer exactly at capacity", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record exactly 1000 samples (bufferSize)
		for i := 0; i < 1000; i++ {
			collector.Record("", int64((i+1)*1000), true)
		}

		p99 := collector.GetP99Latency()
		// P99 of 1-1000ms should be around 990ms
		assert.True(t, p99 >= 990)
	})

	t.Run("buffer partially filled", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		for i := 0; i < 100; i++ {
			collector.Record("", int64((i+1)*1000), true)
		}

		p99 := collector.GetP99Latency()
		assert.True(t, p99 > 0)
	})
}

// TestHandleCollectionMetricsDisableSpecificCollections tests disabling specific collections
func TestHandleCollectionMetricsDisableSpecificCollections(t *testing.T) {
	t.Run("disable specific collections removes them from enabled list", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// First enable some collections
		enablePayload := CollectionMetricsPayload{
			Enabled:     true,
			Collections: []string{"col1", "col2", "col3"},
		}
		enablePayloadBytes, _ := json.Marshal(enablePayload)
		enableCmd := &ClientCommand{
			CommandId: "enable-cmd",
			Payload:   enablePayloadBytes,
		}
		reply := manager.handleCollectionMetrics(enableCmd)
		assert.True(t, reply.Success)

		// Now disable only col2
		disablePayload := CollectionMetricsPayload{
			Enabled:     false,
			Collections: []string{"col2"},
		}
		disablePayloadBytes, _ := json.Marshal(disablePayload)
		disableCmd := &ClientCommand{
			CommandId: "disable-cmd",
			Payload:   disablePayloadBytes,
		}
		reply = manager.handleCollectionMetrics(disableCmd)
		assert.True(t, reply.Success)

		// Check that col1 and col3 are still enabled but col2 is not
		manager.enabledCollectionsMu.RLock()
		assert.True(t, manager.enabledCollections["col1"])
		assert.False(t, manager.enabledCollections["col2"])
		assert.True(t, manager.enabledCollections["col3"])
		manager.enabledCollectionsMu.RUnlock()
	})

	t.Run("disable with empty collections disables all", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable some collections first
		enablePayload := CollectionMetricsPayload{
			Enabled:     true,
			Collections: []string{"col1", "col2"},
		}
		enablePayloadBytes, _ := json.Marshal(enablePayload)
		enableCmd := &ClientCommand{
			CommandId: "enable-cmd",
			Payload:   enablePayloadBytes,
		}
		manager.handleCollectionMetrics(enableCmd)

		// Disable all by passing empty collections with enabled=false
		disablePayload := CollectionMetricsPayload{
			Enabled:     false,
			Collections: []string{},
		}
		disablePayloadBytes, _ := json.Marshal(disablePayload)
		disableCmd := &ClientCommand{
			CommandId: "disable-all-cmd",
			Payload:   disablePayloadBytes,
		}
		reply := manager.handleCollectionMetrics(disableCmd)
		assert.True(t, reply.Success)

		// All collections should be disabled
		manager.enabledCollectionsMu.RLock()
		assert.Len(t, manager.enabledCollections, 0)
		assert.False(t, manager.allCollectionsEnabled)
		manager.enabledCollectionsMu.RUnlock()
	})
}

// TestHandleShowLatencyHistoryTimeBefore tests end time before start time
func TestHandleShowLatencyHistoryTimeBefore(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Create payload where end_time is before start_time
	now := time.Now()
	payload := LatencyHistoryPayload{
		StartTime: now.Format(time.RFC3339),
		EndTime:   now.Add(-1 * time.Hour).Format(time.RFC3339), // end before start
		Detail:    false,
	}
	payloadBytes, _ := json.Marshal(payload)

	cmd := &ClientCommand{
		CommandId: "test-time-before",
		Payload:   payloadBytes,
	}

	reply := manager.handleShowLatencyHistory(cmd)
	assert.False(t, reply.Success)
	assert.Contains(t, reply.ErrorMessage, "end_time must be after start_time")
}

// TestHandleShowLatencyHistoryTimeRangeExceeded tests time range exceeding 1 hour
func TestHandleShowLatencyHistoryTimeRangeExceeded(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	now := time.Now()
	payload := LatencyHistoryPayload{
		StartTime: now.Add(-2 * time.Hour).Format(time.RFC3339),
		EndTime:   now.Format(time.RFC3339), // 2 hour range
		Detail:    false,
	}
	payloadBytes, _ := json.Marshal(payload)

	cmd := &ClientCommand{
		CommandId: "test-range-exceeded",
		Payload:   payloadBytes,
	}

	reply := manager.handleShowLatencyHistory(cmd)
	assert.False(t, reply.Success)
	assert.Contains(t, reply.ErrorMessage, "time range cannot exceed 1 hour")
}

// TestHandleShowErrorsWithMaxCountPayload tests show_errors with explicit max_count
func TestHandleShowErrorsWithMaxCountPayload(t *testing.T) {
	manager := NewClientTelemetryManager(nil, &TelemetryConfig{
		Enabled:           true,
		HeartbeatInterval: 30 * time.Second,
		SamplingRate:      1.0,
		ErrorMaxCount:     100,
	})

	// Record multiple errors
	for i := 0; i < 50; i++ {
		manager.RecordOperation("Search", "col", time.Now(), errors.New("test error"))
	}

	// Request with max_count less than available errors
	payload := struct {
		MaxCount int `json:"max_count"`
	}{MaxCount: 10}
	payloadBytes, _ := json.Marshal(payload)

	cmd := &ClientCommand{
		CommandId: "test-max-count",
		Payload:   payloadBytes,
	}

	reply := manager.handleShowErrors(cmd)
	assert.True(t, reply.Success)

	var errList []*ErrorInfo
	err := json.Unmarshal(reply.Payload, &errList)
	assert.NoError(t, err)
	assert.LessOrEqual(t, len(errList), 10)
}

// TestRecordOperationWithRequestIDSamplingDisabled tests that sampled=false skips recording
func TestRecordOperationWithRequestIDSamplingDisabled(t *testing.T) {
	manager := NewClientTelemetryManager(nil, &TelemetryConfig{
		Enabled:           true,
		HeartbeatInterval: 30 * time.Second,
		SamplingRate:      0.0, // 0% sampling rate means nothing gets sampled
		ErrorMaxCount:     100,
	})

	// When sampling rate is 0, shouldSample returns false
	// RecordOperationWithRequestID returns early without creating a collector
	manager.RecordOperationWithRequestID("Search", "col", "req-123", time.Now(), nil)

	// Get metrics to verify NO recording happened (due to 0% sampling)
	manager.mu.RLock()
	collector := manager.collectors["Search"]
	manager.mu.RUnlock()

	// Collector should NOT exist because shouldSample returned false
	assert.Nil(t, collector)
}

// TestAggregateSnapshotsMaxLatency tests maxLatency tracking in aggregation
func TestAggregateSnapshotsMaxLatency(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Record operations with varying latencies
	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(i) * time.Millisecond)
		manager.RecordOperation("Search", "", time.Now().Add(-time.Duration(i+5)*time.Millisecond), nil)
	}
	manager.createSnapshot()

	// Record more operations with higher latencies
	for i := 0; i < 10; i++ {
		manager.RecordOperation("Search", "", time.Now().Add(-time.Duration(i+20)*time.Millisecond), nil)
	}
	manager.createSnapshot()

	snapshots := manager.GetMetricsSnapshots()
	result := manager.aggregateSnapshots(snapshots, 0, time.Now().UnixMilli())

	assert.NotNil(t, result)
	assert.True(t, result.SnapshotCount >= 1)
	if result.Aggregated.Metrics["Search"] != nil {
		assert.True(t, result.Aggregated.Metrics["Search"].MaxLatencyMs > 0)
	}
}

// TestCalculateP99ZeroSamples tests the zero samples early return in calculateP99
func TestCalculateP99ZeroSamples(t *testing.T) {
	t.Run("GetP99Latency returns 0 for empty collector", func(t *testing.T) {
		collector := NewOperationMetricsCollector()
		// No records - totalSamples is 0
		p99 := collector.GetP99Latency()
		assert.Equal(t, float64(0), p99)
	})

	t.Run("GetMetricsSnapshot returns nil for empty collector", func(t *testing.T) {
		collector := NewOperationMetricsCollector()
		// No records - GetMetricsSnapshot should return nil
		metrics := collector.GetMetricsSnapshot()
		assert.Nil(t, metrics)
	})
}

// TestCalculateP99ElseBranch explicitly tests the else branch in calculateP99
func TestCalculateP99ElseBranch(t *testing.T) {
	t.Run("verify else branch when totalSamples exceeds bufferSize", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record exactly 1001 samples to exceed bufferSize (1000)
		// This ensures totalSamples > bufferSize and triggers the else branch
		for i := 0; i < 1001; i++ {
			collector.Record("", int64((i+1)*1000), true) // 1us to 1001us
		}

		// GetP99Latency internally calls calculateP99 which should take else branch
		p99 := collector.GetP99Latency()

		// P99 should be calculated from wrapped buffer
		// Since we have 1001 samples and bufferSize 1000, totalSamples = 1001
		// The else branch copies full latencySamples array
		assert.True(t, p99 > 0)
	})

	t.Run("GetMetricsSnapshot with wrapped buffer", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Record 2000 samples to ensure buffer has wrapped
		for i := 0; i < 2000; i++ {
			collector.Record("", int64((i%100+1)*1000), true)
		}

		// GetMetricsSnapshot calls calculateP99 without resetting
		metrics := collector.GetMetricsSnapshot()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(2000), metrics.RequestCount)
		assert.True(t, metrics.P99LatencyMs > 0)
	})

	t.Run("multiple GetP99Latency calls with wrapped buffer", func(t *testing.T) {
		collector := NewOperationMetricsCollector()

		// Fill buffer completely and wrap
		for i := 0; i < 1500; i++ {
			collector.Record("", int64(i*100+100), true)
		}

		// Call GetP99Latency multiple times - should consistently trigger else branch
		p991 := collector.GetP99Latency()
		p992 := collector.GetP99Latency()
		assert.Equal(t, p991, p992)
		assert.True(t, p991 > 0)
	})
}

// TestBuildClientInfoWithNonNilClient tests buildClientInfo with client config
func TestBuildClientInfoWithNonNilClient(t *testing.T) {
	// This test verifies the branches in buildClientInfo when client is not nil
	// The function checks m.client, m.client.config, and getCurrentDB()

	t.Run("manager without client", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())
		info := manager.buildClientInfo()

		assert.NotNil(t, info)
		assert.Equal(t, "GoMilvusClient", info.SdkType)
		assert.NotEmpty(t, info.Reserved["client_id"])
		// User should be empty when client is nil
		assert.Empty(t, info.User)
	})

	t.Run("manager with client that has config", func(t *testing.T) {
		// Create a minimal client with config
		client := &Client{
			config: &ClientConfig{
				Username: "testuser",
			},
			currentDB: "", // empty DB
		}
		manager := NewClientTelemetryManager(client, DefaultTelemetryConfig())

		info := manager.buildClientInfo()
		assert.NotNil(t, info)
		assert.Equal(t, "testuser", info.User)
	})

	t.Run("manager with client that has currentDB", func(t *testing.T) {
		// Create a minimal client with currentDB set
		client := &Client{
			config: &ClientConfig{
				Username: "dbuser",
			},
			currentDB: "test_database", // non-empty DB to trigger the branch
		}
		manager := NewClientTelemetryManager(client, DefaultTelemetryConfig())

		info := manager.buildClientInfo()
		assert.NotNil(t, info)
		assert.Equal(t, "dbuser", info.User)
		assert.Equal(t, "test_database", info.Reserved["db_name"])
	})
}

// TestSendHeartbeatEdgeCases tests various paths in sendHeartbeat
func TestSendHeartbeatEdgeCases(t *testing.T) {
	t.Run("sendHeartbeat with nil client service", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// This should return early because client is nil
		// Coverage of early return path
		manager.sendHeartbeat()
	})

	t.Run("sendHeartbeat with telemetry disabled", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, &TelemetryConfig{
			Enabled:           false,
			HeartbeatInterval: 30 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     100,
		})

		// Should return early because enabled is false
		manager.sendHeartbeat()
	})

	t.Run("sendHeartbeat with snapshots and no commands", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Record some operations to have data
		manager.RecordOperation("Search", "", time.Now(), nil)
		manager.createSnapshot()

		// sendHeartbeat will try but client is nil, covers snapshot collection path
		manager.sendHeartbeat()
	})

	t.Run("sendHeartbeat with pending replies", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Add pending replies
		manager.pendingRepliesMu.Lock()
		manager.pendingReplies = append(manager.pendingReplies, &commonpb.CommandReply{
			CommandId: "test-cmd-1",
			Success:   true,
		})
		manager.pendingRepliesMu.Unlock()

		// sendHeartbeat will try but client is nil
		manager.sendHeartbeat()
	})

	t.Run("sendHeartbeat with enabled collection metrics", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable collection metrics
		manager.enabledCollectionsMu.Lock()
		manager.enabledCollections["testcol"] = true
		manager.enabledCollectionsMu.Unlock()

		// Record operations with collection
		manager.RecordOperation("Search", "testcol", time.Now(), nil)
		manager.createSnapshot()

		// sendHeartbeat covers collection filtering logic
		manager.sendHeartbeat()
	})

	t.Run("sendHeartbeat with all collections enabled wildcard", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Enable all collections via wildcard
		manager.enabledCollectionsMu.Lock()
		manager.allCollectionsEnabled = true
		manager.enabledCollectionsMu.Unlock()

		// Record operations with various collections
		manager.RecordOperation("Search", "col1", time.Now(), nil)
		manager.RecordOperation("Insert", "col2", time.Now(), nil)
		manager.createSnapshot()

		// sendHeartbeat covers wildcard collection metrics path
		manager.sendHeartbeat()
	})
}

// TestHandleShowLatencyHistoryDetailModeWithOperations tests detail mode path
func TestHandleShowLatencyHistoryDetailModeWithOperations(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Record operations and create snapshots
	for i := 0; i < 3; i++ {
		manager.RecordOperation("Search", "", time.Now().Add(-time.Duration(i)*time.Millisecond), nil)
		manager.RecordOperation("Insert", "", time.Now().Add(-time.Duration(i+1)*time.Millisecond), nil)
		manager.createSnapshot()
	}

	now := time.Now()
	payload := LatencyHistoryPayload{
		StartTime: now.Add(-10 * time.Minute).Format(time.RFC3339),
		EndTime:   now.Format(time.RFC3339),
		Detail:    true, // detail mode
	}
	payloadBytes, _ := json.Marshal(payload)

	cmd := &ClientCommand{
		CommandId: "test-detail-operations",
		Payload:   payloadBytes,
	}

	reply := manager.handleShowLatencyHistory(cmd)
	assert.True(t, reply.Success)

	var resp LatencyHistoryResponse
	err := json.Unmarshal(reply.Payload, &resp)
	assert.NoError(t, err)
	assert.True(t, resp.TotalSnapshots > 0)
	assert.True(t, len(resp.Snapshots) > 0)

	// Check that operations are included in snapshots
	hasSearch := false
	for _, snapshot := range resp.Snapshots {
		if snapshot.Metrics["Search"] != nil {
			hasSearch = true
			break
		}
	}
	assert.True(t, hasSearch)
}

// TestCalculateP99FromSamplesWrappedBuffer tests P99 calculation when buffer has wrapped
func TestCalculateP99FromSamplesWrappedBuffer(t *testing.T) {
	bufferSize := 10

	t.Run("totalSamples exceeds bufferSize", func(t *testing.T) {
		// Simulate buffer wrap: totalSamples > bufferSize
		samples := make([]int64, bufferSize)
		for i := 0; i < bufferSize; i++ {
			samples[i] = int64((i + 1) * 100) // 100, 200, ..., 1000
		}

		// totalSamples = 15 means buffer has wrapped
		totalSamples := int64(15)
		p99 := calculateP99FromSamples(samples, totalSamples, bufferSize)

		// P99 index = int(10 * 0.99) = 9, so should be 1000
		assert.Equal(t, float64(1000), p99)
	})

	t.Run("single sample", func(t *testing.T) {
		samples := []int64{500, 0, 0, 0, 0}
		totalSamples := int64(1)
		p99 := calculateP99FromSamples(samples, totalSamples, 5)

		// With 1 sample, P99 index = int(1 * 0.99) = 0, which equals len-1, so adjusted to 0
		assert.Equal(t, float64(500), p99)
	})

	t.Run("index equals length adjustment", func(t *testing.T) {
		// Create samples where index calculation would be >= len(sorted)
		samples := []int64{100}
		totalSamples := int64(1)
		p99 := calculateP99FromSamples(samples, totalSamples, 1)

		// index = int(1 * 0.99) = 0, but since len(sorted)=1, index >= len triggers adjustment
		assert.Equal(t, float64(100), p99)
	})
}

// TestCalculateP99BufferWrap tests P99 calculation with buffer wrap in collector
func TestCalculateP99BufferWrap(t *testing.T) {
	collector := NewOperationMetricsCollector()

	// Record more samples than buffer size to trigger wrap
	// Default bufferSize is 1000
	for i := 0; i < 1500; i++ {
		collector.Record("test-col", int64(i*100), true) // latency in microseconds
	}

	// This exercises the buffer wrap path in calculateP99
	p99 := collector.GetP99Latency()
	assert.True(t, p99 >= 0)
}

// TestHandleShowErrorsPayloadTruncation tests error truncation when payload is too large
func TestHandleShowErrorsPayloadTruncation(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Create many errors with long messages to exceed 1MB limit
	for i := 0; i < 1000; i++ {
		// Each error message is ~10KB
		longMsg := make([]byte, 10*1024)
		for j := range longMsg {
			longMsg[j] = 'a'
		}
		manager.errorCollector.RecordError("TestOp", "", "error: "+string(longMsg), "req-id")
	}

	cmd := &ClientCommand{
		CommandId: "test-truncation",
		Payload:   []byte(`{"max_count": 1000}`),
	}

	reply := manager.handleShowErrors(cmd)
	assert.True(t, reply.Success)

	// Payload should be truncated to fit under 1MB
	assert.True(t, len(reply.Payload) <= maxErrorPayloadSize)
}

// TestHandleShowErrorsSingleLargeError tests truncation of a single very large error
func TestHandleShowErrorsSingleLargeError(t *testing.T) {
	manager := NewClientTelemetryManager(nil, &TelemetryConfig{
		Enabled:           true,
		HeartbeatInterval: 30 * time.Second,
		SamplingRate:      1.0,
		ErrorMaxCount:     1, // Only keep 1 error
	})

	// Create a single error with a message larger than 1MB
	hugeMsg := make([]byte, 2*1024*1024) // 2MB
	for i := range hugeMsg {
		hugeMsg[i] = 'x'
	}
	manager.errorCollector.RecordError("HugeErrorOp", "", "huge error: "+string(hugeMsg), "req-id")

	cmd := &ClientCommand{
		CommandId: "test-single-large-error",
		Payload:   []byte(`{"max_count": 1}`),
	}

	reply := manager.handleShowErrors(cmd)
	assert.True(t, reply.Success)

	// Payload should be truncated
	assert.True(t, len(reply.Payload) <= maxErrorPayloadSize)
}

// TestAggregateSnapshotsNilGlobal tests aggregation with nil Global metrics
func TestAggregateSnapshotsNilGlobal(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Create snapshots with nil Global metrics
	snapshots := []*MetricsSnapshot{
		{
			Timestamp: time.Now().UnixMilli(),
			EndTime:   time.Now().Add(30 * time.Second).UnixMilli(),
			Metrics: []*OperationMetrics{
				{
					Operation: "Search",
					Global:    nil, // Nil global metrics - should be skipped
				},
				{
					Operation: "Insert",
					Global: &Metrics{
						RequestCount: 100,
						SuccessCount: 95,
						ErrorCount:   5,
						AvgLatencyMs: 10.5,
						P99LatencyMs: 50.0,
						MaxLatencyMs: 100.0,
					},
				},
			},
		},
	}

	startTime := time.Now().UnixMilli()
	endTime := time.Now().Add(time.Minute).UnixMilli()
	result := manager.aggregateSnapshots(snapshots, startTime, endTime)

	// Search with nil Global should be skipped
	assert.Nil(t, result.Aggregated.Metrics["Search"])

	// Insert should be present
	assert.NotNil(t, result.Aggregated.Metrics["Insert"])
	assert.Equal(t, int64(100), result.Aggregated.Metrics["Insert"].RequestCount)
}

// TestHandleShowLatencyHistoryDetailModeNilGlobal tests detail mode with nil Global
func TestHandleShowLatencyHistoryDetailModeNilGlobal(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	// Add a snapshot with nil Global manually
	manager.snapshotsMu.Lock()
	manager.snapshots = append(manager.snapshots, &MetricsSnapshot{
		Timestamp: time.Now().Add(-5 * time.Minute).UnixMilli(),
		EndTime:   time.Now().Add(-4 * time.Minute).UnixMilli(),
		Metrics: []*OperationMetrics{
			{
				Operation: "Search",
				Global:    nil, // Nil global - should be skipped in detail mode
			},
		},
	})
	manager.snapshotsMu.Unlock()

	now := time.Now()
	payload := LatencyHistoryPayload{
		StartTime: now.Add(-10 * time.Minute).Format(time.RFC3339),
		EndTime:   now.Format(time.RFC3339),
		Detail:    true,
	}
	payloadBytes, _ := json.Marshal(payload)

	cmd := &ClientCommand{
		CommandId: "test-nil-global",
		Payload:   payloadBytes,
	}

	reply := manager.handleShowLatencyHistory(cmd)
	assert.True(t, reply.Success)

	var resp LatencyHistoryResponse
	err := json.Unmarshal(reply.Payload, &resp)
	assert.NoError(t, err)
	assert.Equal(t, 1, resp.TotalSnapshots)
	// Search with nil Global should not have metrics
	if len(resp.Snapshots) > 0 {
		_, hasSearch := resp.Snapshots[0].Metrics["Search"]
		assert.False(t, hasSearch)
	}
}

// TestAggregateSnapshotsZeroRequestCount tests aggregation with zero request count
func TestAggregateSnapshotsZeroRequestCount(t *testing.T) {
	manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

	snapshots := []*MetricsSnapshot{
		{
			Timestamp: time.Now().UnixMilli(),
			EndTime:   time.Now().Add(30 * time.Second).UnixMilli(),
			Metrics: []*OperationMetrics{
				{
					Operation: "Search",
					Global: &Metrics{
						RequestCount: 0, // Zero request count
						SuccessCount: 0,
						ErrorCount:   0,
						AvgLatencyMs: 0,
						P99LatencyMs: 0,
						MaxLatencyMs: 0,
					},
				},
			},
		},
	}

	startTime := time.Now().UnixMilli()
	endTime := time.Now().Add(time.Minute).UnixMilli()
	result := manager.aggregateSnapshots(snapshots, startTime, endTime)

	// Should handle zero request count without division by zero
	assert.NotNil(t, result.Aggregated.Metrics["Search"])
	assert.Equal(t, float64(0), result.Aggregated.Metrics["Search"].AvgLatencyMs)
	assert.Equal(t, float64(0), result.Aggregated.Metrics["Search"].P99LatencyMs)
}
