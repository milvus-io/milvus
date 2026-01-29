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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTelemetryIntegration demonstrates how the telemetry system works
// This can be run standalone to verify metrics collection
func TestTelemetryIntegration(t *testing.T) {
	t.Run("demonstrate_metrics_collection", func(t *testing.T) {
		// Create a telemetry manager with custom config
		config := &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second, // Short interval for testing
			SamplingRate:      1.0,             // 100% sampling for testing
		}
		manager := NewClientTelemetryManager(nil, config)

		// Simulate some operations
		fmt.Println("=== Simulating client operations ===")

		// Simulate Search operations
		for i := 0; i < 10; i++ {
			startTime := time.Now().Add(-time.Duration(i+1) * time.Millisecond)
			manager.RecordOperation("Search", "test_collection", startTime, nil)
		}
		fmt.Println("Recorded 10 Search operations")

		// Simulate Insert operations with some errors
		for i := 0; i < 5; i++ {
			startTime := time.Now().Add(-time.Duration(i+5) * time.Millisecond)
			var err error
			if i == 2 {
				err = fmt.Errorf("simulated insert error")
			}
			manager.RecordOperation("Insert", "test_collection", startTime, err)
		}
		fmt.Println("Recorded 5 Insert operations (1 error)")

		// Simulate Query operations on different collections
		manager.RecordOperation("Query", "collection_a", time.Now().Add(-10*time.Millisecond), nil)
		manager.RecordOperation("Query", "collection_b", time.Now().Add(-20*time.Millisecond), nil)
		manager.RecordOperation("Query", "collection_a", time.Now().Add(-15*time.Millisecond), nil)
		fmt.Println("Recorded 3 Query operations on 2 collections")

		// Collect and display metrics
		fmt.Println("\n=== Collected Metrics ===")
		metrics := manager.collectMetrics()

		for _, m := range metrics {
			fmt.Printf("\nOperation: %s\n", m.Operation)
			fmt.Printf("  Global Metrics:\n")
			fmt.Printf("    Request Count: %d\n", m.Global.RequestCount)
			fmt.Printf("    Success Count: %d\n", m.Global.SuccessCount)
			fmt.Printf("    Error Count: %d\n", m.Global.ErrorCount)
			fmt.Printf("    Avg Latency: %.2f ms\n", m.Global.AvgLatencyMs)
			fmt.Printf("    P99 Latency: %.2f ms\n", m.Global.P99LatencyMs)

			if len(m.CollectionMetrics) > 0 {
				fmt.Printf("  Per-Collection Metrics:\n")
				for coll, cm := range m.CollectionMetrics {
					fmt.Printf("    %s: requests=%d, success=%d, errors=%d, avg=%.2fms\n",
						coll, cm.RequestCount, cm.SuccessCount, cm.ErrorCount, cm.AvgLatencyMs)
				}
			}
		}

		assert.Len(t, metrics, 3) // Search, Insert, Query
	})

	t.Run("demonstrate_command_handling", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Track config changes
		var configUpdates []string

		// Register command handlers
		manager.RegisterCommandHandler("update_config", func(cmd *ClientCommand) *CommandReply {
			configKey := string(cmd.Payload)
			configUpdates = append(configUpdates, configKey)
			fmt.Printf("Config updated: %s (persistent=%v)\n", configKey, cmd.Persistent)
			return &CommandReply{
				CommandId: cmd.CommandId,
				Success:   true,
			}
		})

		manager.RegisterCommandHandler("set_log_level", func(cmd *ClientCommand) *CommandReply {
			level := string(cmd.Payload)
			fmt.Printf("Log level set to: %s\n", level)
			return &CommandReply{
				CommandId: cmd.CommandId,
				Success:   true,
			}
		})

		// Simulate receiving commands from server
		fmt.Println("\n=== Simulating server commands ===")
		commands := []*ClientCommand{
			{
				CommandId:   "cmd-1",
				CommandType: "update_config",
				Payload:     []byte("max_connections=100"),
				CreateTime:  1000,
				Persistent:  true,
				TargetScope: "global",
			},
			{
				CommandId:   "cmd-2",
				CommandType: "set_log_level",
				Payload:     []byte("debug"),
				CreateTime:  2000,
				Persistent:  false,
				TargetScope: "client",
			},
			{
				CommandId:   "cmd-3",
				CommandType: "update_config",
				Payload:     []byte("timeout=30s"),
				CreateTime:  3000,
				Persistent:  true,
				TargetScope: "global",
			},
		}

		manager.processCommands(commands)

		// Check state
		fmt.Printf("\nConfig hash: %s\n", manager.GetConfigHash())

		// Get pending replies
		replies := manager.getPendingReplies()
		fmt.Printf("Pending replies: %d\n", len(replies))
		for _, r := range replies {
			fmt.Printf("  Command %s: success=%v\n", r.CommandId, r.Success)
		}

		assert.NotEmpty(t, manager.GetConfigHash())
		assert.Len(t, configUpdates, 2) // Two update_config commands
		assert.Len(t, replies, 3)
	})

	t.Run("demonstrate_heartbeat_info", func(t *testing.T) {
		manager := NewClientTelemetryManager(nil, DefaultTelemetryConfig())

		// Set some state
		manager.SetConfigHash("abc123")

		// Record some operations
		manager.RecordOperation("Search", "coll1", time.Now().Add(-5*time.Millisecond), nil)
		manager.RecordOperation("Search", "coll1", time.Now().Add(-10*time.Millisecond), nil)

		// This is what would be sent in a heartbeat
		fmt.Println("\n=== Heartbeat payload info ===")
		fmt.Printf("Config Hash: %s\n", manager.GetConfigHash())

		metrics := manager.collectMetrics()
		fmt.Printf("Metrics count: %d\n", len(metrics))

		replies := manager.getPendingReplies()
		fmt.Printf("Pending replies: %d\n", len(replies))
	})
}

// Example_telemetryConfig shows how a user might configure telemetry
func Example_telemetryConfig() {
	// When creating a client, you can configure telemetry
	config := &ClientConfig{
		Address: "localhost:19530",
		TelemetryConfig: &TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 30 * time.Second,
			SamplingRate:      1.0,
		},
	}

	// The telemetry is automatically started when client is created
	// All operations are automatically instrumented
	_ = config

	// To disable telemetry:
	// config.TelemetryConfig = &TelemetryConfig{Enabled: false}

	fmt.Println("Telemetry is configured automatically with the client")
	// Output: Telemetry is configured automatically with the client
}
