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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestCommandRouter_Route(t *testing.T) {
	router := NewCommandRouter()
	router.RegisterHandler(CommandTypeCollectionMetrics, NewCollectionMetricsHandler())

	t.Run("unknown_command_type", func(t *testing.T) {
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-1",
			CommandType: "unknown_type",
			Payload:     []byte{},
		}
		reply := router.Route(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "unknown command type")
	})

	t.Run("collection_metrics_empty_collections", func(t *testing.T) {
		payload := &CollectionMetricsPayload{
			Collections: []string{}, // empty, should fail when enabled=true
			Enabled:     true,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-4",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data,
		}
		reply := router.Route(cmd)
		assert.False(t, reply.Success)
		assert.Contains(t, reply.ErrorMessage, "collections list cannot be empty when enabled=true")
	})

	t.Run("collection_metrics_success", func(t *testing.T) {
		payload := &CollectionMetricsPayload{
			Collections:  []string{"test_collection"},
			Enabled:      true,
			MetricsTypes: []string{"latency", "qps"},
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-5",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data,
		}
		reply := router.Route(cmd)
		assert.True(t, reply.Success)
		assert.Empty(t, reply.ErrorMessage)
	})

	t.Run("collection_metrics_wildcard_enable", func(t *testing.T) {
		// Test that "*" wildcard is accepted
		payload := &CollectionMetricsPayload{
			Collections: []string{"*"},
			Enabled:     true,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-6",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data,
		}
		reply := router.Route(cmd)
		assert.True(t, reply.Success)
		assert.Empty(t, reply.ErrorMessage)
	})

	t.Run("collection_metrics_wildcard_disable", func(t *testing.T) {
		// Test that "*" wildcard is accepted for disable
		payload := &CollectionMetricsPayload{
			Collections: []string{"*"},
			Enabled:     false,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-7",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data,
		}
		reply := router.Route(cmd)
		assert.True(t, reply.Success)
		assert.Empty(t, reply.ErrorMessage)
	})

	t.Run("collection_metrics_disable_empty_collections_allowed", func(t *testing.T) {
		// Test that empty collections is allowed for disable (disables all)
		payload := &CollectionMetricsPayload{
			Collections: []string{},
			Enabled:     false,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId:   "test-8",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data,
		}
		reply := router.Route(cmd)
		assert.True(t, reply.Success)
		assert.Empty(t, reply.ErrorMessage)
	})
}

func TestShowErrorsHandler(t *testing.T) {
	handler := NewShowErrorsHandler(nil)

	cmd := &commonpb.ClientCommand{
		CommandId: "test-1",
		Payload:   []byte{},
	}

	errMsg := handler(cmd)
	assert.Empty(t, errMsg)
}

func TestPushConfigHandler(t *testing.T) {
	handler := NewPushConfigHandler()

	t.Run("empty_config", func(t *testing.T) {
		payload := &PushConfigPayload{} // no fields set
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId: "test-1",
			Payload:   data,
		}
		errMsg := handler(cmd)
		assert.NotEmpty(t, errMsg)
		assert.Contains(t, errMsg, "at least one config field must be set")
	})

	t.Run("valid_config_enabled", func(t *testing.T) {
		enabled := true
		payload := &PushConfigPayload{
			Enabled:    &enabled,
			TTLSeconds: 3600,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId: "test-2",
			Payload:   data,
		}
		errMsg := handler(cmd)
		assert.Empty(t, errMsg)
	})

	t.Run("valid_config_heartbeat", func(t *testing.T) {
		heartbeat := int64(30000)
		payload := &PushConfigPayload{
			HeartbeatIntervalMs: &heartbeat,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId: "test-3",
			Payload:   data,
		}
		errMsg := handler(cmd)
		assert.Empty(t, errMsg)
	})

	t.Run("invalid_sampling_rate", func(t *testing.T) {
		rate := 1.5 // out of range
		payload := &PushConfigPayload{
			SamplingRate: &rate,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId: "test-4",
			Payload:   data,
		}
		errMsg := handler(cmd)
		assert.NotEmpty(t, errMsg)
		assert.Contains(t, errMsg, "sampling_rate must be between")
	})

	t.Run("invalid_heartbeat_interval", func(t *testing.T) {
		heartbeat := int64(-1)
		payload := &PushConfigPayload{
			HeartbeatIntervalMs: &heartbeat,
		}
		data, _ := json.Marshal(payload)
		cmd := &commonpb.ClientCommand{
			CommandId: "test-5",
			Payload:   data,
		}
		errMsg := handler(cmd)
		assert.NotEmpty(t, errMsg)
		assert.Contains(t, errMsg, "heartbeat_interval_ms must be positive")
	})
}

func TestCommandRouter_RouteMultipleCommands(t *testing.T) {
	router := NewCommandRouter()
	router.RegisterHandler(CommandTypeCollectionMetrics, NewCollectionMetricsHandler())

	payload1 := &CollectionMetricsPayload{
		Collections: []string{"coll1"},
		Enabled:     true,
	}
	data1, _ := json.Marshal(payload1)

	commands := []*commonpb.ClientCommand{
		{
			CommandId:   "cmd-1",
			CommandType: CommandTypeCollectionMetrics,
			Payload:     data1,
		},
		{
			CommandId:   "cmd-2",
			CommandType: "unknown_type",
			Payload:     []byte{},
		},
	}

	replies := router.RouteAndCollectReplies(commands)
	assert.Len(t, replies, 2)
	assert.True(t, replies[0].Success)
	assert.False(t, replies[1].Success)
}

// TestShowErrorsHandlerWithInvalidPayload tests error handler with bad payload
func TestShowErrorsHandlerWithInvalidPayload(t *testing.T) {
	handler := NewShowErrorsHandler(nil)

	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-1",
		CommandType: "show_errors",
		Payload:     []byte("{invalid json"),
	}

	errMsg := handler(cmd)
	assert.NotEmpty(t, errMsg)
	assert.Contains(t, errMsg, "failed to parse payload")
}

// TestShowErrorsHandlerWithCollector tests error handler with collector
func TestShowErrorsHandlerWithCollector(t *testing.T) {
	mockCollector := &mockCollectorForRouter{
		errors: []string{"error1", "error2"},
	}

	handler := NewShowErrorsHandler(mockCollector)

	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-1",
		CommandType: "show_errors",
		Payload:     []byte("{}"),
	}

	errMsg := handler(cmd)
	assert.Empty(t, errMsg)
}

// TestCollectionMetricsHandlerSuccess tests successful collection metrics handler
func TestCollectionMetricsHandlerSuccess(t *testing.T) {
	handler := NewCollectionMetricsHandler()

	payload := &CollectionMetricsPayload{
		Collections:  []string{"col1", "col2"},
		Enabled:      true,
		MetricsTypes: []string{"latency", "qps"},
	}

	data, _ := json.Marshal(payload)
	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-1",
		CommandType: CommandTypeCollectionMetrics,
		Payload:     data,
	}

	errMsg := handler(cmd)
	assert.Empty(t, errMsg)
}

// TestCollectionMetricsHandlerWithInvalidPayload tests with bad payload
func TestCollectionMetricsHandlerWithInvalidPayload(t *testing.T) {
	handler := NewCollectionMetricsHandler()

	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-1",
		CommandType: CommandTypeCollectionMetrics,
		Payload:     []byte("{invalid json"),
	}

	errMsg := handler(cmd)
	assert.NotEmpty(t, errMsg)
	assert.Contains(t, errMsg, "failed to parse payload")
}

// TestPushConfigHandlerWithInvalidPayload tests with bad payload
func TestPushConfigHandlerWithInvalidPayload(t *testing.T) {
	handler := NewPushConfigHandler()

	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-1",
		CommandType: CommandTypePushConfig,
		Payload:     []byte("{invalid json"),
	}

	errMsg := handler(cmd)
	assert.NotEmpty(t, errMsg)
	assert.Contains(t, errMsg, "failed to parse payload")
}

// TestCommandRouterWithEmptyCommands tests routing with no commands
func TestCommandRouterWithEmptyCommands(t *testing.T) {
	router := NewCommandRouter()
	router.RegisterHandler(CommandTypeCollectionMetrics, NewCollectionMetricsHandler())

	replies := router.RouteAndCollectReplies([]*commonpb.ClientCommand{})
	assert.Empty(t, replies)
}

// TestCommandRouterRouteNilCommand tests routing with nil command
func TestCommandRouterRouteNilCommand(t *testing.T) {
	router := NewCommandRouter()

	// RouteAndCollectReplies should skip nil commands
	replies := router.RouteAndCollectReplies([]*commonpb.ClientCommand{
		nil,
		{
			CommandId:   "cmd-1",
			CommandType: "unknown",
			Payload:     []byte{},
		},
	})

	// Should only route the non-nil command
	assert.Equal(t, 1, len(replies))
	assert.False(t, replies[0].Success)
}

// Mock collector for router tests
type mockCollectorForRouter struct {
	errors []string
}

func (m *mockCollectorForRouter) GetRecentErrors(maxCount int) {
}
