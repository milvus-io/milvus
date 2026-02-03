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

package proxy

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestConvertClientTelemetryResponse(t *testing.T) {
	resp := &milvuspb.GetClientTelemetryResponse{
		Status: &commonpb.Status{},
		Clients: []*milvuspb.ClientTelemetry{
			{
				ClientInfo: &commonpb.ClientInfo{
					SdkType: "pymilvus",
					Host:    "192.168.1.100",
					Reserved: map[string]string{
						"client_id": "client-123",
					},
				},
				Status:    "active",
				Databases: []string{"default"},
				Metrics: []*commonpb.OperationMetrics{
					{
						Operation: "Search",
						Global: &commonpb.Metrics{
							RequestCount: 100,
							SuccessCount: 95,
							ErrorCount:   5,
						},
					},
				},
			},
		},
		Aggregated: &commonpb.Metrics{
			RequestCount: 100,
			SuccessCount: 95,
			ErrorCount:   5,
		},
	}

	wrapped := ConvertClientTelemetryResponse(resp)
	assert.NotNil(t, wrapped)
	assert.Equal(t, 1, len(wrapped.Clients))
	assert.Equal(t, "client-123", wrapped.Clients[0].ClientID)
	assert.Equal(t, "pymilvus", wrapped.Clients[0].ClientInfo.SdkType)
	assert.Equal(t, "active", wrapped.Clients[0].Status)
	assert.Equal(t, 1, len(wrapped.Clients[0].Databases))
	assert.Equal(t, int64(100), wrapped.Aggregated.RequestCount)
}

func TestConvertClientTelemetryResponseEmpty(t *testing.T) {
	resp := &milvuspb.GetClientTelemetryResponse{
		Status:  &commonpb.Status{},
		Clients: []*milvuspb.ClientTelemetry{},
	}

	wrapped := ConvertClientTelemetryResponse(resp)
	assert.NotNil(t, wrapped)
	assert.Equal(t, 0, len(wrapped.Clients))
}

func TestConvertClientTelemetryResponseNil(t *testing.T) {
	wrapped := ConvertClientTelemetryResponse(nil)
	assert.NotNil(t, wrapped)
	assert.Equal(t, 0, len(wrapped.Clients))
}

func TestConvertClientTelemetryResponseWithoutClientID(t *testing.T) {
	// Test when ClientInfo doesn't have client_id in reserved map
	resp := &milvuspb.GetClientTelemetryResponse{
		Status: &commonpb.Status{},
		Clients: []*milvuspb.ClientTelemetry{
			{
				ClientInfo: &commonpb.ClientInfo{
					SdkType: "pymilvus",
					Host:    "192.168.1.100",
				},
				Status: "active",
			},
		},
	}

	wrapped := ConvertClientTelemetryResponse(resp)
	assert.NotNil(t, wrapped)
	assert.Equal(t, 1, len(wrapped.Clients))
	assert.Equal(t, "", wrapped.Clients[0].ClientID)
}

func TestConvertCommandResponse(t *testing.T) {
	cmd := &commonpb.ClientCommand{
		CommandId:   "cmd-123",
		CommandType: "show_errors",
		TargetScope: "global",
		CreateTime:  1674067200000,
		Payload:     []byte(`{"max_errors": 100}`),
	}

	wrapped := ConvertCommandResponse(cmd, false)
	assert.NotNil(t, wrapped)
	assert.Equal(t, "cmd-123", wrapped.CommandID)
	assert.Equal(t, "show_errors", wrapped.CommandType)
	assert.Equal(t, "global", wrapped.TargetScope)
	assert.Equal(t, int64(1674067200000), wrapped.CreateTime)
	assert.Equal(t, false, wrapped.Persistent)
	assert.Equal(t, 19, wrapped.PayloadSize)
}

func TestConvertCommandResponsePersistent(t *testing.T) {
	cmd := &commonpb.ClientCommand{
		CommandId:   "cfg-456",
		CommandType: "push_config",
		TargetScope: "client:client-789",
		CreateTime:  1674067200000,
		Payload:     []byte(`{}`),
	}

	wrapped := ConvertCommandResponse(cmd, true)
	assert.Equal(t, true, wrapped.Persistent)
}

// TestCommandReplyJSONSerialization verifies that CommandReply.Payload is serialized
// as a plain string (not base64 encoded) for proper JavaScript parsing
func TestCommandReplyJSONSerialization(t *testing.T) {
	t.Run("payload should be string in JSON output", func(t *testing.T) {
		reply := &CommandReply{
			CommandID:      "cmd-123",
			CommandType:    "get_config",
			CommandPayload: `{"setting": "value"}`,
			Success:        true,
			Payload:        `{"user_config": {"address": "localhost:19530"}, "pushed_config": {}}`,
			ReceivedAt:     1234567890,
		}

		// Serialize to JSON
		data, err := json.Marshal(reply)
		assert.NoError(t, err)

		// The payload should NOT be base64 encoded
		// It should appear as a JSON-escaped string in the output
		jsonStr := string(data)
		assert.Contains(t, jsonStr, `"payload":"{\"user_config\"`)
		assert.NotContains(t, jsonStr, "eyJ1c2VyX2NvbmZpZyI") // base64 prefix for {"user_config

		// Verify it can be deserialized back
		var deserialized CommandReply
		err = json.Unmarshal(data, &deserialized)
		assert.NoError(t, err)
		assert.Equal(t, reply.Payload, deserialized.Payload)
		assert.Equal(t, reply.CommandPayload, deserialized.CommandPayload)
	})

	t.Run("frontend can parse payload directly", func(t *testing.T) {
		// Simulate what the API returns
		clientMetrics := ClientMetrics{
			ClientID: "client-123",
			CommandReplies: []*CommandReply{
				{
					CommandID:   "cmd-get-config",
					CommandType: "get_config",
					Success:     true,
					Payload:     `{"user_config": {"telemetry_enabled": true}, "pushed_config": {}}`,
					ReceivedAt:  1234567890,
				},
			},
		}

		// Serialize the API response
		data, err := json.Marshal(clientMetrics)
		assert.NoError(t, err)

		// Parse as generic JSON (simulating JavaScript JSON.parse)
		var parsed map[string]interface{}
		err = json.Unmarshal(data, &parsed)
		assert.NoError(t, err)

		// Get command_replies array
		repliesRaw, ok := parsed["command_replies"].([]interface{})
		assert.True(t, ok)
		assert.Len(t, repliesRaw, 1)

		// Get first reply
		replyMap, ok := repliesRaw[0].(map[string]interface{})
		assert.True(t, ok)

		// Get payload as string
		payloadStr, ok := replyMap["payload"].(string)
		assert.True(t, ok, "payload should be a string, not base64 or array")

		// JavaScript can JSON.parse this directly
		var payloadData map[string]interface{}
		err = json.Unmarshal([]byte(payloadStr), &payloadData)
		assert.NoError(t, err)
		assert.Contains(t, payloadData, "user_config")
		assert.Contains(t, payloadData, "pushed_config")
	})
}

// TestConvertClientTelemetryResponseWithCommandReplies tests command replies parsing
func TestConvertClientTelemetryResponseWithCommandReplies(t *testing.T) {
	// Simulate server-side StoredCommandReply JSON (with string payloads)
	repliesJSON := `[{"command_id":"cmd-1","command_type":"get_config","success":true,"payload":"{\"user_config\":{\"address\":\"localhost\"}}","received_at":1234567890}]`

	resp := &milvuspb.GetClientTelemetryResponse{
		Status: &commonpb.Status{},
		Clients: []*milvuspb.ClientTelemetry{
			{
				ClientInfo: &commonpb.ClientInfo{
					SdkType: "GoMilvusClient",
					Reserved: map[string]string{
						"client_id":       "client-123",
						"command_replies": repliesJSON,
					},
				},
				Status: "active",
			},
		},
	}

	wrapped := ConvertClientTelemetryResponse(resp)
	assert.NotNil(t, wrapped)
	assert.Len(t, wrapped.Clients, 1)
	assert.Len(t, wrapped.Clients[0].CommandReplies, 1)

	reply := wrapped.Clients[0].CommandReplies[0]
	assert.Equal(t, "cmd-1", reply.CommandID)
	assert.Equal(t, "get_config", reply.CommandType)
	assert.True(t, reply.Success)
	assert.Equal(t, `{"user_config":{"address":"localhost"}}`, reply.Payload)
}
