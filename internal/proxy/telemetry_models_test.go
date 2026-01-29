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
