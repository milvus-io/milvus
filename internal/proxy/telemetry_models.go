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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// CommandReply represents a client's response to a command
// NOTE: Payload and CommandPayload are stored as strings (not []byte) to ensure
// proper JSON serialization without base64 encoding, making the API response
// directly parseable by JavaScript clients.
type CommandReply struct {
	CommandID      string `json:"command_id"`
	CommandType    string `json:"command_type,omitempty"`
	CommandPayload string `json:"command_payload,omitempty"`
	Success        bool   `json:"success"`
	ErrorMsg       string `json:"error_msg,omitempty"`
	Payload        string `json:"payload,omitempty"`
	ReceivedAt     int64  `json:"received_at"`
}

// ClientMetrics wraps client telemetry data for JSON API responses
type ClientMetrics struct {
	ClientID          string                       `json:"client_id"`
	ClientInfo        *commonpb.ClientInfo         `json:"client_info"`
	LastHeartbeatTime int64                        `json:"last_heartbeat_time"`
	Status            string                       `json:"status"`
	Databases         []string                     `json:"databases"`
	Metrics           []*commonpb.OperationMetrics `json:"metrics,omitempty"`
	CommandReplies    []*CommandReply              `json:"command_replies,omitempty"`
}

// ClientTelemetryResponse wraps GetClientTelemetry response for JSON API
type ClientTelemetryResponse struct {
	Clients    []ClientMetrics   `json:"clients"`
	Aggregated *commonpb.Metrics `json:"aggregated,omitempty"`
}

// CommandResponse wraps command info for JSON API
type CommandResponse struct {
	CommandID   string `json:"command_id"`
	CommandType string `json:"command_type"`
	TargetScope string `json:"target_scope"`
	CreateTime  int64  `json:"create_time"`
	Persistent  bool   `json:"persistent"`
	PayloadSize int    `json:"payload_size"`
}

// ConvertClientTelemetryResponse converts proto GetClientTelemetryResponse to API response
// No caching needed - caching is handled by CommandStore internally
func ConvertClientTelemetryResponse(resp *milvuspb.GetClientTelemetryResponse) *ClientTelemetryResponse {
	if resp == nil {
		return &ClientTelemetryResponse{
			Clients: []ClientMetrics{},
		}
	}

	clients := make([]ClientMetrics, len(resp.Clients))
	for i, ct := range resp.Clients {
		// Extract client_id from ClientInfo.Reserved map
		clientID := ""
		var commandReplies []*CommandReply
		if ct.ClientInfo != nil && ct.ClientInfo.Reserved != nil {
			clientID = ct.ClientInfo.Reserved["client_id"]

			// Parse command_replies from Reserved field
			if repliesJSON := ct.ClientInfo.Reserved["command_replies"]; repliesJSON != "" {
				if err := json.Unmarshal([]byte(repliesJSON), &commandReplies); err != nil {
					// Log error but don't fail the entire response
					commandReplies = nil
				}
			}
		}

		clients[i] = ClientMetrics{
			ClientID:          clientID,
			ClientInfo:        ct.ClientInfo,
			LastHeartbeatTime: ct.LastHeartbeatTime,
			Status:            ct.Status,
			Databases:         ct.Databases,
			Metrics:           ct.Metrics,
			CommandReplies:    commandReplies,
		}
	}

	return &ClientTelemetryResponse{
		Clients:    clients,
		Aggregated: resp.Aggregated,
	}
}

// ConvertCommandResponse converts proto ClientCommand to API response
func ConvertCommandResponse(cmd *commonpb.ClientCommand, persistent bool) *CommandResponse {
	return &CommandResponse{
		CommandID:   cmd.CommandId,
		CommandType: cmd.CommandType,
		TargetScope: cmd.TargetScope,
		CreateTime:  cmd.CreateTime,
		Persistent:  persistent,
		PayloadSize: len(cmd.Payload),
	}
}
