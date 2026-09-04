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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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

// ClientMetrics wraps client telemetry data for JSON API responses.
//
// The three collection fields are always present and always an array, never null and never
// absent. Metrics in particular covers one heartbeat window rather than the client's whole
// life -- counters reset as the client takes each snapshot -- so a client that has been
// quiet legitimately reports none, and "no operations in that window" must not be
// indistinguishable from a missing or broken field. Consumers can rely on `metrics`
// existing and branch on its length; combined with `status`, an empty array plus "active"
// means idle, while "inactive" means the client stopped heartbeating.
//
// The window reported is the older of the two the coordinator retains, so it lags the
// client by one heartbeat interval and one idle interval does not blank it. See
// servedMetricsLocked in internal/rootcoord/telemetry for why it is served that way.
type ClientMetrics struct {
	ClientID          string                       `json:"client_id"`
	ClientInfo        *commonpb.ClientInfo         `json:"client_info"`
	LastHeartbeatTime int64                        `json:"last_heartbeat_time"`
	Status            string                       `json:"status"`
	Databases         []string                     `json:"databases"`
	Metrics           []*commonpb.OperationMetrics `json:"metrics"`
	CommandReplies    []*CommandReply              `json:"command_replies"`
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

// orEmpty returns s, or an empty slice when s is nil, so it marshals to [] rather than null.
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
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
			// A nil slice marshals to null; emit an empty array so every consumer sees
			// the same shape whether or not the client had anything to report.
			Databases:      orEmpty(ct.Databases),
			Metrics:        orEmpty(ct.Metrics),
			CommandReplies: orEmpty(commandReplies),
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
