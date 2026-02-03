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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// CommandType constants for different command types
const (
	// CommandTypeShowErrors: display last 100 error messages
	CommandTypeShowErrors = "show_errors"
	// CommandTypeCollectionMetrics: enable fine-grained collection-level metrics
	CommandTypeCollectionMetrics = "collection_metrics"
	// CommandTypePushConfig: push persistent configuration to clients
	CommandTypePushConfig = "push_config"
)

// ErrorMessagesPayload represents the payload for show_errors command
type ErrorMessagesPayload struct {
	// MaxCount: maximum number of errors to return (default: 100)
	MaxCount int `json:"max_count,omitempty"`
}

// CollectionMetricsPayload represents the payload for collection_metrics command
type CollectionMetricsPayload struct {
	// Collections: list of collection names to enable fine-grained metrics
	// Use ["*"] to enable/disable metrics for all collections
	Collections []string `json:"collections"`
	// Enabled: true to enable, false to disable fine-grained metrics
	Enabled bool `json:"enabled"`
	// MetricsTypes: optional list of metric types to collect (e.g., latency, qps)
	MetricsTypes []string `json:"metrics_types,omitempty"`
}

// PushConfigPayload represents the payload for push_config command
// This payload allows dynamic configuration of client telemetry settings
type PushConfigPayload struct {
	// Enabled: enable or disable telemetry collection on client
	Enabled *bool `json:"enabled,omitempty"`
	// HeartbeatIntervalMs: heartbeat interval in milliseconds
	HeartbeatIntervalMs *int64 `json:"heartbeat_interval_ms,omitempty"`
	// SamplingRate: sampling rate for telemetry (0.0-1.0)
	SamplingRate *float64 `json:"sampling_rate,omitempty"`
	// TTLSeconds: time-to-live for this configuration (0 means persistent)
	TTLSeconds int64 `json:"ttl_seconds,omitempty"`
}

// CommandHandler handles a specific command type
// Returns error message if handler fails, empty string if successful
type CommandHandler func(cmd *commonpb.ClientCommand) string

// NewShowErrorsHandler creates a handler for show_errors command
func NewShowErrorsHandler(errorCollector ErrorCollector) CommandHandler {
	return func(cmd *commonpb.ClientCommand) string {
		payload := &ErrorMessagesPayload{
			MaxCount: 100, // default
		}
		if len(cmd.Payload) > 0 {
			if err := json.Unmarshal(cmd.Payload, payload); err != nil {
				return "failed to parse payload: " + err.Error()
			}
		}

		// Collect last N errors if collector is available
		if errorCollector != nil {
			errorCollector.GetRecentErrors(payload.MaxCount)
		}

		return ""
	}
}

// NewCollectionMetricsHandler creates a handler for collection_metrics command
// Supports "*" wildcard to enable/disable metrics for all collections
func NewCollectionMetricsHandler() CommandHandler {
	return func(cmd *commonpb.ClientCommand) string {
		payload := &CollectionMetricsPayload{}
		if err := json.Unmarshal(cmd.Payload, payload); err != nil {
			return "failed to parse payload: " + err.Error()
		}

		// Validate collection list is not empty when enabling
		if payload.Enabled && len(payload.Collections) == 0 {
			return "collections list cannot be empty when enabled=true"
		}

		// Handler validates and accepts the configuration
		// Client will receive this command and start collecting fine-grained metrics
		// for specified collections. Use "*" to enable/disable all collections.
		return ""
	}
}

// NewPushConfigHandler creates a handler for push_config command
// This command is persistent and stored in etcd
func NewPushConfigHandler() CommandHandler {
	return func(cmd *commonpb.ClientCommand) string {
		payload := &PushConfigPayload{}
		if err := json.Unmarshal(cmd.Payload, payload); err != nil {
			return "failed to parse payload: " + err.Error()
		}

		// Validate at least one config field is set
		if payload.Enabled == nil && payload.HeartbeatIntervalMs == nil && payload.SamplingRate == nil {
			return "at least one config field must be set (enabled, heartbeat_interval_ms, or sampling_rate)"
		}

		// Validate sampling rate range if provided
		if payload.SamplingRate != nil && (*payload.SamplingRate < 0.0 || *payload.SamplingRate > 1.0) {
			return "sampling_rate must be between 0.0 and 1.0"
		}

		// Validate heartbeat interval if provided
		if payload.HeartbeatIntervalMs != nil && *payload.HeartbeatIntervalMs <= 0 {
			return "heartbeat_interval_ms must be positive"
		}

		// Handler accepts configuration
		// Persistent commands with TTL are stored in etcd
		return ""
	}
}

// ErrorCollector interface for collecting and retrieving errors
type ErrorCollector interface {
	// GetRecentErrors returns the most recent N errors
	GetRecentErrors(maxCount int)
}
