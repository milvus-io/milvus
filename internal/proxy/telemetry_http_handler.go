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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// TelemetryAuthMiddleware creates a Gin middleware that validates Basic Auth
// credentials against Milvus's authentication system.
// It checks if authentication is enabled and validates username/password.
func TelemetryAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if authorization is enabled
		if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
			c.Next()
			return
		}

		// Get Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing authorization header",
			})
			return
		}

		// Parse Basic Auth header
		if !strings.HasPrefix(authHeader, "Basic ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid authorization format, expected Basic auth",
			})
			return
		}

		// Decode Base64 credentials
		encoded := strings.TrimPrefix(authHeader, "Basic ")
		decoded, err := crypto.Base64Decode(encoded)
		if err != nil {
			log.Warn("TelemetryAuthMiddleware: failed to decode credentials", zap.Error(err))
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid credentials encoding",
			})
			return
		}

		// Parse username:password
		parts := strings.SplitN(decoded, ":", 2)
		if len(parts) != 2 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid credentials format",
			})
			return
		}

		username := parts[0]
		password := parts[1]

		// Validate credentials using Milvus auth system
		if !passwordVerify(c.Request.Context(), username, password, privilege.GetPrivilegeCache()) {
			log.Warn("TelemetryAuthMiddleware: authentication failed", zap.String("username", username))
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid username or password",
			})
			return
		}

		// Store username in context for potential use by handlers
		c.Set("username", username)
		c.Next()
	}
}

// getTelemetryClients returns all connected clients with optional filtering
// Query params:
//   - database: filter clients by accessed database
//   - client_id: filter to specific client
//   - include_metrics: include operation metrics (true/false)
func getTelemetryClients(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		// Parse query parameters
		database := c.Query("database")
		clientID := c.Query("client_id")
		includeMetrics := c.Query("include_metrics") == "true"

		// Build request to RootCoord
		req := &milvuspb.GetClientTelemetryRequest{
			Database:       database,
			ClientId:       clientID,
			IncludeMetrics: includeMetrics,
		}

		// Call RootCoord via RPC
		resp, err := node.GetClientTelemetry(ctx, req)
		if err != nil {
			log.Ctx(ctx).Warn("getTelemetryClients: failed to get client telemetry",
				zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		// Check response status
		if !merr.Ok(resp.Status) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		// Convert to API response format
		wrapped := ConvertClientTelemetryResponse(resp)
		c.JSON(http.StatusOK, wrapped)
	}
}

// getTelemetryClientMetrics returns detailed metrics for a specific client
func getTelemetryClientMetrics(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		clientID := c.Param("clientId")
		if clientID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "client_id parameter is required",
			})
			return
		}

		// Call with client_id filter
		req := &milvuspb.GetClientTelemetryRequest{
			ClientId:       clientID,
			IncludeMetrics: true,
		}

		resp, err := node.GetClientTelemetry(ctx, req)
		if err != nil {
			log.Ctx(ctx).Warn("getTelemetryClientMetrics: failed to get client metrics",
				zap.Error(err),
				zap.String("client_id", clientID))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		wrapped := ConvertClientTelemetryResponse(resp)
		c.JSON(http.StatusOK, wrapped)
	}
}

// postTelemetryCommand pushes a command to clients
// JSON body:
//
//	{
//	  "command_type": "show_errors|collection_metrics|debug_log|push_config",
//	  "target_client_id": "client-123" or "" for global,
//	  "target_database": "db_name" or "" for global (mutually exclusive with target_client_id),
//	  "payload": {...},
//	  "ttl_seconds": 3600,
//	  "persistent": false
//	}
func postTelemetryCommand(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		// Parse request body
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "failed to read request body",
			})
			return
		}

		var cmdReq struct {
			CommandType    string          `json:"command_type"`
			TargetClientID string          `json:"target_client_id"`
			TargetDatabase string          `json:"target_database"`
			Payload        json.RawMessage `json:"payload"`
			TTLSeconds     int64           `json:"ttl_seconds"`
			Persistent     bool            `json:"persistent"`
		}

		if err := json.Unmarshal(body, &cmdReq); err != nil {
			log.Ctx(ctx).Warn("postTelemetryCommand: failed to parse request",
				zap.Error(err))
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}

		// Validate command type
		if cmdReq.CommandType == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "command_type is required",
			})
			return
		}

		payload := bytes.TrimSpace(cmdReq.Payload)
		var payloadBytes []byte
		if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
			payloadBytes = nil
		} else if payload[0] == '"' {
			var unquoted string
			if err := json.Unmarshal(payload, &unquoted); err == nil {
				payloadBytes = []byte(unquoted)
			} else {
				payloadBytes = payload
			}
		} else {
			payloadBytes = payload
		}

		// Build RPC request
		pushReq := &milvuspb.PushClientCommandRequest{
			CommandType:    cmdReq.CommandType,
			TargetClientId: cmdReq.TargetClientID,
			TargetDatabase: cmdReq.TargetDatabase,
			Payload:        payloadBytes,
			TtlSeconds:     cmdReq.TTLSeconds,
			Persistent:     cmdReq.Persistent,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			log.Ctx(ctx).Warn("postTelemetryCommand: failed to push command",
				zap.Error(err),
				zap.String("command_type", cmdReq.CommandType))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			log.Ctx(ctx).Warn("postTelemetryCommand: rpc returned error",
				zap.String("reason", resp.Status.Reason))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"command_id": resp.CommandId,
			"status":     "created",
		})
	}
}

// deleteTelemetryCommand removes a command
// URL param: commandId
func deleteTelemetryCommand(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		commandID := c.Param("commandId")
		if commandID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "command_id parameter is required",
			})
			return
		}

		delReq := &milvuspb.DeleteClientCommandRequest{
			CommandId: commandID,
		}

		resp, err := node.DeleteClientCommand(ctx, delReq)
		if err != nil {
			log.Ctx(ctx).Warn("deleteTelemetryCommand: failed to delete command",
				zap.Error(err),
				zap.String("command_id", commandID))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"command_id": commandID,
			"status":     "deleted",
		})
	}
}

// getTelemetryClientHistory returns historical metrics for a specific client
// Query params:
//   - start_time: RFC3339 format start time
//   - end_time: RFC3339 format end time
//   - detail: "true" to return all snapshots instead of aggregated metrics (default: aggregated)
func getTelemetryClientHistory(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		clientID := c.Param("clientId")
		if clientID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "client_id parameter is required",
			})
			return
		}

		startTime := c.Query("start_time")
		endTime := c.Query("end_time")
		detail := c.Query("detail") == "true"

		if startTime == "" || endTime == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "start_time and end_time query parameters are required (RFC3339 format)",
			})
			return
		}

		// Build the payload for show_latency_history command
		payload := map[string]interface{}{
			"start_time": startTime,
			"end_time":   endTime,
			"detail":     detail,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to marshal payload: " + err.Error(),
			})
			return
		}

		// Push command to the specific client
		pushReq := &milvuspb.PushClientCommandRequest{
			CommandType:    "show_latency_history",
			TargetClientId: clientID,
			Payload:        payloadBytes,
			TtlSeconds:     0, // Keep until client replies; server deletes on reply
			Persistent:     false,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			log.Ctx(ctx).Warn("getTelemetryClientHistory: failed to push command",
				zap.Error(err),
				zap.String("client_id", clientID))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		// Return the command ID - client will need to poll for results
		// via the command reply in the next heartbeat
		c.JSON(http.StatusOK, gin.H{
			"command_id": resp.CommandId,
			"status":     "pending",
			"message":    "Command sent to client. Results will be available in the client's next heartbeat response.",
		})
	}
}

// getTelemetryClientConfig sends a get_config command to a specific client
// The client will respond with its configuration in the next heartbeat
// URL param: clientId
func getTelemetryClientConfig(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		clientID := c.Param("clientId")
		if clientID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "client_id parameter is required",
			})
			return
		}

		// Push get_config command to the specific client
		pushReq := &milvuspb.PushClientCommandRequest{
			CommandType:    "get_config",
			TargetClientId: clientID,
			TtlSeconds:     0, // Keep until client replies; server deletes on reply
			Persistent:     false,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			log.Ctx(ctx).Warn("getTelemetryClientConfig: failed to push command",
				zap.Error(err),
				zap.String("client_id", clientID))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": resp.Status.Reason,
			})
			return
		}

		// Return the command ID - client will respond with config in next heartbeat
		c.JSON(http.StatusOK, gin.H{
			"command_id": resp.CommandId,
			"status":     "pending",
			"message":    "Command sent to client. Config will be available in the client's command reply.",
		})
	}
}
