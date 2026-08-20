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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
			mlog.Warn(context.TODO(), "TelemetryAuthMiddleware: failed to decode credentials", mlog.Err(err))
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
			mlog.Warn(context.TODO(), "TelemetryAuthMiddleware: authentication failed", mlog.String("username", username))
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid username or password",
			})
			return
		}

		// Store username in context for potential use by handlers
		c.Set("username", username)
		authCtx := metadata.NewIncomingContext(c.Request.Context(), metadata.Pairs(
			util.HeaderAuthorize,
			crypto.Base64Encode(username+util.CredentialSeparator+password),
		))
		c.Request = c.Request.WithContext(authCtx)
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
			mlog.Warn(ctx, "getTelemetryClients: failed to get client telemetry",
				mlog.Err(err))
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

// respondToPushedCommand finishes an endpoint that just pushed a command to one client.
//
// The reply is not available yet -- the client answers on its next heartbeat -- so this
// returns the command ID immediately and the caller collects the result later from the
// command-reply endpoint. The body has the same shape that endpoint returns, so a caller
// parses one thing either way.
func respondToPushedCommand(c *gin.Context, node *Proxy, clientID, commandID string) {
	c.JSON(http.StatusOK, commandReplyPayload(commandID, clientID, nil, 0))
}

// getTelemetryCommandReply returns a client's reply to a previously pushed command.
//
// Commands are answered asynchronously, on the client's next heartbeat, so a caller that
// pushed a command needs a way to collect the result. Without this endpoint the only way
// was to list every client and scan the command_replies array by hand.
//
// URL param: commandId
// Query params:
//   - client_id: the client the command was sent to. Optional, but strongly preferred --
//     it turns a scan of every cached client into a lookup of one. Each client in the scan
//     contributes its entire stored reply history to the response, since command_replies is
//     encoded into ClientInfo.Reserved regardless of IncludeMetrics, so an untargeted
//     lookup costs proportionally to the size of the fleet.
//
// This is a single lookup, not a subscription: it returns what is known right now. There is
// deliberately no server-side blocking mode. A caller that wants to wait polls this endpoint
// on its own schedule, which keeps the cost of waiting where the caller can see and control
// it -- one request, one internal query -- instead of turning a single HTTP request into
// dozens of full-history transfers inside the cluster.
//
// Always 200 on a successful lookup; branch on the "status" field ("done" or "pending").
// "pending" is a normal state, not an error: the client answers on its next heartbeat, and
// replies are evicted once a client accumulates more than 50 of them. "responded" and
// "observed_clients" are observations, not a progress bar -- the server does not record
// which clients a broadcast command reached, so neither number establishes completeness.
func getTelemetryCommandReply(node *Proxy) gin.HandlerFunc {
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
				"error": "commandId parameter is required",
			})
			return
		}

		clientID := c.Query("client_id")

		replies, known, err := findCommandReplies(ctx, node, clientID, commandID)
		if err != nil {
			mlog.Warn(ctx, "getTelemetryCommandReply: failed to look up reply",
				mlog.Err(err),
				mlog.String("command_id", commandID))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, commandReplyPayload(commandID, clientID, replies, known))
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
			mlog.Warn(ctx, "getTelemetryClientMetrics: failed to get client metrics",
				mlog.Err(err),
				mlog.String("client_id", clientID))
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
//	  "ttl_seconds": 3600,   // optional, see below
//	  "persistent": false
//	}
//
// ttl_seconds bounds how long an unanswered one-time command occupies RootCoord memory. It
// is not a delivery window and deliberately does not encode a number of heartbeat cycles,
// because the server is never told a client's heartbeat interval.
//
//	omitted   -> one hour
//	0         -> never expires
//	positive  -> expires that many seconds after the push
//	negative  -> never expires
//
// A reply reclaims a command early only when it named a single client. A global or
// database-scoped command is answered by many clients on their own heartbeats, so its TTL
// is the only thing that ever removes it -- and until then it is still delivered to clients
// that connect later. Combining a broadcast scope with ttl_seconds: 0 therefore creates a
// command that never goes away and keeps being handed to every new client.
//
// Persistent configs ignore ttl_seconds entirely.
// listTelemetryCommands returns the commands the coordinator is currently holding: one-time
// commands that have neither expired nor been answered, and persistent configs.
//
// The UI's command panel has always called this endpoint; until it existed the panel showed
// an empty list whatever was actually pending, which is worse than showing nothing.
func listTelemetryCommands(node *Proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if node == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "proxy node not initialized",
			})
			return
		}

		resp, err := node.ListClientCommands(ctx, &rootcoordpb.ListClientCommandsRequest{})
		if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
			mlog.Warn(ctx, "listTelemetryCommands: failed to list commands", mlog.Err(err))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		commands := make([]*CommandResponse, 0, len(resp.GetCommands()))
		for _, cmd := range resp.GetCommands() {
			commands = append(commands, ConvertCommandResponse(cmd, cmd.GetPersistent()))
		}

		// Always an array, never null: the page branches on length, and a null would read
		// as a broken response rather than as "nothing outstanding".
		c.JSON(http.StatusOK, gin.H{"commands": commands})
	}
}

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
			// A pointer because JSON, unlike the RPC field, can distinguish an omitted
			// ttl_seconds from an explicit 0. That distinction is resolved here and never
			// travels: the RPC carries a concrete value.
			TTLSeconds *int64 `json:"ttl_seconds"`
			Persistent bool   `json:"persistent"`
		}

		if err := json.Unmarshal(body, &cmdReq); err != nil {
			mlog.Warn(ctx, "postTelemetryCommand: failed to parse request",
				mlog.Err(err))
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
			TtlSeconds:     resolveCommandTTL(cmdReq.TTLSeconds),
			Persistent:     cmdReq.Persistent,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			mlog.Warn(ctx, "postTelemetryCommand: failed to push command",
				mlog.Err(err),
				mlog.String("command_type", cmdReq.CommandType))
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if !merr.Ok(resp.Status) {
			mlog.Warn(ctx, "postTelemetryCommand: rpc returned error",
				mlog.String("reason", resp.Status.Reason))
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
			mlog.Warn(ctx, "deleteTelemetryCommand: failed to delete command",
				mlog.Err(err),
				mlog.String("command_id", commandID))
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
//
// The client answers on its next heartbeat, so this returns a command ID rather than the
// answer; collect it from the command-reply endpoint. If the client stays offline for over
// an hour the command expires unfetched and the answer never arrives -- re-issue it once
// the client is back.
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
			// Bounded on purpose: an answer an hour late is of no use to whoever asked,
			// and an unbounded command leaks if the client never comes back. The cost is
			// that a client offline for over an hour never sees this command.
			TtlSeconds: defaultCommandTTLSeconds,
			Persistent: false,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			mlog.Warn(ctx, "getTelemetryClientHistory: failed to push command",
				mlog.Err(err),
				mlog.String("client_id", clientID))
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

		respondToPushedCommand(c, node, clientID, resp.CommandId)
	}
}

// getTelemetryClientConfig sends a get_config command to a specific client
// The client will respond with its configuration in the next heartbeat
// URL param: clientId
//
// The client answers on its next heartbeat, so this returns a command ID rather than the
// answer; collect it from the command-reply endpoint. If the client stays offline for over
// an hour the command expires unfetched and the answer never arrives -- re-issue it once
// the client is back.
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
			// Bounded on purpose: an answer an hour late is of no use to whoever asked,
			// and an unbounded command leaks if the client never comes back. The cost is
			// that a client offline for over an hour never sees this command.
			TtlSeconds: defaultCommandTTLSeconds,
			Persistent: false,
		}

		resp, err := node.PushClientCommand(ctx, pushReq)
		if err != nil {
			mlog.Warn(ctx, "getTelemetryClientConfig: failed to push command",
				mlog.Err(err),
				mlog.String("client_id", clientID))
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

		respondToPushedCommand(c, node, clientID, resp.CommandId)
	}
}
