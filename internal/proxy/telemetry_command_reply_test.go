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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// telemetryRespWithReply builds the shape the server actually returns: replies are JSON
// encoded into ClientInfo.Reserved["command_replies"], not a dedicated proto field.
func telemetryRespWithReply(clientID, commandID string, success bool, payload string) *milvuspb.GetClientTelemetryResponse {
	replies := []map[string]interface{}{
		{
			"command_id":   commandID,
			"command_type": "get_config",
			"success":      success,
			"payload":      payload,
			"received_at":  int64(1700000000000),
		},
	}
	encoded, _ := json.Marshal(replies)

	return &milvuspb.GetClientTelemetryResponse{
		Status: merr.Success(),
		Clients: []*milvuspb.ClientTelemetry{
			{
				ClientInfo: &commonpb.ClientInfo{
					SdkType: "Go",
					Reserved: map[string]string{
						"client_id":       clientID,
						"command_replies": string(encoded),
					},
				},
			},
		},
	}
}

// telemetryRespManyReplies builds the answer to a command every client received -- what a
// command with no target_client_id and no target_database produces, since it is stored with
// scope "global" and matchesScope returns true for everyone. Every client answers the same
// command ID. clientIDs are listed out of order on purpose: the server iterates a sync.Map,
// whose order is unspecified, so the endpoint must impose its own.
func telemetryRespManyReplies(commandID string, clientIDs ...string) *milvuspb.GetClientTelemetryResponse {
	resp := &milvuspb.GetClientTelemetryResponse{Status: merr.Success()}
	for _, clientID := range clientIDs {
		encoded, _ := json.Marshal([]map[string]interface{}{
			{
				"command_id":   commandID,
				"command_type": "show_errors",
				"success":      true,
				"payload":      `{"from":"` + clientID + `"}`,
				"received_at":  int64(1700000000000),
			},
		})
		resp.Clients = append(resp.Clients, &milvuspb.ClientTelemetry{
			ClientInfo: &commonpb.ClientInfo{
				SdkType: "Go",
				Reserved: map[string]string{
					"client_id":       clientID,
					"command_replies": string(encoded),
				},
			},
		})
	}
	return resp
}

func telemetryRespNoReply(clientID string) *milvuspb.GetClientTelemetryResponse {
	return &milvuspb.GetClientTelemetryResponse{
		Status: merr.Success(),
		Clients: []*milvuspb.ClientTelemetry{
			{
				ClientInfo: &commonpb.ClientInfo{
					SdkType:  "Go",
					Reserved: map[string]string{"client_id": clientID},
				},
			},
		},
	}
}

func TestParseWaitParam(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"empty means no wait", "", 0, false},
		{"duration string", "30s", 30 * time.Second, false},
		{"compound duration", "1m30s", 90 * time.Second, false},
		{"bare seconds", "15", 15 * time.Second, false},
		{"fractional seconds", "0.5", 500 * time.Millisecond, false},
		{"negative clamps to zero", "-5s", 0, false},
		{"over the cap is clamped", "10m", maxTelemetryWait, false},
		{"garbage is rejected", "soon", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWaitParam(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

// TestResolveCommandTTL pins where the default lives and why. The proto documents
// ttl_seconds as "0 = no expiry", and a plain proto3 int64 collapses "omitted" and
// "explicit 0" into the same value -- so only this layer, which decodes JSON into a
// pointer, can apply a default without redefining what an existing caller's 0 means.
func TestResolveCommandTTL(t *testing.T) {
	ttl := func(v int64) *int64 { return &v }

	cases := []struct {
		name      string
		requested *int64
		expected  int64
	}{
		{"omitted gets the default", nil, defaultCommandTTLSeconds},
		{"explicit zero stays no-expiry", ttl(0), 0},
		{"positive is honored", ttl(120), 120},
		{"negative means never expire", ttl(-1), -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, resolveCommandTTL(tc.requested))
		})
	}

	assert.EqualValues(t, 3600, defaultCommandTTLSeconds,
		"a bound on memory, not a delivery window: the server is never told a client's heartbeat interval")
}

// TestPostCommandTTLFromJSON covers the same distinction through the decoder, since the
// pointer only helps if the request struct actually keeps it.
func TestPostCommandTTLFromJSON(t *testing.T) {
	decode := func(body string) int64 {
		var req struct {
			TTLSeconds *int64 `json:"ttl_seconds"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &req))
		return resolveCommandTTL(req.TTLSeconds)
	}

	assert.EqualValues(t, defaultCommandTTLSeconds, decode(`{"command_type":"show_errors"}`),
		"a body with no ttl_seconds must get the default")
	assert.EqualValues(t, 0, decode(`{"command_type":"show_errors","ttl_seconds":0}`),
		"an explicit 0 must survive as no-expiry, the meaning the proto documents")
	assert.EqualValues(t, 45, decode(`{"command_type":"show_errors","ttl_seconds":45}`))
}

func TestCommandReplyPayload(t *testing.T) {
	t.Run("pending when there is no reply", func(t *testing.T) {
		body := commandReplyPayload("cmd-1", "client-1", nil, 1, 0)

		assert.Equal(t, replyStatusPending, body["status"])
		assert.Equal(t, "cmd-1", body["command_id"])
		assert.Equal(t, "client-1", body["client_id"])
		assert.NotContains(t, body, "reply")
		assert.Contains(t, body, "message")
	})

	t.Run("done when a reply exists", func(t *testing.T) {
		reply := &CommandReply{CommandID: "cmd-1", Success: true}
		body := commandReplyPayload("cmd-1", "client-1",
			[]clientCommandReply{{ClientID: "client-1", Reply: reply}}, 1, 250*time.Millisecond)

		assert.Equal(t, replyStatusDone, body["status"])
		assert.Equal(t, reply, body["reply"])
		assert.Equal(t, int64(250), body["waited_ms"])
	})

	t.Run("omits client id when unknown", func(t *testing.T) {
		assert.NotContains(t, commandReplyPayload("cmd-1", "", nil, 0, 0), "client_id")
	})

	// replies must marshal as [] rather than null, for the same reason metrics does:
	// a caller must be able to read the field unconditionally.
	t.Run("replies is always an array", func(t *testing.T) {
		encoded, err := json.Marshal(commandReplyPayload("cmd-1", "", nil, 0, 0))
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"replies":[]`)
		assert.Contains(t, string(encoded), `"responded":0`)
	})

	// Both numbers are observations. observed_clients counts what the lookup scanned, not
	// what the command targeted, so neither reaching it nor falling short of it means
	// anything about completeness.
	t.Run("reports how many clients answered and how many were observed", func(t *testing.T) {
		body := commandReplyPayload("cmd-1", "",
			[]clientCommandReply{{ClientID: "a", Reply: &CommandReply{CommandID: "cmd-1"}}}, 3, 0)

		assert.Equal(t, 1, body["responded"])
		assert.Equal(t, 3, body["observed_clients"])
	})
}

func TestGetTelemetryCommandReplyHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	newCtx := func(url string, commandID string) (*httptest.ResponseRecorder, *gin.Context) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", url, nil)
		c.Params = gin.Params{{Key: "commandId", Value: commandID}}
		return w, c
	}

	t.Run("nil node returns error", func(t *testing.T) {
		w, c := newCtx("/", "cmd-1")
		getTelemetryCommandReply(nil)(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("missing command id is rejected", func(t *testing.T) {
		w, c := newCtx("/", "")
		getTelemetryCommandReply(&Proxy{})(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid wait is rejected", func(t *testing.T) {
		w, c := newCtx("/?wait=whenever", "cmd-1")
		getTelemetryCommandReply(&Proxy{})(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("returns the reply when the client has answered", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespWithReply("client-1", "cmd-1", true, `{"telemetry_enabled":true}`), nil)

		w, c := newCtx("/?client_id=client-1", "cmd-1")
		getTelemetryCommandReply(proxy)(c)

		require.Equal(t, http.StatusOK, w.Code)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replyStatusDone, body["status"])
		assert.Equal(t, "client-1", body["client_id"])

		reply := body["reply"].(map[string]interface{})
		assert.Equal(t, "cmd-1", reply["command_id"])
		assert.Equal(t, true, reply["success"])
		assert.Equal(t, `{"telemetry_enabled":true}`, reply["payload"])
	})

	t.Run("pending is a 200, not an error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespNoReply("client-1"), nil)

		w, c := newCtx("/?client_id=client-1", "cmd-1")
		getTelemetryCommandReply(proxy)(c)

		require.Equal(t, http.StatusOK, w.Code)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replyStatusPending, body["status"])
		assert.NotContains(t, body, "reply")
	})

	t.Run("a reply for a different command is not matched", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespWithReply("client-1", "some-other-command", true, "{}"), nil)

		w, c := newCtx("/", "cmd-1")
		getTelemetryCommandReply(proxy)(c)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replyStatusPending, body["status"])
	})

	t.Run("client_id turns the lookup into a targeted one", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		// Metrics must not be requested: replies are populated regardless, and pulling
		// metrics would make polling far more expensive than it needs to be.
		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.MatchedBy(
			func(req *milvuspb.GetClientTelemetryRequest) bool {
				return req.ClientId == "client-9" && !req.IncludeMetrics
			})).Return(telemetryRespWithReply("client-9", "cmd-1", true, "{}"), nil)

		w, c := newCtx("/?client_id=client-9", "cmd-1")
		getTelemetryCommandReply(proxy)(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("wait polls until the reply lands", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		// First look finds nothing; the client answers before the second.
		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespNoReply("client-1"), nil).Once()
		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespWithReply("client-1", "cmd-1", true, "{}"), nil).Once()

		w, c := newCtx("/?client_id=client-1&wait=5s", "cmd-1")

		start := time.Now()
		getTelemetryCommandReply(proxy)(c)
		elapsed := time.Since(start)

		require.Equal(t, http.StatusOK, w.Code)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replyStatusDone, body["status"], "should have picked up the reply on the second poll")
		assert.GreaterOrEqual(t, elapsed, telemetryReplyPollInterval, "must have actually waited a poll cycle")
		assert.Less(t, elapsed, 5*time.Second, "must return as soon as the reply lands, not burn the full budget")
	})

	t.Run("wait gives up and reports pending", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{mixCoord: mixCoord}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
			Return(telemetryRespNoReply("client-1"), nil)

		w, c := newCtx("/?client_id=client-1&wait=600ms", "cmd-1")

		start := time.Now()
		getTelemetryCommandReply(proxy)(c)
		elapsed := time.Since(start)

		require.Equal(t, http.StatusOK, w.Code)
		// The budget must be honored, not rounded up to the next poll tick.
		assert.GreaterOrEqual(t, elapsed, 600*time.Millisecond)
		assert.Less(t, elapsed, 900*time.Millisecond, "wait must not overshoot to the next poll interval")

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replyStatusPending, body["status"], "timing out must not be reported as an error")
	})
}

func TestWaitForCommandReplyRespectsCancellation(t *testing.T) {
	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
		Return(telemetryRespNoReply("client-1"), nil)

	// A caller that hangs up mid-wait must unblock promptly, well before the budget.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	replies, _, err := waitForCommandReply(ctx, proxy, "client-1", "cmd-1", 30*time.Second)

	assert.NoError(t, err, "cancellation is not a lookup failure")
	assert.Empty(t, replies)
	assert.Less(t, time.Since(start), 5*time.Second)
}

// TestBroadcastCommandReturnsEveryReply is the regression test for a command that every
// client answers. A command naming no target is stored with scope "global" and delivered to
// all of them under one command ID, so returning the first match found while ranging a
// sync.Map handed back one arbitrary client's answer as though it were the cluster's --
// non-deterministically, and with no hint that more existed.
func TestBroadcastCommandReturnsEveryReply(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
		Return(telemetryRespManyReplies("cmd-broadcast", "client-c", "client-a", "client-b"), nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/", nil)
	c.Params = gin.Params{{Key: "commandId", Value: "cmd-broadcast"}}

	getTelemetryCommandReply(proxy)(c)
	require.Equal(t, http.StatusOK, w.Code)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	assert.Equal(t, replyStatusDone, body["status"])
	assert.EqualValues(t, 3, body["responded"], "every answer must be reported, not just one")
	assert.EqualValues(t, 3, body["observed_clients"])

	replies, ok := body["replies"].([]interface{})
	require.True(t, ok, "replies must be present as an array")
	require.Len(t, replies, 3)

	// Ordered by client ID so that repeating the request is stable, and each answer is
	// attributable -- without the client_id an operator cannot tell whose data this is.
	var gotClients []string
	for _, entry := range replies {
		e := entry.(map[string]interface{})
		clientID := e["client_id"].(string)
		gotClients = append(gotClients, clientID)
		reply := e["reply"].(map[string]interface{})
		assert.Equal(t, `{"from":"`+clientID+`"}`, reply["payload"],
			"each entry must carry the reply of the client it is labeled with")
	}
	assert.Equal(t, []string{"client-a", "client-b", "client-c"}, gotClients)
}

// TestTargetedLookupStillReturnsOneReply pins the common case: a command aimed at a single
// client keeps the singular convenience fields, so the shared response shape did not become
// harder to read for the endpoints that can only ever have one answer.
func TestTargetedLookupStillReturnsOneReply(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
		Return(telemetryRespWithReply("client-1", "cmd-1", true, `{"telemetry_enabled":true}`), nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/?client_id=client-1", nil)
	c.Params = gin.Params{{Key: "commandId", Value: "cmd-1"}}

	getTelemetryCommandReply(proxy)(c)
	require.Equal(t, http.StatusOK, w.Code)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	assert.Equal(t, replyStatusDone, body["status"])
	assert.EqualValues(t, 1, body["responded"])
	assert.Equal(t, "client-1", body["client_id"])
	require.Contains(t, body, "reply")
	assert.Equal(t, `{"telemetry_enabled":true}`, body["reply"].(map[string]interface{})["payload"])
}

// TestWaitShorterThanPollInterval covers a budget below the poll interval. Deriving the
// deadline from ticks alone would block for a whole interval no matter how small the
// caller's budget was.
func TestWaitShorterThanPollInterval(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
		Return(telemetryRespNoReply("client-1"), nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/?client_id=client-1&wait=50ms", nil)
	c.Params = gin.Params{{Key: "commandId", Value: "cmd-1"}}

	start := time.Now()
	getTelemetryCommandReply(proxy)(c)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Less(t, elapsed, telemetryReplyPollInterval,
		"a sub-interval wait must return on its own budget, not on the next poll tick")
}

// TestUntargetedWaitReturnsOnceEveryoneAnswered covers the stopping condition for a wait
// with no client_id.
//
// Such a wait cannot stop at the first reply -- that would hand one client's answer back as
// the cluster's -- but it also must not block for the whole budget once there is nothing
// left to wait for. It used to do exactly that: ?wait=90s on a command every client had
// already answered still hung for 90 seconds, which makes the endpoint painful for the CLI
// and agent use it was added for.
func TestUntargetedWaitReturnsOnceEveryoneAnswered(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	// Every known client has already answered.
	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).
		Return(telemetryRespManyReplies("cmd-broadcast", "client-a", "client-b"), nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/?wait=30s", nil)
	c.Params = gin.Params{{Key: "commandId", Value: "cmd-broadcast"}}

	start := time.Now()
	getTelemetryCommandReply(proxy)(c)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Less(t, elapsed, telemetryReplyPollInterval,
		"must not keep polling once every observed client has answered")

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, replyStatusDone, body["status"])
	assert.EqualValues(t, 2, body["responded"])
	assert.EqualValues(t, 2, body["observed_clients"])
}

// TestUntargetedWaitKeepsWaitingWhileSomeoneIsSilent is the other half: a partial answer
// must not end the wait, or the early exit above would just be "return the first reply".
func TestUntargetedWaitKeepsWaitingWhileSomeoneIsSilent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mixCoord := mocks.NewMockMixCoordClient(t)
	proxy := &Proxy{mixCoord: mixCoord}
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	// Two clients known, only one has answered -- and that never changes.
	resp := telemetryRespManyReplies("cmd-broadcast", "client-a")
	resp.Clients = append(resp.Clients, telemetryRespNoReply("client-b").Clients...)
	mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(resp, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/?wait=600ms", nil)
	c.Params = gin.Params{{Key: "commandId", Value: "cmd-broadcast"}}

	start := time.Now()
	getTelemetryCommandReply(proxy)(c)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, w.Code)
	assert.GreaterOrEqual(t, elapsed, 600*time.Millisecond,
		"a silent client must keep the wait open for the whole budget")

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.EqualValues(t, 1, body["responded"])
	assert.EqualValues(t, 2, body["observed_clients"])
}
