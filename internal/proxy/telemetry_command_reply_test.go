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

func TestCommandReplyPayload(t *testing.T) {
	t.Run("pending when there is no reply", func(t *testing.T) {
		body := commandReplyPayload("cmd-1", "client-1", nil, 0)

		assert.Equal(t, replyStatusPending, body["status"])
		assert.Equal(t, "cmd-1", body["command_id"])
		assert.Equal(t, "client-1", body["client_id"])
		assert.NotContains(t, body, "reply")
		assert.Contains(t, body, "message")
	})

	t.Run("done when a reply exists", func(t *testing.T) {
		reply := &CommandReply{CommandID: "cmd-1", Success: true}
		body := commandReplyPayload("cmd-1", "client-1", reply, 250*time.Millisecond)

		assert.Equal(t, replyStatusDone, body["status"])
		assert.Equal(t, reply, body["reply"])
		assert.Equal(t, int64(250), body["waited_ms"])
	})

	t.Run("omits client id when unknown", func(t *testing.T) {
		assert.NotContains(t, commandReplyPayload("cmd-1", "", nil, 0), "client_id")
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
		getTelemetryCommandReply(proxy)(c)

		require.Equal(t, http.StatusOK, w.Code)

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
	reply, _, err := waitForCommandReply(ctx, proxy, "client-1", "cmd-1", 30*time.Second)

	assert.NoError(t, err, "cancellation is not a lookup failure")
	assert.Nil(t, reply)
	assert.Less(t, time.Since(start), 5*time.Second)
}
