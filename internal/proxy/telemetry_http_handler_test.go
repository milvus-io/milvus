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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestTelemetryAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("auth disabled - allows request", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		called := false
		middleware := TelemetryAuthMiddleware()

		// Create a gin engine to properly test middleware chain
		router := gin.New()
		router.Use(middleware)
		router.GET("/", func(c *gin.Context) {
			called = true
			c.Status(http.StatusOK)
		})

		router.ServeHTTP(w, c.Request)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.True(t, called)
	})

	t.Run("auth enabled - missing authorization header", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		middleware := TelemetryAuthMiddleware()

		router := gin.New()
		router.Use(middleware)
		router.GET("/", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		router.ServeHTTP(w, c.Request)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "missing authorization header", resp["error"])
	})

	t.Run("auth enabled - invalid authorization format (not Basic)", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		c.Request.Header.Set("Authorization", "Bearer sometoken")

		middleware := TelemetryAuthMiddleware()

		router := gin.New()
		router.Use(middleware)
		router.GET("/", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		router.ServeHTTP(w, c.Request)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "invalid authorization format, expected Basic auth", resp["error"])
	})

	t.Run("auth enabled - invalid base64 encoding", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		c.Request.Header.Set("Authorization", "Basic not-valid-base64!!!")

		middleware := TelemetryAuthMiddleware()

		router := gin.New()
		router.Use(middleware)
		router.GET("/", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		router.ServeHTTP(w, c.Request)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "invalid credentials encoding", resp["error"])
	})

	t.Run("auth enabled - invalid credentials format (no colon)", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		// Encode "usernamepassword" without colon
		encoded := base64.StdEncoding.EncodeToString([]byte("usernamepassword"))
		c.Request.Header.Set("Authorization", "Basic "+encoded)

		middleware := TelemetryAuthMiddleware()

		router := gin.New()
		router.Use(middleware)
		router.GET("/", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		router.ServeHTTP(w, c.Request)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "invalid credentials format", resp["error"])
	})

	// Note: Testing actual password verification (valid and invalid passwords) requires
	// initializing the privilege cache which is complex to set up. The password verification
	// logic is tested elsewhere in the codebase (util_test.go, authentication_interceptor_test.go).
	// The tests above cover all the middleware's input validation before password verification.
}

func TestGetTelemetryClientsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := getTelemetryClients(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		handler := getTelemetryClients(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("successful response", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(&milvuspb.GetClientTelemetryResponse{
			Status: merr.Success(),
			Clients: []*milvuspb.ClientTelemetry{
				{
					ClientInfo: &commonpb.ClientInfo{
						SdkType:    "Go",
						SdkVersion: "1.0.0",
					},
				},
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?database=test&include_metrics=true", nil)

		handler := getTelemetryClients(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("with query parameters", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.MatchedBy(func(req *milvuspb.GetClientTelemetryRequest) bool {
			return req.Database == "testdb" && req.ClientId == "client-456" && req.IncludeMetrics == true
		})).Return(&milvuspb.GetClientTelemetryResponse{
			Status:  merr.Success(),
			Clients: []*milvuspb.ClientTelemetry{},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/?database=testdb&client_id=client-456&include_metrics=true", nil)

		handler := getTelemetryClients(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		handler := getTelemetryClients(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(&milvuspb.GetClientTelemetryResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "internal error",
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)

		handler := getTelemetryClients(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "internal error", resp["error"])
	})
}

func TestGetTelemetryClientMetricsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := getTelemetryClientMetrics(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientMetrics(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("missing clientId returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/", nil)
		c.Params = gin.Params{{Key: "clientId", Value: ""}}

		handler := getTelemetryClientMetrics(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "client_id parameter is required", resp["error"])
	})

	t.Run("successful response", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.MatchedBy(func(req *milvuspb.GetClientTelemetryRequest) bool {
			return req.ClientId == "client-123" && req.IncludeMetrics == true
		})).Return(&milvuspb.GetClientTelemetryResponse{
			Status: merr.Success(),
			Clients: []*milvuspb.ClientTelemetry{
				{
					ClientInfo: &commonpb.ClientInfo{
						SdkType: "Go",
					},
				},
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientMetrics(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientMetrics(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().GetClientTelemetry(mock.Anything, mock.Anything).Return(&milvuspb.GetClientTelemetryResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "client not found",
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientMetrics(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestPostTelemetryCommandHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := postTelemetryCommand(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", nil)

		handler := postTelemetryCommand(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("invalid JSON body", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString("not valid json"))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "invalid request body", resp["error"])
	})

	t.Run("missing command_type", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		body := `{"target_client_id": "client-123"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "command_type is required", resp["error"])
	})

	t.Run("successful command push", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "show_errors" && req.TargetClientId == "client-123"
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-456",
		}, nil)

		body := `{"command_type": "show_errors", "target_client_id": "client-123", "ttl_seconds": 3600}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "cmd-456", resp["command_id"])
		assert.Equal(t, "created", resp["status"])
	})

	t.Run("command with null payload", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "debug_log" && req.Payload == nil
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-789",
		}, nil)

		body := `{"command_type": "debug_log", "payload": null}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("command with string payload", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "push_config" && string(req.Payload) == "config_value"
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-101",
		}, nil)

		body := `{"command_type": "push_config", "payload": "config_value"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("command with object payload", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "collection_metrics"
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-102",
		}, nil)

		body := `{"command_type": "collection_metrics", "payload": {"key": "value"}}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		body := `{"command_type": "show_errors"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(&milvuspb.PushClientCommandResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "command rejected",
			},
		}, nil)

		body := `{"command_type": "show_errors"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "command rejected", resp["error"])
	})

	t.Run("command with empty payload", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "debug_log" && req.Payload == nil
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-103",
		}, nil)

		body := `{"command_type": "debug_log"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("command with target_database", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "push_config" &&
				req.TargetDatabase == "db_alpha" &&
				req.TargetClientId == "" // target_database is mutually exclusive with target_client_id
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-db-104",
		}, nil)

		body := `{"command_type": "push_config", "target_database": "db_alpha", "payload": {"sampling_rate": 0.8}}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "cmd-db-104", resp["command_id"])
		assert.Equal(t, "created", resp["status"])
	})

	t.Run("command with both target_client_id and target_database", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		// Both fields can be provided - server-side will prioritize target_client_id
		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "push_config" &&
				req.TargetClientId == "client-123" &&
				req.TargetDatabase == "db_beta"
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-both-105",
		}, nil)

		body := `{"command_type": "push_config", "target_client_id": "client-123", "target_database": "db_beta"}`
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(body))

		handler := postTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestDeleteTelemetryCommandHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := deleteTelemetryCommand(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("DELETE", "/cmd-123", nil)
		c.Params = gin.Params{{Key: "commandId", Value: "cmd-123"}}

		handler := deleteTelemetryCommand(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("missing commandId returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("DELETE", "/", nil)
		c.Params = gin.Params{{Key: "commandId", Value: ""}}

		handler := deleteTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "command_id parameter is required", resp["error"])
	})

	t.Run("successful delete", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().DeleteClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.DeleteClientCommandRequest) bool {
			return req.CommandId == "cmd-123"
		})).Return(&milvuspb.DeleteClientCommandResponse{
			Status: merr.Success(),
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("DELETE", "/cmd-123", nil)
		c.Params = gin.Params{{Key: "commandId", Value: "cmd-123"}}

		handler := deleteTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "cmd-123", resp["command_id"])
		assert.Equal(t, "deleted", resp["status"])
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().DeleteClientCommand(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("DELETE", "/cmd-123", nil)
		c.Params = gin.Params{{Key: "commandId", Value: "cmd-123"}}

		handler := deleteTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().DeleteClientCommand(mock.Anything, mock.Anything).Return(&milvuspb.DeleteClientCommandResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "command not found",
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("DELETE", "/cmd-123", nil)
		c.Params = gin.Params{{Key: "commandId", Value: "cmd-123"}}

		handler := deleteTelemetryCommand(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "command not found", resp["error"])
	})
}

func TestGetTelemetryClientConfigHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := getTelemetryClientConfig(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/config", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientConfig(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("missing clientId returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/config", nil)
		c.Params = gin.Params{{Key: "clientId", Value: ""}}

		handler := getTelemetryClientConfig(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "client_id parameter is required", resp["error"])
	})

	t.Run("successful response", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "get_config" &&
				req.TargetClientId == "client-123" &&
				req.TtlSeconds == 0 &&
				req.Persistent == false
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-config-1",
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/config", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientConfig(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "cmd-config-1", resp["command_id"])
		assert.Equal(t, "pending", resp["status"])
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/config", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientConfig(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(&milvuspb.PushClientCommandResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "client not found",
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/config", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientConfig(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "client not found", resp["error"])
	})
}

func TestGetTelemetryClientHistoryHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handler can be created without panic", func(t *testing.T) {
		handler := getTelemetryClientHistory(nil)
		assert.NotNil(t, handler)
	})

	t.Run("nil node returns error", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(nil)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "proxy node not initialized", resp["error"])
	})

	t.Run("missing clientId returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: ""}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "client_id parameter is required", resp["error"])
	})

	t.Run("missing start_time/end_time returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "start_time and end_time query parameters are required (RFC3339 format)", resp["error"])
	})

	t.Run("missing only start_time returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("missing only end_time returns error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?start_time=2024-01-01T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("successful response", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			return req.CommandType == "show_latency_history" &&
				req.TargetClientId == "client-123" &&
				req.Persistent == false
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-history-1",
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "cmd-history-1", resp["command_id"])
		assert.Equal(t, "pending", resp["status"])
	})

	t.Run("successful response with detail=true", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.MatchedBy(func(req *milvuspb.PushClientCommandRequest) bool {
			// Verify payload contains detail: true
			var payload map[string]interface{}
			json.Unmarshal(req.Payload, &payload)
			return req.CommandType == "show_latency_history" &&
				req.TargetClientId == "client-456" &&
				payload["detail"] == true
		})).Return(&milvuspb.PushClientCommandResponse{
			Status:    merr.Success(),
			CommandId: "cmd-history-2",
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-456/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&detail=true", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-456"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("RPC error", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(nil, assert.AnError)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("response status not ok", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		proxy := &Proxy{
			mixCoord: mixCoord,
		}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		mixCoord.EXPECT().PushClientCommand(mock.Anything, mock.Anything).Return(&milvuspb.PushClientCommandResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "client not found",
			},
		}, nil)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request, _ = http.NewRequest("GET", "/client-123/history?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z", nil)
		c.Params = gin.Params{{Key: "clientId", Value: "client-123"}}

		handler := getTelemetryClientHistory(proxy)
		handler(c)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "client not found", resp["error"])
	})
}
