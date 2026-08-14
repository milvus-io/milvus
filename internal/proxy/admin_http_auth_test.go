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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	mhttp "github.com/milvus-io/milvus/internal/http"
)

func TestAdminAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("flag disabled preserves legacy access", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AdminAuthEnabled.Key, "false")
		t.Cleanup(func() { Params.Reset(Params.CommonCfg.AdminAuthEnabled.Key) })

		router := gin.New()
		router.GET("/", adminAuthMiddleware(), func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		router.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusNoContent, recorder.Code)
	})

	t.Run("flag enabled requires credentials", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AdminAuthEnabled.Key, "true")
		t.Cleanup(func() { Params.Reset(Params.CommonCfg.AdminAuthEnabled.Key) })

		router := gin.New()
		router.GET("/", adminAuthMiddleware(), func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		router.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "authentication required")
	})

	t.Run("flag enabled rejects non-root users", func(t *testing.T) {
		Params.Save(Params.CommonCfg.AdminAuthEnabled.Key, "true")
		t.Cleanup(func() { Params.Reset(Params.CommonCfg.AdminAuthEnabled.Key) })

		router := gin.New()
		router.GET("/", adminAuthMiddleware(), func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.SetBasicAuth("alice", "password")
		router.ServeHTTP(recorder, request)

		assert.Equal(t, http.StatusForbidden, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "only root")
	})
}

func TestRegisterRestRouterProtectsOperatorRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	Params.Save(Params.CommonCfg.AdminAuthEnabled.Key, "true")
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
	t.Cleanup(func() {
		Params.Reset(Params.CommonCfg.AdminAuthEnabled.Key)
		Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	})

	router := gin.New()
	(&Proxy{}).RegisterRestRouter(router)

	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: mhttp.ClusterConfigsPath},
		{method: http.MethodGet, path: mhttp.HookConfigsPath},
		{method: http.MethodGet, path: mhttp.TelemetryClientsPath},
		{method: http.MethodGet, path: mhttp.TelemetryClientsPath + "/client-1"},
		{method: http.MethodGet, path: mhttp.TelemetryClientsPath + "/client-1/config"},
		{method: http.MethodGet, path: "/_telemetry/clients/client-1/history"},
		{method: http.MethodPost, path: mhttp.TelemetryCommandsPath},
		{method: http.MethodDelete, path: mhttp.TelemetryCommandsPath + "/command-1"},
		{method: http.MethodGet, path: "/_telemetry/commands/command-1/reply"},
	}

	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, nil)
			router.ServeHTTP(recorder, request)

			assert.Equal(t, http.StatusUnauthorized, recorder.Code)
			assert.Contains(t, recorder.Body.String(), "authentication required")
		})
	}
}
