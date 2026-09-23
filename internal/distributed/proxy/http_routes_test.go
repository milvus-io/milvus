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

package grpcproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/netutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMetricsPortV1Registration(t *testing.T) {
	params := paramtable.Get()
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.EnableV1.Key)
		params.Reset(params.CommonCfg.AuthorizationEnabled.Key)
	})
	require.NoError(t, params.Save(params.CommonCfg.AuthorizationEnabled.Key, "true"))
	paramSegment := regexp.MustCompile(`/:[^/]+`)
	legacyEngine := gin.New()
	httpserver.NewHandlers(&proxy.Proxy{}).RegisterRoutesTo(legacyEngine.Group(apiPathPrefix))
	legacyRoutes := legacyEngine.Routes()
	require.Len(t, legacyRoutes, 47)
	var consoleRoutes gin.RoutesInfo
	for _, setting := range []string{"default", "true", "false"} {
		t.Run(setting, func(t *testing.T) {
			if setting == "default" {
				require.NoError(t, params.Reset(params.HTTPCfg.EnableV1.Key))
			} else {
				require.NoError(t, params.Save(params.HTTPCfg.EnableV1.Key, setting))
			}
			// Use the production Proxy registrar, including all 2.6 console routes.
			engine := newMetricsPortEngine(gin.New(), &proxy.Proxy{})
			routes := engine.Routes()
			if setting != "false" {
				require.Len(t, routes, 27)
				consoleRoutes = routes
			} else {
				require.Empty(t, routes)
			}
			for _, route := range routes {
				require.True(t, strings.HasPrefix(route.Path, "/api/v1/_"),
					"%s %s must not publish a retired business API", route.Method, route.Path)
			}
			for _, route := range consoleRoutes {
				w := httptest.NewRecorder()
				req := httptest.NewRequest(route.Method, paramSegment.ReplaceAllString(route.Path, "/x"), nil)
				engine.ServeHTTP(w, req)
				if setting == "false" {
					assert.Equal(t, http.StatusNotFound, w.Code, "%s %s", route.Method, route.Path)
				} else {
					assert.Equal(t, http.StatusUnauthorized, w.Code, "%s %s", route.Method, route.Path)
				}
			}
			for _, route := range legacyRoutes {
				w := httptest.NewRecorder()
				engine.ServeHTTP(w, httptest.NewRequest(route.Method, paramSegment.ReplaceAllString(route.Path, "/x"), nil))
				assert.Equal(t, http.StatusNotFound, w.Code, "%s %s", route.Method, route.Path)
			}
		})
	}
}

func TestHTTPV1SwitchKeepsV2Serving(t *testing.T) {
	params := paramtable.Get()
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.EnableV1.Key)
		params.Reset(params.CommonCfg.AuthorizationEnabled.Key)
	})
	require.NoError(t, params.Save(params.CommonCfg.AuthorizationEnabled.Key, "false"))
	for _, setting := range []string{"default", "true", "false"} {
		t.Run(setting, func(t *testing.T) {
			if setting == "default" {
				require.NoError(t, params.Reset(params.HTTPCfg.EnableV1.Key))
			} else {
				require.NoError(t, params.Save(params.HTTPCfg.EnableV1.Key, setting))
			}
			server := getServer(t)
			mockProxy := server.proxy.(*mocks.MockProxy)
			calls := 2
			v1Status := http.StatusOK
			if setting == "false" {
				calls = 1
				v1Status = http.StatusNotFound
			}
			mockProxy.EXPECT().ShowCollections(mock.Anything, mock.Anything).
				Return(&milvuspb.ShowCollectionsResponse{Status: merr.Success(), CollectionNames: []string{"books"}}, nil).Times(calls)
			startProxyHTTPServerForTest(t, server)
			endpoint := "http://" + server.listenerManager.httpListener.Address()
			client := &http.Client{Timeout: 5 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			for _, tc := range []struct {
				method, path, body string
				status             int
			}{
				{http.MethodGet, "/v1/vector/collections", "", v1Status},
				{http.MethodPost, "/v2/vectordb/collections/list", `{}`, http.StatusOK},
				{http.MethodGet, "/api/v1/health", "", http.StatusNotFound},
			} {
				req, err := http.NewRequest(tc.method, endpoint+tc.path, strings.NewReader(tc.body))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, err)
				assert.Equal(t, tc.status, resp.StatusCode, "%s %s: %s", tc.method, tc.path, body)
				if tc.status == http.StatusOK {
					assert.Contains(t, string(body), `"books"`)
				}
			}
			if setting == "false" {
				v1Engine := gin.New()
				httpserver.NewHandlersV1(mockProxy).RegisterRoutesToV1(v1Engine.Group("/v1"))
				require.Len(t, v1Engine.Routes(), 10)
				for _, route := range v1Engine.Routes() {
					req, err := http.NewRequest(route.Method, endpoint+route.Path, nil)
					require.NoError(t, err)
					resp, err := client.Do(req)
					require.NoError(t, err)
					resp.Body.Close()
					assert.Equal(t, http.StatusNotFound, resp.StatusCode, "%s %s", route.Method, route.Path)
				}
			}
		})
	}
}

func startProxyHTTPServerForTest(t *testing.T, server *Server) {
	t.Helper()
	accesslog.InitAccessLogger(paramtable.Get())
	listener, err := netutil.NewListener()
	require.NoError(t, err)
	server.listenerManager = &listenerManager{httpListener: listener}
	errChan := make(chan error, 2)
	server.wg.Add(1)
	go server.startHTTPServer(errChan)
	require.NoError(t, <-errChan)
	t.Cleanup(func() {
		assert.NoError(t, server.httpServer.Close())
		server.wg.Wait()
	})
}

func TestMetricsPortConsoleAuthorization(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.HTTPCfg.EnableV1.Key, "true"))
	hookutil.InitOnceHook()
	hookutil.SetMockAPIHook("alice", nil)
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.EnableV1.Key)
		params.Reset(params.CommonCfg.AuthorizationEnabled.Key)
		hookutil.SetMockAPIHook("", nil)
	})
	for _, authorization := range []bool{false, true} {
		t.Run(strconv.FormatBool(authorization), func(t *testing.T) {
			require.NoError(t, params.Save(params.CommonCfg.AuthorizationEnabled.Key, strconv.FormatBool(authorization)))
			engine := newMetricsPortEngine(gin.New(), &proxy.Proxy{})
			for _, authenticated := range []bool{false, true} {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/_cluster/configs", nil)
				if authenticated {
					req.Header.Set("Authorization", "Bearer test-api-key")
				}
				w := httptest.NewRecorder()
				engine.ServeHTTP(w, req)
				want := http.StatusOK
				if authorization && !authenticated {
					want = http.StatusUnauthorized
				}
				assert.Equal(t, want, w.Code)
			}
		})
	}
}
