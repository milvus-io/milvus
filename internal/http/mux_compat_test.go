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

package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func resetManagementMuxes(t *testing.T) {
	t.Helper()
	paramtable.Init()
	previousPrivate, previousDefault := metricsServer, http.DefaultServeMux
	metricsServer, http.DefaultServeMux = http.NewServeMux(), http.NewServeMux()
	key := paramtable.Get().CommonCfg.AdminAuthEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() {
		metricsServer, http.DefaultServeMux = previousPrivate, previousDefault
		paramtable.Get().Reset(key)
	})
}

func TestManagementHTTPHandlerPreservesPathValues(t *testing.T) {
	resetManagementMuxes(t)
	t.Cleanup(installVerifier(t, "proxy", func(context.Context, string, string) error { return nil }))
	Register(&Handler{
		Path: "GET /management/items/{id}", AdminAuth: true,
		HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			// #nosec G705 -- In-memory test response to fixed synthetic requests.
			fmt.Fprintf(w, "%s:%s", r.Pattern, r.PathValue("id"))
		},
	})
	for _, legacy := range []bool{false, true} {
		for _, gateOn := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%t/admin=%t", legacy, gateOn), func(t *testing.T) {
				require.NoError(t, paramtable.Get().Save(paramtable.Get().CommonCfg.AdminAuthEnabled.Key, strconv.FormatBool(gateOn)))
				req := httptest.NewRequest(http.MethodGet, "/management/items/a%2Fb", nil)
				req.SetBasicAuth("root", "right")
				req.Header.Set(AdminRequestHeader, "true")
				w := httptest.NewRecorder()
				managementHTTPHandler(legacy).ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, "GET /management/items/{id}:a/b", w.Body.String())
			})
		}
	}
}

// Compare the assembled handler with the shared ServeMux used before the gate.
// This also covers method, host, wildcard and redirect precedence rather than
// treating a longer pattern string as a more-specific route.
func TestManagementHTTPHandlerMatchesLegacyRoutePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, private, legacy, method, target string
	}{
		{"nested pprof", "GET /debug/pprof/", "GET /debug/pprof/custom", "GET", "/debug/pprof/custom"},
		{"exact over wildcard", "/management/{id}", "/management/custom", "GET", "/management/custom"},
		{"wildcard over subtree", "/management/", "/management/{id}", "GET", "/management/custom"},
		{"private more specific", "/management/items/{id}", "/management/", "GET", "/management/items/123"},
		{"method", "/management/{id}", "GET /management/{id}", "GET", "/management/123"},
		{"head over get", "GET /management/{id}", "HEAD /management/{id}", "HEAD", "/management/123"},
		{"host", "/management/{id}", "milvus.example/management/{id}", "GET", "http://milvus.example/management/123"},
		{"multi wildcard", "/management/{id...}", "/management/items/{id}", "GET", "/management/items/123"},
		{"escaped literal", "/management/{id}", "/management/a%2Fb", "GET", "/management/a%2Fb"},
		{"subtree root", "/management/", "/management/{$}", "GET", "/management/"},
		{"exact before redirect", "/management/items", "/management/items/", "GET", "/management/items"},
		{"legacy slash redirect", "/management/", "/management/items/", "GET", "/management/items"},
		{"clean path redirect", "/management/", "/management/items/{id}", "GET", "/management//items/123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetManagementMuxes(t)
			private := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				// #nosec G705 -- In-memory test response to fixed synthetic requests.
				fmt.Fprintf(w, "private:%s:%s", r.Pattern, r.PathValue("id"))
			})
			legacy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				// #nosec G705 -- In-memory test response to fixed synthetic requests.
				fmt.Fprintf(w, "legacy:%s:%s", r.Pattern, r.PathValue("id"))
			})
			baseline := http.NewServeMux()
			baseline.Handle(tc.private, private)
			baseline.Handle(tc.legacy, legacy)
			Register(&Handler{Path: tc.private, Handler: private, AdminAuth: true})
			http.DefaultServeMux.Handle(tc.legacy, legacy)
			actual := managementHTTPHandler(true)
			request := func(handler http.Handler) *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.target, nil))
				return w
			}
			want, got := request(baseline), request(actual)
			assert.Equal(t, want.Code, got.Code)
			assert.Equal(t, want.Body.String(), got.Body.String())
			assert.Equal(t, want.Header(), got.Header())

			// Turning the gate on must hide the legacy route even after it has
			// won a request. Compare against the real protected private mux.
			require.NoError(t, paramtable.Get().Save(paramtable.Get().CommonCfg.AdminAuthEnabled.Key, "true"))
			want, got = request(metricsServer), request(actual)
			assert.Equal(t, want.Code, got.Code)
			assert.Equal(t, want.Body.String(), got.Body.String())
			assert.Equal(t, want.Header(), got.Header())
		})
	}
}

func TestRegisterPprofPreservesHTTPMethods(t *testing.T) {
	resetManagementMuxes(t)
	registerPprof()
	for _, gateOn := range []bool{false, true} {
		require.NoError(t, paramtable.Get().Save(paramtable.Get().CommonCfg.AdminAuthEnabled.Key, strconv.FormatBool(gateOn)))
		for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/profile", "/debug/pprof/symbol", "/debug/pprof/trace"} {
			for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
				_, pattern := metricsServer.Handler(httptest.NewRequest(method, path, nil))
				assert.Empty(t, pattern, "%s %s must not dispatch a pprof handler", method, path)
			}
		}
		w := httptest.NewRecorder()
		managementHTTPHandler(true).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/debug/pprof/cmdline", nil))
		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Equal(t, "GET, HEAD", w.Header().Get("Allow"))
	}
}

func TestManagementHTTPHandlerKeepsPrivateConflictingRoutes(t *testing.T) {
	for _, tc := range []struct{ private, legacy string }{
		{"/management/{id}", "/management/{id}"},
		{"/management/{id}", "/management/{other}"},
		{"/management/items/{id}", "/management/{other}/123"},
	} {
		t.Run(tc.legacy, func(t *testing.T) {
			resetManagementMuxes(t)
			Register(&Handler{
				Path: tc.private, AdminAuth: true,
				HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					// #nosec G705 -- In-memory test response to fixed synthetic requests.
					fmt.Fprintf(w, "private:%s", r.PathValue("id"))
				},
			})
			handler := managementHTTPHandler(true)
			// Late DefaultServeMux registrations must also obey the same policy.
			http.DefaultServeMux.HandleFunc(tc.legacy, func(http.ResponseWriter, *http.Request) {
				t.Error("a conflicting legacy route must not displace a private route")
			})
			path := "/management/123"
			if tc.private == "/management/items/{id}" {
				path = "/management/items/123"
			}
			w := httptest.NewRecorder()
			require.NotPanics(t, func() {
				handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			})
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "private:123", w.Body.String())
		})
	}
}
