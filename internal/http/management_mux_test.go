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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func setupManagementMode(t *testing.T, mode string) {
	t.Helper()
	paramtable.Init()
	previous, previousDefault := metricsServer, http.DefaultServeMux
	metricsServer, http.DefaultServeMux = nil, http.NewServeMux()
	params := paramtable.Get()
	keys := []string{params.CommonCfg.ManagementMetricsOnly.Key, params.HTTPCfg.EnablePprof.Key, params.HTTPCfg.EnableWebUI.Key}
	values := []string{params.CommonCfg.ManagementMetricsOnly.GetValue(), params.HTTPCfg.EnablePprof.GetValue(), params.HTTPCfg.EnableWebUI.GetValue()}
	t.Cleanup(func() {
		metricsServer, http.DefaultServeMux = previous, previousDefault
		for i, key := range keys {
			require.NoError(t, params.Save(key, values[i]))
		}
	})
	for i, value := range []string{mode, "true", "true"} {
		require.NoError(t, params.Save(keys[i], value))
	}
}

func TestManagementMetricsOnlyRejectsDiagnosticAndFallbackRoutes(t *testing.T) {
	setupManagementMode(t, "true")
	called := false
	canary := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte("private-diagnostic-canary"))
	})
	// Simulate implicit pprof and host-qualified dependency registrations.
	http.DefaultServeMux.Handle("/debug/pprof/", canary)
	http.DefaultServeMux.Handle("admin.example/metrics", canary)
	http.DefaultServeMux.Handle("/debug/vars", canary)
	registerDefaults()
	RegisterStopComponent(func(string) error { called = true; return nil })
	for _, path := range []string{RootPath, ConfigGetPath, ConfigAlterPath, "/future-diagnostic", "admin.example/metrics"} {
		Register(&Handler{Path: path, Handler: canary})
	}
	for _, path := range []string{
		"/management/config/get?keys=etcd.endpoints", "/management/config/alter",
		"/management/stop?role=querynode", "/management/streaming/nodes",
		"/log/level", "/eventlog", "/webui", "/webui/", "/webui/index.html",
		"/api/v1/_cluster/configs", "/api/v1/_hook/configs", "/api/v1/_qn/segments",
		"/api/v1/metrics", "/api/v1/health", "/api/v1/collection",
		"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/profile", "/debug/vars",
		"/future-diagnostic", "/metrics/", "/metrics/../eventlog", "//metrics",
		"/%6detrics", "/management%2fcheck%2fready", "/healthz/../webui/",
	} {
		t.Run(path, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, path, nil)
			r.Header.Set("Origin", "https://untrusted.example")
			r.Header.Set("Accept", "text/html")
			metricsServer.ServeHTTP(w, r)
			assert.Equal(t, http.StatusNotFound, w.Code)
			assert.Equal(t, "404 page not found\n", w.Body.String())
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
			assert.Empty(t, w.Header().Get("Location"))
		})
	}
	assert.False(t, called, "no diagnostic, action or fallback handler may execute")

	// A same-path host-qualified DefaultServeMux entry cannot intercept scrapes.
	Register(&Handler{Path: MetricsPath, HandlerFunc: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }})
	w := httptest.NewRecorder()
	metricsServer.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://admin.example/metrics", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.False(t, called)
}

func TestManagementMetricsOnlyScrapesAndProbes(t *testing.T) {
	setupManagementMode(t, "true")
	registerDefaults()
	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "management_mode_test_value", Help: "Test scrape."})
	registry.MustRegister(gauge)
	gauge.Set(7)
	for _, path := range []string{MetricsPath, MetricsDefaultPath} {
		Register(&Handler{Path: path, Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{})})
	}
	RegisterCheckComponentReady(func(role string) error {
		if role != "healthy" {
			return errors.New("private-component-failure-canary")
		}
		return nil
	})
	ts := httptest.NewServer(metricsServer)
	defer ts.Close()
	for _, path := range []string{MetricsPath, MetricsDefaultPath} {
		r, err := ts.Client().Get(ts.URL + path)
		require.NoError(t, err)
		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, r.StatusCode)
		assert.Contains(t, string(body), "management_mode_test_value 7")
		assert.Empty(t, r.Header.Get("Access-Control-Allow-Origin"))
	}
	for _, tc := range []struct {
		path   string
		status int
	}{
		{LivezRouterPath, http.StatusOK},
		{RouteCheckComponentReady + "?role=healthy", http.StatusOK},
		{RouteCheckComponentReady + "?role=unhealthy", http.StatusInternalServerError},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			req, err := http.NewRequest(method, ts.URL+tc.path, nil)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			r, err := ts.Client().Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(r.Body)
			r.Body.Close()
			require.NoError(t, err)
			assert.Equal(t, tc.status, r.StatusCode)
			if method == http.MethodHead {
				assert.Empty(t, body)
			} else {
				assert.Equal(t, http.StatusText(tc.status), string(body))
			}
		}
	}
}

func TestManagementMetricsOnlyProbeProjectionAndMethods(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		m := newManagementMux(true, true)
		calls := 0
		for _, path := range []string{HealthzRouterPath, LivezRouterPath, RouteCheckComponentReady} {
			m.register(&Handler{Path: path, HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("X-Component", "private-component-canary")
				w.WriteHeader(status)
				w.Write([]byte(`{"detail":"private-component-canary"}`))
			}})
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, path, nil)
			r.Header.Set("Content-Type", "application/json")
			m.ServeHTTP(w, r)
			assert.Equal(t, status, w.Code)
			assert.Equal(t, http.StatusText(status), w.Body.String())
			assert.Empty(t, w.Header().Get("X-Component"))
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
		}
		for _, path := range []string{MetricsPath, MetricsDefaultPath, HealthzRouterPath, LivezRouterPath, RouteCheckComponentReady} {
			for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodConnect} {
				w := httptest.NewRecorder()
				m.ServeHTTP(w, httptest.NewRequest(method, path, nil))
				assert.Equal(t, http.StatusNotFound, w.Code)
			}
		}
		assert.Equal(t, 3, calls)
	}
}

func TestManagementMetricsOnlyStartupPolicy(t *testing.T) {
	for _, mode := range []string{"false", "true"} {
		t.Run(mode, func(t *testing.T) {
			setupManagementMode(t, mode)
			registerDefaults()
			// Later config writes cannot change either dispatch or registration.
			nextMode, want := "true", http.StatusNoContent
			if mode == "true" {
				nextMode, want = "false", http.StatusNotFound
			}
			require.NoError(t, paramtable.Get().Save(paramtable.Get().CommonCfg.ManagementMetricsOnly.Key, nextMode))
			Register(&Handler{Path: "/later-diagnostic", HandlerFunc: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }})
			w := httptest.NewRecorder()
			metricsServer.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/later-diagnostic", nil))
			assert.Equal(t, want, w.Code)
		})
	}
	t.Run("invalid configuration cannot silently disable the mode", func(t *testing.T) {
		setupManagementMode(t, "treu")
		assert.Panics(t, registerDefaults)
	})
}
