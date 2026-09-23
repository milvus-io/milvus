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

import "net/http"

// managementMux owns the startup policy for this process's metrics port.
// Restricted mode never serves DefaultServeMux or an unlisted registration,
// including the Proxy's catch-all Gin router and future diagnostic handlers.
type managementMux struct {
	mux         *http.ServeMux
	metricsOnly bool
}

func newManagementMux(metricsOnly, enablePprof bool) *managementMux {
	mux := http.NewServeMux()
	if !metricsOnly && enablePprof {
		mux = http.DefaultServeMux
	}
	return &managementMux{mux: mux, metricsOnly: metricsOnly}
}

func metricsOnlyPath(path string) bool {
	switch path {
	case MetricsPath, MetricsDefaultPath, HealthzRouterPath, LivezRouterPath, RouteCheckComponentReady:
		return true
	default:
		return false
	}
}

func (m *managementMux) register(h *Handler) {
	if m.metricsOnly && !metricsOnlyPath(h.Path) {
		return
	}
	handler := h.Handler
	if h.HandlerFunc != nil {
		handler = h.HandlerFunc
	}
	if handler == nil {
		return
	}
	if m.metricsOnly && h.Path != MetricsPath && h.Path != MetricsDefaultPath {
		handler = probeStatusOnly(handler)
	}
	m.mux.Handle(h.Path, handler)
}

func (m *managementMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.metricsOnly && (!metricsOnlyPath(r.URL.EscapedPath()) ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead)) {
		// Check before ServeMux can clean/redirect a path or a handler can
		// act on it. No CORS middleware runs on rejected requests.
		http.NotFound(w, r)
		return
	}
	m.mux.ServeHTTP(w, r)
}

// Probe callers need the status code, not component names or failure details.
// Discard the original headers and body without buffering arbitrary output.
func probeStatusOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := &probeStatus{header: make(http.Header)}
		next.ServeHTTP(result, r)
		status := result.status
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			w.Write([]byte(http.StatusText(status)))
		}
	})
}

type probeStatus struct {
	header http.Header
	status int
}

func (p *probeStatus) Header() http.Header { return p.header }

func (p *probeStatus) WriteHeader(status int) {
	if p.status == 0 && status >= http.StatusOK {
		p.status = status
	}
}

func (p *probeStatus) Write(body []byte) (int, error) {
	if p.status == 0 {
		p.status = http.StatusOK
	}
	return len(body), nil
}
