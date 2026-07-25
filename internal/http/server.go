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
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	netpprof "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/expr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	DefaultListenPort = "9091"
	ListenPortEnvKey  = "METRICS_PORT"
)

var (
	metricsServer *http.ServeMux
	server        *http.Server

	// passwordVerifyFunc is a callback function to verify user password.
	// This is set by the proxy package to avoid circular dependency.
	passwordVerifyFunc func(ctx context.Context, username, password string) bool

	// fallbackPasswordVerifyFunc backs credential checks on nodes that do not
	// host credential metadata themselves (querynode, datanode, streamingnode).
	// It is consulted only when passwordVerifyFunc is unset.
	//
	// Two slots rather than one because standalone runs every role in a single
	// process: proxy, mix coord and the worker nodes would all register into a
	// single global and the winner would depend on goroutine scheduling. Keeping
	// the in-process verifiers (proxy, mix coord) in the primary slot and the
	// RPC-backed worker verifier in the fallback slot makes the outcome
	// deterministic and always prefers the cheaper local check.
	fallbackPasswordVerifyFunc func(ctx context.Context, username, password string) bool

	passwordVerifyMu sync.RWMutex
)

// RegisterPasswordVerifyFunc registers a function to verify user password.
// This should be called by the proxy package during initialization.
func RegisterPasswordVerifyFunc(fn func(ctx context.Context, username, password string) bool) {
	passwordVerifyMu.Lock()
	defer passwordVerifyMu.Unlock()
	passwordVerifyFunc = fn
}

// RegisterFallbackPasswordVerifyFunc registers a credential verifier used only
// when no primary verifier is available on this node. Worker nodes call this so
// that the management plane and pprof remain reachable to root once
// common.security.adminAuthEnabled is turned on; without it those endpoints
// would fail closed with 503 on every worker.
func RegisterFallbackPasswordVerifyFunc(fn func(ctx context.Context, username, password string) bool) {
	passwordVerifyMu.Lock()
	defer passwordVerifyMu.Unlock()
	fallbackPasswordVerifyFunc = fn
}

// getPasswordVerifyFunc returns the effective credential verifier, preferring
// the primary (in-process) one. Returns nil when this node cannot verify
// credentials at all, which callers must surface as 503 rather than 401.
func getPasswordVerifyFunc() func(ctx context.Context, username, password string) bool {
	passwordVerifyMu.RLock()
	defer passwordVerifyMu.RUnlock()
	if passwordVerifyFunc != nil {
		return passwordVerifyFunc
	}
	return fallbackPasswordVerifyFunc
}

// Embedding all static files of webui folder to binary
//
//go:embed webui
var staticFiles embed.FS

// Provide alias for native http package
// avoiding import alias when using http package

type (
	ResponseWriter = http.ResponseWriter
	Request        = http.Request
)

type Handler struct {
	Path        string
	HandlerFunc http.HandlerFunc
	Handler     http.Handler
	// AuthPolicy, when set, gates this handler behind an HTTP Basic Auth check
	// for the milvus root user. A nil AuthPolicy (the default) leaves the
	// handler unauthenticated — appropriate for /healthz, /metrics, k8s probes,
	// and other endpoints that must remain reachable without credentials.
	//
	// See AuthAlways (e.g. /expr) and AuthByAdminFlag (e.g. /management/*)
	// in auth.go for the predefined policies.
	AuthPolicy AuthPolicy
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"msg": msg})
}

func registerDefaults() {
	Register(&Handler{
		Path: LogLevelRouterPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			level := mlog.GetAtomicLevel()
			level.ServeHTTP(w, req)
		},
		// zap's AtomicLevel handler serves both GET (read the level) and PUT
		// (change it). The whole endpoint is gated rather than just the PUT:
		// raising the level to debug is a mutation of production logging that
		// can surface request payloads and config values in the log stream, and
		// splitting the gate by method would leave the read side advertising
		// the current posture. /healthz and /livez below remain the
		// unauthenticated way to ask whether a node is alive.
		AuthPolicy: AuthByAdminFlag,
	})
	Register(&Handler{
		Path:    HealthzRouterPath,
		Handler: healthz.Handler(),
	})
	Register(&Handler{
		Path:    LivezRouterPath,
		Handler: healthz.LivenessHandler(),
	})
	Register(&Handler{
		Path:    EventLogRouterPath,
		Handler: eventlog.Handler(),
		// /eventlog attaches a listener to the process event stream, which
		// carries internal operational detail; gate it with the rest of the
		// management plane.
		AuthPolicy: AuthByAdminFlag,
	})
	Register(&Handler{
		Path: ExprPath,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Check if expr endpoint is enabled
			if !paramtable.Get().CommonCfg.ExprEnabled.GetAsBool() {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"msg": "expr endpoint is disabled. Set common.security.exprEnabled to true to enable it."}`))
				return
			}

			code := req.URL.Query().Get("code")
			var auth string

			// Only Proxy nodes can access /expr endpoint
			if !expr.HasRegistered("proxy") || getPasswordVerifyFunc() == nil {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"msg": "/expr endpoint is only available on Proxy nodes"}`))
				return
			}

			if err := CheckExprAuth(req.Context(), req); err != nil {
				writeJSONError(w, HTTPStatusFromPrivilegeError(err), err.Error())
				return
			}
			// Use bypass since we've already authenticated
			auth = expr.AuthBypass

			output, err := expr.Exec(code, auth)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError,
					fmt.Sprintf("failed to execute expression, %s", err.Error()))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := make(map[string]string)
			resp["output"] = output
			json.NewEncoder(w).Encode(resp)
		}),
	})
	Register(&Handler{
		Path:    StaticPath,
		Handler: GetStaticHandler(),
	})

	if paramtable.Get().HTTPCfg.EnableWebUI.GetAsBool() {
		RegisterWebUIHandler()
	}

	if paramtable.Get().HTTPCfg.EnablePprof.GetAsBool() {
		registerPprof()
	}
}

// registerPprof attaches the standard net/http/pprof handlers explicitly,
// gated by adminAuthEnabled. Previously they were attached via a blank import
// of net/http/pprof in pkg/metrics, which relied on package-init registration
// to http.DefaultServeMux and Register() opportunistically using that mux.
// That arrangement made the auth posture invisible at registration sites and
// allowed any third-party init-time registration to slip onto port 9091.
//
// Pprof endpoints expose process internals (heap dumps, goroutine stacks,
// CPU profiles) that can reveal cached credentials and query data; they are
// gated by AuthByAdminFlag so deployments with adminAuthEnabled=true require
// root credentials.
func registerPprof() {
	// /debug/pprof/ is the index page; the standard pprof.Index handler also
	// dispatches /debug/pprof/heap, /goroutine, /allocs, /threadcreate,
	// /block, /mutex via path inspection — so we only need to register the
	// prefix entry plus the four endpoints that have dedicated handlers.
	Register(&Handler{
		Path:        "/debug/pprof/",
		HandlerFunc: netpprof.Index,
		AuthPolicy:  AuthByAdminFlag,
	})
	Register(&Handler{
		Path:        "/debug/pprof/cmdline",
		HandlerFunc: netpprof.Cmdline,
		AuthPolicy:  AuthByAdminFlag,
	})
	Register(&Handler{
		Path:        "/debug/pprof/profile",
		HandlerFunc: netpprof.Profile,
		AuthPolicy:  AuthByAdminFlag,
	})
	Register(&Handler{
		Path:        "/debug/pprof/symbol",
		HandlerFunc: netpprof.Symbol,
		AuthPolicy:  AuthByAdminFlag,
	})
	Register(&Handler{
		Path:        "/debug/pprof/trace",
		HandlerFunc: netpprof.Trace,
		AuthPolicy:  AuthByAdminFlag,
	})
}

func RegisterStopComponent(triggerComponentStop func(role string) error) {
	// register restful api to trigger stop
	Register(&Handler{
		Path: RouteTriggerStopPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			role := req.URL.Query().Get("role")
			mlog.Info(ctx, "start to trigger component stop", mlog.String("role", role))
			if err := triggerComponentStop(role); err != nil {
				mlog.Warn(ctx, "failed to trigger component stop", mlog.Err(err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"msg": "failed to trigger component stop, %s"}`, err.Error())
				return
			}
			mlog.Info(ctx, "finish to trigger component stop", mlog.String("role", role))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
		// /management/stop can DoS a running component, so it is gated by
		// adminAuthEnabled. /management/check/ready below stays open because
		// k8s probes cannot present credentials.
		AuthPolicy: AuthByAdminFlag,
	})
}

func RegisterCheckComponentReady(checkActive func(role string) error) {
	// register restful api to check component ready
	Register(&Handler{
		Path: RouteCheckComponentReady,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			role := req.URL.Query().Get("role")
			mlog.Info(ctx, "start to check component ready", mlog.String("role", role))
			if err := checkActive(role); err != nil {
				mlog.Warn(ctx, "failed to check component ready", mlog.Err(err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"msg": "failed to to check component ready, %s"}`, err.Error())
				return
			}
			mlog.Info(ctx, "finish to check component ready", mlog.String("role", role))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
	})
}

// RegisterWebUIHandler serves the web console's static assets.
//
// These are registered without an AuthPolicy on purpose. The bundle is an HTML
// and JS shell that carries no cluster data of its own: every value it displays
// is fetched at runtime from /api/v1/*, which is served by the gin tree and
// already authenticated. Gating the assets would therefore protect nothing that
// is not already protected, while breaking the console for deployments that
// serve it to operators who authenticate at the API layer.
func RegisterWebUIHandler() {
	httpFS := http.FS(staticFiles)
	fileServer := http.FileServer(httpFS)
	serveIndex := serveFile(RouteWebUI+"index.html", httpFS)
	Register(&Handler{
		Path:    RouteWebUI,
		Handler: handleNotFound(fileServer, serveIndex),
	})

	// Telemetry UI handler
	serveTelemetry := serveFile("webui/telemetry.html", httpFS)
	Register(&Handler{
		Path:    TelemetryUIPath,
		Handler: serveTelemetry,
	})
}

type responseInterceptor struct {
	http.ResponseWriter
	is404 bool
}

func (ri *responseInterceptor) WriteHeader(status int) {
	if status == http.StatusNotFound {
		ri.is404 = true
		return
	}
	ri.ResponseWriter.WriteHeader(status)
}

func (ri *responseInterceptor) Write(p []byte) (int, error) {
	if ri.is404 {
		return len(p), nil // Pretend the data was written for a 404
	}
	return ri.ResponseWriter.Write(p)
}

// handleNotFound attempts to serve a fallback handler (on404) if the main handler returns a 404 status.
func handleNotFound(handler, on404 http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ri := &responseInterceptor{ResponseWriter: w}
		handler.ServeHTTP(ri, r)

		if ri.is404 {
			on404.ServeHTTP(w, r)
		}
	})
}

// serveFile serves the specified file content (like "index.html") for HTML requests.
func serveFile(filename string, fs http.FileSystem) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !acceptsHTML(r) {
			http.NotFound(w, r)
			return
		}

		file, err := fs.Open(filename)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer file.Close()

		fi, err := file.Stat()
		if err != nil {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, fi.Name(), fi.ModTime(), file)
	}
}

// acceptsHTML checks if the request header specifies that HTML is acceptable.
func acceptsHTML(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/html")
}

func Register(h *Handler) {
	if metricsServer == nil {
		// Always use a dedicated mux. We no longer fall back to
		// http.DefaultServeMux when pprof is enabled — pprof endpoints are
		// now registered explicitly (see registerPprof) so that third-party
		// init() hooks cannot smuggle extra routes onto the metrics port.
		metricsServer = http.NewServeMux()
	}

	handler := h.Handler
	if h.HandlerFunc != nil {
		handler = h.HandlerFunc
	}
	if handler == nil {
		return
	}
	if h.AuthPolicy != nil {
		handler = wrapAdminAuth(handler, h.AuthPolicy)
	}
	metricsServer.Handle(h.Path, handler)
}

func ServeHTTP() {
	registerDefaults()
	go func() {
		bindAddr := getHTTPAddr()
		mlog.Info(context.TODO(), "management listen", mlog.String("addr", bindAddr))
		server = &http.Server{Handler: metricsServer, Addr: bindAddr, ReadTimeout: 10 * time.Second}

		if runtime.GOARCH != "arm64" {
			// enable mutex && block profile, sampling rate 10%
			runtime.SetMutexProfileFraction(10)
			runtime.SetBlockProfileRate(10)
		}

		if err := server.ListenAndServe(); err != nil {
			mlog.Error(context.TODO(), "handle metrics failed", mlog.Err(err))
		}
	}()
}

func getHTTPAddr() string {
	port := os.Getenv(ListenPortEnvKey)
	_, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Sprintf(":%s", DefaultListenPort)
	}
	paramtable.Get().Save(paramtable.Get().CommonCfg.MetricsPort.Key, port)

	return fmt.Sprintf(":%s", port)
}
