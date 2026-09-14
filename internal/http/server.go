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
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	DefaultListenPort = "9091"
	ListenPortEnvKey  = "METRICS_PORT"
)

var (
	metricsServer *http.ServeMux
	server        *http.Server
)

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
	// AdminAuth gates this handler behind HTTP Basic Auth for the milvus root
	// user whenever common.security.adminAuthEnabled is on. Leaving it false
	// keeps the handler unauthenticated, which is what /healthz, /livez and
	// /metrics need. Register refuses to publish an ungated operator endpoint;
	// see mustBeGated.
	AdminAuth bool

	// BrowserDocument marks a page a human opens. A
	// 401 then carries WWW-Authenticate so the browser can collect credentials,
	// and a top-level cross-site navigation is let through, because showing a
	// page is not an action. Leave it off on JSON APIs and on anything with
	// side effects, which it would open to cross-site link clicks.
	BrowserDocument bool
}

// openOperatorPaths are the only operator-surface paths allowed to stay
// anonymous: Kubernetes probes cannot present credentials, and gating
// readiness would take down every rolling update in the fleet.
var openOperatorPaths = map[string]struct{}{
	RouteCheckComponentReady: {},
}

// gatedPathPrefixes must match what common.security.adminAuthEnabled documents
// as requiring root, for everything served through Register. It cannot see
// /api/v1, which the proxy serves from its own gin tree behind
// metricsPortAuthMiddleware; the probe and scrape surface stays open by design.
var gatedPathPrefixes = []string{
	"/management/",
	"/debug/pprof/",
	LogLevelRouterPath,
	EventLogRouterPath,
	RouteWebUI,
	TelemetryUIPath,
}

// serveMuxPatternPath extracts the path from the patterns accepted by Go's
// ServeMux: [METHOD ][HOST]/[PATH]. The registration guard must classify the
// same path the mux will serve; checking the raw pattern would let a method or
// host qualifier hide an operator route from the gate.
func serveMuxPatternPath(pattern string) string {
	if fields := strings.Fields(pattern); len(fields) > 1 {
		pattern = fields[len(fields)-1]
	}
	if strings.HasPrefix(pattern, "/") {
		return pattern
	}
	if slash := strings.IndexByte(pattern, '/'); slash >= 0 {
		return pattern[slash:]
	}
	return pattern
}

// mustBeGated reports whether pattern is one that Register refuses to publish
// with AdminAuth unset. The zero value is "open", right for /healthz and wrong
// for the operator surface, where the next route added would otherwise ship
// anonymous with nothing to say so.
//
// It covers what goes through Register only; the proxy's /api/v1 tree is
// classified by its /_ prefix and covered by a route-enumerating test.
func mustBeGated(pattern string) bool {
	path := serveMuxPatternPath(pattern)
	if _, ok := openOperatorPaths[path]; ok {
		return false
	}
	for _, prefix := range gatedPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
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
		// zap's AtomicLevel handler serves both GET and PUT, and the whole
		// endpoint is gated rather than just the PUT: raising the level to
		// debug can surface request payloads and config values in the log
		// stream. /healthz and /livez stay open for liveness.
		AdminAuth: true,
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
		// carries internal operational detail. Discovery is gated with the
		// rest of the management plane, and its unauthenticated gRPC stream
		// binds to loopback whenever the gate is on -- gating only the
		// discovery call would leave the actual data channel open.
		AdminAuth: true,
	})

	if paramtable.Get().HTTPCfg.EnableWebUI.GetAsBool() {
		RegisterWebUIHandler()
	}

	if paramtable.Get().HTTPCfg.EnablePprof.GetAsBool() {
		registerPprof()
	}
}

// registerPprof attaches the standard net/http/pprof handlers explicitly. They
// used to arrive via pkg/metrics's blank import of net/http/pprof plus
// Register() serving http.DefaultServeMux. Milvus-owned handlers now live on a
// private mux, and gated mode never serves the default one, so the only way
// pprof reaches a protected port is an explicit call with a visible auth
// posture. Heap dumps can reveal cached credentials, so these are gated.
func registerPprof() {
	// /debug/pprof/ is the index page; the standard pprof.Index handler also
	// dispatches /debug/pprof/heap, /goroutine, /allocs, /threadcreate,
	// /block, /mutex via path inspection — so we only need to register the
	// prefix entry plus the four endpoints that have dedicated handlers.
	Register(&Handler{
		Path:        "GET /debug/pprof/",
		HandlerFunc: netpprof.Index,
		AdminAuth:   true,
	})
	Register(&Handler{
		Path:        "GET /debug/pprof/cmdline",
		HandlerFunc: netpprof.Cmdline,
		AdminAuth:   true,
	})
	Register(&Handler{
		Path:        "GET /debug/pprof/profile",
		HandlerFunc: netpprof.Profile,
		AdminAuth:   true,
	})
	Register(&Handler{
		Path:        "GET /debug/pprof/symbol",
		HandlerFunc: netpprof.Symbol,
		AdminAuth:   true,
	})
	Register(&Handler{
		Path:        "GET /debug/pprof/trace",
		HandlerFunc: netpprof.Trace,
		AdminAuth:   true,
	})
}

func RegisterStopComponent(triggerComponentStop func(role string) error) {
	// register restful api to trigger stop
	Register(&Handler{
		Path: RouteTriggerStopPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			role := req.URL.Query().Get("role")
			mlog.Info(ctx, "start to trigger component stop", mlog.String("role", truncateForLog(role)))
			if err := triggerComponentStop(role); err != nil {
				mlog.Warn(ctx, "failed to trigger component stop", mlog.Err(err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"msg": "failed to trigger component stop, %s"}`, err.Error())
				return
			}
			mlog.Info(ctx, "finish to trigger component stop", mlog.String("role", truncateForLog(role)))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
		// /management/stop can DoS a running component. /management/check/ready
		// below stays open because k8s probes cannot present credentials.
		AdminAuth: true,
	})
}

func RegisterCheckComponentReady(checkActive func(role string) error) {
	// register restful api to check component ready
	Register(&Handler{
		Path: RouteCheckComponentReady,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			role := req.URL.Query().Get("role")
			// Rated and truncated: this is the one gated-prefix path that stays
			// anonymous by design, so role is caller-controlled and the handler
			// runs on every Kubernetes probe.
			mlog.RatedDebug(ctx, 1.0, "start to check component ready",
				mlog.String("role", truncateForLog(role)))
			if err := checkActive(role); err != nil {
				mlog.RatedWarn(ctx, 1.0, "failed to check component ready", mlog.Err(err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"msg": "failed to to check component ready, %s"}`, err.Error())
				return
			}
			mlog.RatedDebug(ctx, 1.0, "finish to check component ready",
				mlog.String("role", truncateForLog(role)))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
	})
}

// RegisterWebUIHandler serves the web console's static assets.
//
// The bundle carries no cluster data of its own; it is gated with
// BrowserDocument because it is the only place a browser can be told to ask for
// a password. Both halves of the console need it: browsers scope cached
// credentials by protection space, so gating only the console shell would leave
// its XHRs against /api/v1/_* taking silent 401s.
func RegisterWebUIHandler() {
	httpFS := http.FS(staticFiles)
	fileServer := http.FileServer(httpFS)
	serveIndex := serveFile(RouteWebUI+"index.html", httpFS)
	Register(&Handler{
		Path:            RouteWebUI,
		Handler:         handleNotFound(fileServer, serveIndex),
		AdminAuth:       true,
		BrowserDocument: true,
	})

	// Telemetry UI handler
	serveTelemetry := serveFile("webui/telemetry.html", httpFS)
	Register(&Handler{
		Path:            TelemetryUIPath,
		Handler:         serveTelemetry,
		AdminAuth:       true,
		BrowserDocument: true,
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
		// Always register Milvus-owned routes on a dedicated mux. Whether the
		// legacy DefaultServeMux remains visible while the gate is off is decided
		// by managementHTTPHandler; keeping registration private is what lets the
		// gate hide those legacy routes immediately when it is enabled.
		metricsServer = http.NewServeMux()
	}

	handler := h.Handler
	if h.HandlerFunc != nil {
		handler = h.HandlerFunc
	}
	if handler == nil {
		return
	}
	if !h.AdminAuth && mustBeGated(h.Path) {
		panic(fmt.Sprintf(
			"http.Register: %q is an operator endpoint and must set AdminAuth "+
				"(or be added to openOperatorPaths if it has to answer "+
				"Kubernetes probes)", h.Path))
	}
	if h.AdminAuth {
		handler = wrapAdminAuth(handler, h.Path, h.BrowserDocument)
	}
	metricsServer.Handle(h.Path, handler)
}

// managementHTTPHandler preserves the old DefaultServeMux behavior only in
// the configuration where Milvus used it before this gate existed: pprof is
// enabled and adminAuthEnabled is off. In that mode a third-party or expvar
// route registered on the default mux must retain ServeMux precedence, including
// beneath private subtree patterns. Once the gate is on, only Milvus-owned routes
// are reachable, so an init-time http.Handle cannot bypass authentication.
func managementHTTPHandler(legacyDefaultMux bool) http.Handler {
	privateMux := metricsServer
	if !legacyDefaultMux {
		return privateMux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if AdminAuthEnabled() {
			privateMux.ServeHTTP(w, req)
			return
		}
		legacyMux := http.DefaultServeMux
		_, privatePattern := privateMux.Handler(req)
		_, legacyPattern := legacyMux.Handler(req)
		switch {
		case legacyPattern == "" || legacyPattern == privatePattern:
			privateMux.ServeHTTP(w, req)
		case privatePattern == "" || privatePattern == RootPath:
			legacyMux.ServeHTTP(w, req)
		case legacyPattern == RootPath:
			privateMux.ServeHTTP(w, req)
		default:
			// Only overlapping extension routes need a selector. Built-in pprof
			// patterns are identical on both muxes and take the first branch.
			// Let ServeMux compare hosts, methods, wildcards and redirects; then
			// delegate through the chosen mux's ServeHTTP to populate PathValue.
			managementRouteSelector(privateMux, legacyMux, privatePattern, legacyPattern).ServeHTTP(w, req)
		}
	})
}

func managementRouteSelector(privateMux, legacyMux *http.ServeMux, privatePattern, legacyPattern string) (selected *http.ServeMux) {
	selected = privateMux
	defer func() {
		if recover() != nil {
			// Separate muxes can contain ambiguous patterns that the old shared
			// mux would have rejected at registration. Keep the private route
			// authoritative instead of panicking or exposing a legacy handler.
			selected = privateMux
		}
	}()
	selector := http.NewServeMux()
	selector.Handle(privatePattern, privateMux)
	selector.Handle(legacyPattern, legacyMux)
	return selector
}

func ServeHTTP() {
	registerDefaults()
	// Say which posture this process is in. A mistyped key parses as false, so
	// without this an operator who believes they enabled the gate has no way to
	// discover otherwise short of probing the port.
	mlog.Info(context.TODO(), "management plane authentication",
		mlog.Bool("enabled", AdminAuthEnabled()),
		mlog.String("key", paramtable.Get().CommonCfg.AdminAuthEnabled.Key))
	// The handle is deliberately dropped: this watch lives as long as the
	// process, and ServeHTTP is called once per process.
	_ = configureEventlogListenerMode(eventlogListenerModeWatcher, eventlog.EnsureListenerMode)
	go func() {
		bindAddr := getHTTPAddr()
		mlog.Info(context.TODO(), "management listen", mlog.String("addr", bindAddr))
		server = &http.Server{
			Handler:     managementHTTPHandler(paramtable.Get().HTTPCfg.EnablePprof.GetAsBool()),
			Addr:        bindAddr,
			ReadTimeout: 10 * time.Second,
		}

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

// configureEventlogListenerMode keeps the eventlog gRPC listener's bind address
// in step with the gate: without it, enabling the gate on a process that already
// answered an /eventlog discovery request leaves that wildcard listener
// reachable until restart.
//
// It watches the config dispatcher rather than ParamItem.RegisterCallback, which
// only forwards UpdateType and would miss the first write of a key not yet in
// etcd -- the common case for a flag being turned on.
// eventlogListenerModeWatcher is the dispatcher identifier ServeHTTP installs
// under. Unregister removes by identifier, so a caller that wants its watch
// back must pass its own.
const eventlogListenerModeWatcher = "eventlog.listener.mode"

func configureEventlogListenerMode(identifier string, ensureMode func(bool) error) config.EventHandler {
	adminAuth := &paramtable.Get().CommonCfg.AdminAuthEnabled
	var mu sync.Mutex
	applied := false
	current := false
	apply := func() {
		// Read the flag and apply it under one lock. Reading first and then
		// waiting for the lock would let a goroutine that observed "false"
		// overwrite a later goroutine's "true" — a lost update that leaves the
		// wildcard listener running while the gate is on.
		mu.Lock()
		defer mu.Unlock()
		// A source-switch event can arrive before the dispatcher's typed-cache
		// eviction. Read the effective value directly so a cached value from
		// the previous source cannot leave the old listener running.
		localOnly, _ := strconv.ParseBool(adminAuth.GetValue())
		if err := ensureMode(localOnly); err != nil {
			mlog.Warn(context.TODO(), "configure eventlog listener mode failed",
				mlog.Bool("localOnly", localOnly), mlog.Err(err))
			return
		}
		// Say it out loud on a change: this stops existing streams and moves
		// the listener, so an operator whose remote eventlog consumer went
		// quiet has something to find.
		if !applied || current != localOnly {
			mlog.Info(context.TODO(), "eventlog listener mode applied",
				mlog.Bool("localOnly", localOnly),
				mlog.String("key", adminAuth.Key))
		}
		applied, current = true, localOnly
	}
	handler := config.NewHandler(identifier, func(*config.Event) {
		// Off the dispatcher goroutine: EventDispatcher.Dispatch calls handlers
		// while holding its read lock, and switching modes stops a gRPC server
		// and binds a socket. Blocking there would stall delivery of every
		// other config event in the process.
		go apply()
	})
	paramtable.Get().Watch(adminAuth.Key, handler)
	apply()
	return handler
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
