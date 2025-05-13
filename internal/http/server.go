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
	"embed"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/eventlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
}

func registerDefaults() {
	Register(&Handler{
		Path: LogLevelRouterPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			log.Level().ServeHTTP(w, req)
		},
	})
	Register(&Handler{
		Path:    HealthzRouterPath,
		Handler: healthz.Handler(),
	})
	Register(&Handler{
		Path:    EventLogRouterPath,
		Handler: eventlog.Handler(),
	})
	Register(&Handler{
		Path: ExprPath,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			code := req.URL.Query().Get("code")
			auth := req.URL.Query().Get("auth")
			output, err := expr.Exec(code, auth)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf(`{"msg": "failed to execute expression, %s"}`, err.Error())))
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

	RegisterWebUIHandler()
}

func RegisterStopComponent(triggerComponentStop func(role string) error) {
	// register restful api to trigger stop
	Register(&Handler{
		Path: RouteTriggerStopPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			role := req.URL.Query().Get("role")
			log.Info("start to trigger component stop", zap.String("role", role))
			if err := triggerComponentStop(role); err != nil {
				log.Warn("failed to trigger component stop", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf(`{"msg": "failed to trigger component stop, %s"}`, err.Error())))
				return
			}
			log.Info("finish to trigger component stop", zap.String("role", role))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
	})
}

func RegisterCheckComponentReady(checkActive func(role string) error) {
	// register restful api to check component ready
	Register(&Handler{
		Path: RouteCheckComponentReady,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			role := req.URL.Query().Get("role")
			log.Info("start to check component ready", zap.String("role", role))
			if err := checkActive(role); err != nil {
				log.Warn("failed to check component ready", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf(`{"msg": "failed to to check component ready, %s"}`, err.Error())))
				return
			}
			log.Info("finish to check component ready", zap.String("role", role))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"msg": "OK"}`))
		},
	})
}

func RegisterWebUIHandler() {
	httpFS := http.FS(staticFiles)
	fileServer := http.FileServer(httpFS)
	serveIndex := serveFile(RouteWebUI+"index.html", httpFS)
	Register(&Handler{
		Path:    RouteWebUI,
		Handler: handleNotFound(fileServer, serveIndex),
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
		if paramtable.Get().HTTPCfg.EnablePprof.GetAsBool() {
			metricsServer = http.DefaultServeMux
		} else {
			metricsServer = http.NewServeMux()
		}
	}
	if h.HandlerFunc != nil {
		metricsServer.HandleFunc(h.Path, h.HandlerFunc)
		return
	}
	if h.Handler != nil {
		metricsServer.Handle(h.Path, h.Handler)
	}
}

func ServeHTTP() {
	registerDefaults()
	go func() {
		bindAddr := getHTTPAddr()
		log.Info("management listen", zap.String("addr", bindAddr))
		server = &http.Server{Handler: metricsServer, Addr: bindAddr, ReadTimeout: 10 * time.Second}

		if runtime.GOARCH != "arm64" {
			// enable mutex && block profile, sampling rate 10%
			runtime.SetMutexProfileFraction(10)
			runtime.SetBlockProfileRate(10)
		}

		if err := server.ListenAndServe(); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
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
