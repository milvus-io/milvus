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

package management

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	DefaultListenPort = "9091"
	ListenPortEnvKey  = "METRICS_PORT"
)

type HTTPHandler struct {
	Path        string
	HandlerFunc http.HandlerFunc
	Handler     http.Handler
}

func registerDefaults() {
	Register(&HTTPHandler{
		Path: "/log/level",
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			log.Level().ServeHTTP(w, req)
		},
	})
}

func Register(h *HTTPHandler) {
	if h.HandlerFunc != nil {
		http.HandleFunc(h.Path, h.HandlerFunc)
		return
	}
	if h.Handler != nil {
		http.Handle(h.Path, h.Handler)
	}
}

func ServeHTTP() {
	registerDefaults()
	go func() {
		bindAddr := getHTTPAddr()
		log.Info("management listen", zap.String("addr", bindAddr))
		if err := http.ListenAndServe(bindAddr, nil); err != nil {
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

	return fmt.Sprintf(":%s", port)
}
