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

package eventlog

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	// ContentTypeHeader is the health check request type header.
	ContentTypeHeader = "Content-Type"
	// ContentTypeText is the health check request type text.
	ContentTypeText = "text/plain"
	// ContentTypeJSON is another health check request type text, which response contains more info.
	ContentTypeJSON = "application/json"
)

// eventLogHandler is the event log http handler
type eventLogHandler struct{}

func Handler() http.Handler {
	return &eventLogHandler{}
}

type eventLogResponse struct {
	Status int `json:"status"`
	Port   int `json:"port"`
}

// ServeHTTP handler which start a grpc server listen on random available port.
func (h *eventLogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := &eventLogResponse{
		Status: http.StatusOK,
	}

	port, err := getGrpcLogger()
	if err != nil {
		resp.Status = http.StatusInternalServerError
		writeJSON(w, r, resp)
		return
	}

	resp.Port = port

	writeJSON(w, r, resp)
}

func writeJSON(w http.ResponseWriter, r *http.Request, resp *eventLogResponse) {
	w.Header().Set(ContentTypeHeader, ContentTypeJSON)
	bs, err := json.Marshal(resp)
	if err != nil {
		log.Warn("faild to send response", zap.Error(err))
	}
	w.Write(bs)
}
