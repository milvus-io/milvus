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

package httpserver

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/requestbudget"
	"github.com/milvus-io/milvus/internal/json"
)

var jsonContentType = []string{"application/json; charset=utf-8"}

type jsonRender struct {
	Data any
}

// Render writes data with custom ContentType.
func (r jsonRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	encoder := json.NewEncoder(w)
	return encoder.Encode(r.Data)
}

// WriteContentType writes JSON ContentType.
func (r jsonRender) WriteContentType(w http.ResponseWriter) {
	header := w.Header()
	if val := header["Content-Type"]; len(val) == 0 {
		header["Content-Type"] = jsonContentType
	}
}

// jsonRowsRender is selected only for budgeted row responses. It avoids a
// single non-cancellable Sonic call over the complete result array.
type jsonRowsRender struct {
	Context context.Context
	Data    gin.H
}

func (r jsonRowsRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	return requestbudget.EncodeResponseRows(r.Context, w, map[string]any(r.Data), requestbudget.MaxJSONUnitBytes)
}

func (r jsonRowsRender) WriteContentType(w http.ResponseWriter) {
	jsonRender{}.WriteContentType(w)
}
