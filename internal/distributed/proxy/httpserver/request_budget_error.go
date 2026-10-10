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
	"errors"
	"net/http"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WriteRequestBudgetError preserves the REST error envelope when a request is
// rejected before entering Gin. Once a request deadline has elapsed, the
// transport may reject this write; callers must not renew that deadline just
// to force an error response through.
func WriteRequestBudgetError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := merr.Code(err)
	message := err.Error()
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusRequestTimeout
		code = merr.TimeoutCode
		message = "request timeout"
	} else if errors.Is(err, merr.ErrParameterInvalid) {
		status = http.StatusOK
	}
	body, encodeErr := json.Marshal(map[string]any{HTTPReturnCode: code, HTTPReturnMessage: message})
	if encodeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
