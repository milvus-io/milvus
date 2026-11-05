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

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// WriteJSONResponse writes a JSON response, returning HTTP 500 if marshaling fails.
func WriteJSONResponse(ctx context.Context, w http.ResponseWriter, status int, payload any) {
	bs, err := json.Marshal(payload)
	if err != nil {
		mlog.Warn(ctx, "failed to marshal JSON response", mlog.Err(err))
		status = http.StatusInternalServerError
		bs, _ = json.Marshal(map[string]string{
			"msg": fmt.Sprintf("failed to marshal response, %s", err.Error()),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(bs); err != nil {
		mlog.Warn(ctx, "failed to write JSON response", mlog.Err(err))
	}
}

// WriteJSONWithMsg writes a JSON response containing a msg field.
func WriteJSONWithMsg(ctx context.Context, w http.ResponseWriter, status int, msg string) {
	WriteJSONResponse(ctx, w, status, map[string]string{"msg": msg})
}
