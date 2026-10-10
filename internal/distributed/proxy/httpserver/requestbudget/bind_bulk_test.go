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

package requestbudget

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBindBulkJSONValidatesMetadataWithoutCachingWholeBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"collectionName":"c","data":[{"id":1},{"id":2}]}`))
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	var value struct {
		CollectionName string           `json:"collectionName" binding:"required"`
		Data           []map[string]any `json:"data" binding:"required"`
	}
	rows, err := BindBulkJSON(ctx, &value, 4<<20, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if value.CollectionName != "c" || len(rows) != 2 || rows[0] != `{"id":1}` || rows[1] != `{"id":2}` {
		t.Fatalf("bound metadata=%+v rows=%q", value, rows)
	}
	if _, cached := ctx.Get(gin.BodyBytesKey); cached {
		t.Fatal("bulk binder cached the whole JSON body")
	}
	if rest, err := io.ReadAll(ctx.Request.Body); err != nil || len(rest) != 0 {
		t.Fatalf("unconsumed request body %q: %v", rest, err)
	}
}

func TestBindBulkJSONRejectsKnownOversizeBodyBeforeReading(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"data":[{"id":1}]}`))
	request.ContentLength = 100
	input := &countingJSONReader{reader: request.Body}
	request.Body = io.NopCloser(input)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	if _, err := BindBulkJSON(ctx, &struct{}{}, 1024, 20); !errors.Is(err, merr.ErrParameterTooLarge) {
		t.Fatalf("error = %v, want size rejection", err)
	}
	if input.read != 0 {
		t.Fatalf("read %d bytes before known-size rejection", input.read)
	}
}

func TestBindBulkJSONRejectsMissingCollectionName(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"data":[{"id":1}]}`))
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	var value struct {
		CollectionName string           `json:"collectionName" binding:"required"`
		Data           []map[string]any `json:"data" binding:"required"`
	}
	if _, err := BindBulkJSON(ctx, &value, 4<<20, 8<<20); err == nil {
		t.Fatal("missing collection name passed binding validation")
	}
}
