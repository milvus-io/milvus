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

func TestBindJSONWithLimitCachesCompleteBodyForSchemaConversion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"collectionName":"c","data":[{"id":1},{"id":2}]}`))
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	var value struct {
		CollectionName string           `json:"collectionName" binding:"required"`
		Data           []map[string]any `json:"data" binding:"required"`
	}
	err := BindJSONWithLimit(ctx, &value, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if value.CollectionName != "c" || len(value.Data) != 2 || value.Data[0]["id"] != float64(1) || value.Data[1]["id"] != float64(2) {
		t.Fatalf("bound request=%+v", value)
	}
	if body, cached := ctx.Get(gin.BodyBytesKey); !cached || string(body.([]byte)) != `{"collectionName":"c","data":[{"id":1},{"id":2}]}` {
		t.Fatalf("full JSON body was not cached: cached=%t body=%v", cached, body)
	}
	if rest, err := io.ReadAll(ctx.Request.Body); err != nil || len(rest) != 0 {
		t.Fatalf("unconsumed request body %q: %v", rest, err)
	}
}

func TestBindJSONWithLimitDoesNotRejectOneLargeRowBelowBodyLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const rowValueBytes = (4 << 20) + 1
	body := `{"collectionName":"c","data":[{"text":"` + strings.Repeat("x", rowValueBytes) + `"}]}`
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	var value struct {
		CollectionName string           `json:"collectionName" binding:"required"`
		Data           []map[string]any `json:"data" binding:"required"`
	}
	if err := BindJSONWithLimit(ctx, &value, 8<<20); err != nil {
		t.Fatalf("body below limit rejected because of a row size: %v", err)
	}
	if len(value.Data) != 1 {
		t.Fatalf("bound row count=%d", len(value.Data))
	}
	text, ok := value.Data[0]["text"].(string)
	if !ok || len(text) != rowValueBytes {
		t.Fatalf("bound text length=%d, type=%T", len(text), value.Data[0]["text"])
	}
}

func TestBindJSONWithLimitRejectsKnownOversizeBodyBeforeReading(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"data":[{"id":1}]}`))
	request.ContentLength = 100
	input := &countingBodyReader{reader: request.Body}
	request.Body = io.NopCloser(input)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	if err := BindJSONWithLimit(ctx, &struct{}{}, 20); !errors.Is(err, merr.ErrParameterTooLarge) {
		t.Fatalf("error = %v, want size rejection", err)
	}
	if input.read != 0 {
		t.Fatalf("read %d bytes before known-size rejection", input.read)
	}
}

func TestBindJSONWithLimitRejectsUnknownLengthAtReadLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"data":[{"text":"`+strings.Repeat("x", 64)+`"}]}`))
	request.ContentLength = -1
	input := &countingBodyReader{reader: request.Body}
	request.Body = io.NopCloser(input)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	if err := BindJSONWithLimit(ctx, &struct{}{}, 20); !errors.Is(err, merr.ErrParameterTooLarge) {
		t.Fatalf("error = %v, want size rejection", err)
	}
	if input.read != 21 {
		t.Fatalf("read %d bytes, want limit plus one", input.read)
	}
}

type countingBodyReader struct {
	reader io.Reader
	read   int
}

func (r *countingBodyReader) Read(buf []byte) (int, error) {
	n, err := r.reader.Read(buf)
	r.read += n
	return n, err
}
