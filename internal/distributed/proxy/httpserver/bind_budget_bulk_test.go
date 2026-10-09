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
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/requestbudget"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type budgetBulkFixture struct {
	CollectionName string           `json:"collectionName"`
	Data           []map[string]any `json:"data"`
}

func bulkFixtureContext(body []byte) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/test", bytes.NewReader(body))
	return c
}

func TestBindBudgetBulkRowsLargeArray(t *testing.T) {
	row := `{"text":"` + strings.Repeat("x", 2<<20) + `"}`
	body := []byte(`{"collectionName":"books","data":[` + row + `,` + row + `]}`)
	c := bulkFixtureContext(body)
	var req budgetBulkFixture
	err := bindBudgetBulkRows(c, &req, 1<<20, func(rows []map[string]any) { req.Data = rows })
	if err != nil || len(req.Data) != 2 || req.CollectionName != "books" {
		t.Fatalf("request=%#v error=%v", req, err)
	}
	if saved, ok := c.Get(gin.BodyBytesKey); !ok || !bytes.Equal(saved.([]byte), body) {
		t.Fatal("original body not retained for schema conversion")
	}
}

func TestBindBudgetBulkRowsBoundsSingleFallback(t *testing.T) {
	body := []byte(`{"collectionName":"books","data":{"text":"` + strings.Repeat("x", 4<<20) + `"}}`)
	c := bulkFixtureContext(body)
	var req budgetBulkFixture
	err := bindBudgetBulkRows(c, &req, requestbudget.MaxJSONUnitBytes, func(rows []map[string]any) { req.Data = rows })
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error=%v, want input capacity error", err)
	}

	shortBody := []byte(`{"collectionName":"books","data":{"id":1}}`)
	c = bulkFixtureContext(shortBody)
	err = bindBudgetBulkRows(c, &req, requestbudget.MaxJSONUnitBytes, func(rows []map[string]any) { req.Data = rows })
	if err == nil {
		t.Fatal("bulk bind unexpectedly accepted single-row object")
	}
	var single struct {
		Data map[string]any `json:"data"`
	}
	if err := c.ShouldBindBodyWith(&single, binding.JSON); err != nil || single.Data["id"] != float64(1) {
		t.Fatalf("bounded legacy single fallback=%#v error=%v", single, err)
	}
}

func TestBindBudgetBulkRowsChecksContext(t *testing.T) {
	c := bulkFixtureContext([]byte(`{"data":[{"id":1}]}`))
	ctx, cancel := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	cancel()
	var req budgetBulkFixture
	err := bindBudgetBulkRows(c, &req, 1<<20, func(rows []map[string]any) { req.Data = rows })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want canceled", err)
	}
}

func TestBindBudgetJSONUnitRejectsLargeNonBulk(t *testing.T) {
	c := bulkFixtureContext([]byte(`{"name":"` + strings.Repeat("x", 100) + `"}`))
	var req struct {
		Name string `json:"name"`
	}
	err := bindBudgetJSONUnit(c, &req, 64)
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error=%v, want input capacity error", err)
	}
	if req.Name != "" {
		t.Fatal("oversize value reached Sonic decoder")
	}

	c = bulkFixtureContext([]byte(`{"name":"ok"}`))
	err = bindBudgetJSONUnit(c, &req, 64)
	if err != nil || req.Name != "ok" {
		t.Fatalf("request=%#v error=%v", req, err)
	}
}
