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

// These tests also run as a native-free file list on development hosts that
// lack the Milvus C++ libraries.
package httpserver

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBudgetJSONRenderPreservesEnvelope(t *testing.T) {
	data := gin.H{
		"code":   0,
		"data":   []map[string]any{{"id": 1, "vector": []float64{1, 2}}, {"id": 2, "vector": []float64{3, 4}}},
		"topks":  []int64{2},
		"opaque": []byte{1, 2, 3},
		"nested": gin.H{"ids": []int64{1, 2}, "raw": stdjson.RawMessage(`{"ok":true}`)},
	}
	recorder := httptest.NewRecorder()
	if err := (budgetJSONRender{Data: data, Ctx: context.Background()}).Render(recorder); err != nil {
		t.Fatal(err)
	}
	wantBytes, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := stdjson.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if err := stdjson.Unmarshal(wantBytes, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rendered envelope differs: got %v, want %v", got, want)
	}
}

type slowJSONUnit struct{ calls *atomic.Int32 }

func (v slowJSONUnit) MarshalJSON() ([]byte, error) {
	v.calls.Add(1)
	time.Sleep(20 * time.Millisecond)
	return []byte("1"), nil
}

func TestBudgetJSONRenderStopsBetweenItems(t *testing.T) {
	var calls atomic.Int32
	units := make([]slowJSONUnit, 10)
	for i := range units {
		units[i].calls = &calls
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (budgetJSONRender{Data: gin.H{"data": units}, Ctx: ctx}).Render(httptest.NewRecorder())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("render error = %v, want deadline exceeded", err)
	}
	if got := calls.Load(); got < 1 || got > 3 {
		t.Fatalf("encoded %d units after deadline, want bounded overshoot", got)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("renderer exited too late: %v", elapsed)
	}
}

func TestBudgetJSONRenderStopsInsideNestedData(t *testing.T) {
	var calls atomic.Int32
	units := make([]slowJSONUnit, 10)
	for i := range units {
		units[i].calls = &calls
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	err := (budgetJSONRender{Data: gin.H{"data": gin.H{"ids": units}}, Ctx: ctx}).Render(httptest.NewRecorder())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("render error = %v, want deadline exceeded", err)
	}
	if got := calls.Load(); got < 1 || got > 3 {
		t.Fatalf("encoded %d nested units after deadline", got)
	}
}

func TestBudgetJSONRenderRealVectorStopsBeforeNextUnit(t *testing.T) {
	vector := make([]float64, (4<<20)/8)
	for i := range vector {
		vector[i] = float64(i%1000) / 1000
	}
	var nextCalls atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := (budgetJSONRender{Data: gin.H{"data": []any{vector, slowJSONUnit{calls: &nextCalls}}}, Ctx: ctx}).Render(httptest.NewRecorder())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("render error = %v, want deadline exceeded", err)
	}
	if nextCalls.Load() != 0 {
		t.Fatal("renderer began the next JSON unit after deadline")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("renderer took %v to exit after deadline", elapsed)
	}
}

func TestBudgetJSONRenderRejectsOversizeUnitBeforeSendingSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/probe", func(c *gin.Context) {
		renderBudgetJSON(c, 200, gin.H{"data": []string{strings.Repeat("x", 4<<20)}})
	})
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest("GET", "/probe", nil))
	if recorder.Code != 500 {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	var body map[string]any
	if err := stdjson.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != float64(merr.Code(merr.ErrServiceInternal)) {
		t.Fatalf("error body = %#v", body)
	}
}

func BenchmarkBudgetJSONRender(b *testing.B) {
	vector := make([]float64, 128)
	for i := range vector {
		vector[i] = float64(i) / 128
	}
	rows := make([]map[string]any, 1024)
	for i := range rows {
		rows[i] = map[string]any{"id": i, "vector": vector}
	}
	data := gin.H{"code": 0, "data": rows}
	b.Run("legacy-whole-value", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := (jsonRender{Data: data}).Render(httptest.NewRecorder()); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("budget-per-row", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := (budgetJSONRender{Data: data, Ctx: context.Background()}).Render(httptest.NewRecorder()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
