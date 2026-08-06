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
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type discardBenchmarkWriter struct {
	header http.Header
	status int
	bytes  int64
	writes int64
	start  time.Time
	ttfb   time.Duration
}

func newDiscardBenchmarkWriter() *discardBenchmarkWriter {
	return &discardBenchmarkWriter{
		header: make(http.Header),
		status: http.StatusOK,
		start:  time.Now(),
	}
}

func (w *discardBenchmarkWriter) Header() http.Header {
	return w.header
}

func (w *discardBenchmarkWriter) WriteHeader(status int) {
	w.status = status
}

func (w *discardBenchmarkWriter) Write(data []byte) (int, error) {
	if w.writes == 0 {
		w.ttfb = time.Since(w.start)
	}
	w.bytes += int64(len(data))
	w.writes++
	return len(data), nil
}

func (w *discardBenchmarkWriter) Flush() {}

type benchmarkJSONRows struct {
	rows []map[string]interface{}
}

func (rows *benchmarkJSONRows) Len() int64 {
	return int64(len(rows.rows))
}

func (rows *benchmarkJSONRows) Row(index int64) (map[string]interface{}, error) {
	return rows.rows[index], nil
}

// MarshalJSON preserves the baseline behavior: older jsonRender versions see
// this as a normal value and encode the complete row slice at once. The new
// renderer recognizes the row-source interface and emits rows incrementally.
func (rows *benchmarkJSONRows) MarshalJSON() ([]byte, error) {
	return json.Marshal(rows.rows)
}

func generateRESTLargeTopKRows(rowCount int) []map[string]interface{} {
	value := strings.Repeat("x", 48)
	rows := make([]map[string]interface{}, rowCount)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"primary_key": int64(i),
			"field_1":     value,
			"field_2":     value,
			"field_3":     value,
			"field_4":     value,
			"field_5":     value,
			"distance":    float32(i%1000) / 1000,
		}
	}
	return rows
}

func benchmarkRESTLargeTopK(b *testing.B, rowCount int, timeoutWrapped bool) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "600000")
	b.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	rows := generateRESTLargeTopKRows(rowCount)
	result := gin.H{
		HTTPReturnCode:  int32(0),
		HTTPReturnData:  &benchmarkJSONRows{rows: rows},
		HTTPReturnTopks: []int64{int64(rowCount)},
	}
	handler := func(c *gin.Context) {
		HTTPReturnStream(c, http.StatusOK, result)
	}

	router := gin.New()
	if timeoutWrapped {
		router.POST("/large", timeoutMiddleware(handler))
	} else {
		router.POST("/large", handler)
	}
	request := httptest.NewRequest(http.MethodPost, "/large", nil)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	var totalTTFB time.Duration
	for i := 0; i < b.N; i++ {
		writer := newDiscardBenchmarkWriter()
		router.ServeHTTP(writer, request.Clone(request.Context()))
		if writer.status != http.StatusOK {
			b.Fatalf("unexpected status %d", writer.status)
		}
		if writer.bytes == 0 {
			b.Fatal("empty response")
		}
		totalTTFB += writer.ttfb
	}
	b.StopTimer()
	b.ReportMetric(float64(rowCount), "rows/op")
	b.ReportMetric(float64(totalTTFB.Nanoseconds())/float64(b.N), "first-write-ns/op")
}

func BenchmarkRESTLargeTopK(b *testing.B) {
	gin.SetMode(gin.TestMode)
	for _, rowCount := range []int{10, 1_000, 30_000, 300_000} {
		name := "rows_" + strconv.Itoa(rowCount)
		b.Run("direct/"+name, func(b *testing.B) {
			benchmarkRESTLargeTopK(b, rowCount, false)
		})
		b.Run("timeout/"+name, func(b *testing.B) {
			benchmarkRESTLargeTopK(b, rowCount, true)
		})
	}
}

func benchmarkRESTFewWideRows(b *testing.B, timeoutWrapped bool) {
	// This fixture documents the remaining O(max encoded row) scratch bound.
	// Reported B/op is cumulative allocation volume, not peak live memory; the
	// 64 KiB stream buffer only bounds downstream write buffering.
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "600000")
	b.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	const rowCount = 3
	vector := make([]float32, 64*1024)
	for index := range vector {
		vector[index] = float32(index%1000) / 1000
	}
	payload := strings.Repeat("x", 1024*1024)
	rows := make([]map[string]interface{}, rowCount)
	for index := range rows {
		rows[index] = map[string]interface{}{
			"primary_key": int64(index),
			"vector":      vector,
			"payload":     payload,
		}
	}
	result := gin.H{
		HTTPReturnCode: int32(0),
		HTTPReturnData: &benchmarkJSONRows{rows: rows},
	}
	handler := func(c *gin.Context) {
		HTTPReturnStream(c, http.StatusOK, result)
	}

	router := gin.New()
	if timeoutWrapped {
		router.POST("/wide", timeoutMiddleware(handler))
	} else {
		router.POST("/wide", handler)
	}
	request := httptest.NewRequest(http.MethodPost, "/wide", nil)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	var responseBytes int64
	for iteration := 0; iteration < b.N; iteration++ {
		writer := newDiscardBenchmarkWriter()
		router.ServeHTTP(writer, request.Clone(request.Context()))
		if writer.status != http.StatusOK {
			b.Fatalf("unexpected status %d", writer.status)
		}
		if writer.bytes == 0 {
			b.Fatal("empty response")
		}
		responseBytes = writer.bytes
	}
	b.StopTimer()
	b.ReportMetric(rowCount, "rows/op")
	b.ReportMetric(float64(responseBytes), "response-bytes/op")
}

func BenchmarkRESTFewWideRows(b *testing.B) {
	gin.SetMode(gin.TestMode)
	b.Run("direct", func(b *testing.B) {
		benchmarkRESTFewWideRows(b, false)
	})
	b.Run("timeout", func(b *testing.B) {
		benchmarkRESTFewWideRows(b, true)
	})
}

func generateRESTLargeTopKFieldData(rowCount int) ([]string, []*schemapb.FieldData, *schemapb.IDs, []float32, *schemapb.CollectionSchema) {
	value := strings.Repeat("x", 48)
	outputFields := []string{"field_1", "field_2", "field_3", "field_4", "field_5"}
	fieldData := make([]*schemapb.FieldData, 0, len(outputFields))
	for _, fieldName := range outputFields {
		values := make([]string, rowCount)
		for index := range values {
			values[index] = value
		}
		fieldData = append(fieldData, &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: values},
			}}},
		})
	}
	ids := make([]int64, rowCount)
	scores := make([]float32, rowCount)
	for index := range ids {
		ids[index] = int64(index)
		scores[index] = float32(index%1000) / 1000
	}
	return outputFields, fieldData, &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		}, scores, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			Name:         "primary_key",
			DataType:     schemapb.DataType_Int64,
			IsPrimaryKey: true,
		}}}
}

func benchmarkRESTLargeTopKResponsePath(b *testing.B, rowCount int) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "600000")
	b.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	outputFields, fieldData, ids, scores, schema := generateRESTLargeTopKFieldData(rowCount)
	handler := func(c *gin.Context) {
		rows, err := newQueryResponseRows(0, outputFields, fieldData, ids, scores, true, schema)
		if err != nil {
			b.Fatal(err)
		}
		HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode:  int32(0),
			HTTPReturnData:  rows,
			HTTPReturnTopks: []int64{int64(rowCount)},
		})
	}

	router := gin.New()
	router.POST("/large", timeoutMiddleware(handler))
	request := httptest.NewRequest(http.MethodPost, "/large", nil)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	var totalTTFB time.Duration
	for iteration := 0; iteration < b.N; iteration++ {
		writer := newDiscardBenchmarkWriter()
		router.ServeHTTP(writer, request.Clone(request.Context()))
		if writer.status != http.StatusOK {
			b.Fatalf("unexpected status %d", writer.status)
		}
		if writer.bytes == 0 {
			b.Fatal("empty response")
		}
		totalTTFB += writer.ttfb
	}
	b.StopTimer()
	b.ReportMetric(float64(rowCount), "rows/op")
	b.ReportMetric(float64(totalTTFB.Nanoseconds())/float64(b.N), "first-write-ns/op")
}

func BenchmarkRESTLargeTopKResponsePath(b *testing.B) {
	gin.SetMode(gin.TestMode)
	for _, rowCount := range []int{10, 1_000, 30_000, 300_000} {
		b.Run("rows_"+strconv.Itoa(rowCount), func(b *testing.B) {
			benchmarkRESTLargeTopKResponsePath(b, rowCount)
		})
	}
}
