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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
)

var testResult = gin.H{
	"message": "This is a test message",
	"data":    make(map[string]interface{}),
}

func init() {
	const chunkSize = 1024
	rs := randomString(chunkSize)
	var sb strings.Builder
	for sb.Len() < 10*1024*1024 {
		sb.WriteString(rs)
	}
	testResult["data"] = sb.String()
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(charset[rand.Intn(len(charset))])
	}
	return result.String()
}

type countedJSONValue struct {
	encodedRows *int
	value       string
}

type testJSONRows struct {
	rows     []map[string]interface{}
	rowCalls int
}

func (rows *testJSONRows) Len() int64 {
	return int64(len(rows.rows))
}

func (rows *testJSONRows) Row(index int64) (map[string]interface{}, error) {
	rows.rowCalls++
	return rows.rows[index], nil
}

func (v countedJSONValue) MarshalJSON() ([]byte, error) {
	*v.encodedRows = *v.encodedRows + 1
	return []byte(strconv.Quote(v.value)), nil
}

type streamingCaptureWriter struct {
	header                http.Header
	body                  bytes.Buffer
	status                int
	writeCount            int
	firstWriteEncodedRows int
	encodedRows           *int
}

func newStreamingCaptureWriter(encodedRows *int) *streamingCaptureWriter {
	return &streamingCaptureWriter{
		header:                make(http.Header),
		status:                http.StatusOK,
		firstWriteEncodedRows: -1,
		encodedRows:           encodedRows,
	}
}

func (w *streamingCaptureWriter) Header() http.Header {
	return w.header
}

func (w *streamingCaptureWriter) WriteHeader(status int) {
	w.status = status
}

func (w *streamingCaptureWriter) Write(data []byte) (int, error) {
	if w.firstWriteEncodedRows < 0 {
		w.firstWriteEncodedRows = *w.encodedRows
	}
	w.writeCount++
	return w.body.Write(data)
}

func TestJSONRenderStreamsRowsIncrementally(t *testing.T) {
	encodedRows := 0
	rows := make([]map[string]interface{}, 256)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"id": i,
			"value": countedJSONValue{
				encodedRows: &encodedRows,
				value:       strings.Repeat("x", 256),
			},
		}
	}

	rowSource := &testJSONRows{rows: rows}
	w := newStreamingCaptureWriter(&encodedRows)
	err := (jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: rowSource,
		HTTPReturnTopks: []int64{
			int64(len(rows)),
		},
	}}).Render(w)
	require.NoError(t, err)
	// JSON encoders may invoke a custom Marshaler more than once under race or
	// coverage instrumentation. Row production itself must still happen once,
	// and a response larger than the stream buffer must start writing before
	// all rows have been encoded.
	assert.GreaterOrEqual(t, encodedRows, len(rows))
	assert.Equal(t, len(rows), rowSource.rowCalls)
	assert.Positive(t, w.firstWriteEncodedRows)
	assert.Less(t, w.firstWriteEncodedRows, encodedRows)
	assert.Greater(t, w.writeCount, 1)
	assert.Equal(t, jsonContentType[0], w.header.Get("Content-Type"))

	var response struct {
		Code  int                      `json:"code"`
		Data  []map[string]interface{} `json:"data"`
		Topks []int64                  `json:"topks"`
	}
	require.NoError(t, json.Unmarshal(w.body.Bytes(), &response))
	assert.Zero(t, response.Code)
	assert.Len(t, response.Data, len(rows))
	assert.Equal(t, []int64{int64(len(rows))}, response.Topks)
}

func TestJSONRenderStreamsFewWideRowsWithoutRecorderBuffering(t *testing.T) {
	encodedRows := 0
	rows := make([]map[string]interface{}, 3)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"id": i,
			"vector": countedJSONValue{
				encodedRows: &encodedRows,
				value:       strings.Repeat("x", streamingJSONBufferSize*2),
			},
		}
	}

	rowSource := &testJSONRows{rows: rows}
	recorderBuffer := &bytes.Buffer{}
	recorder := newTimeoutResponseRecorder(recorderBuffer)
	require.NoError(t, (jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: rowSource,
	}}).Render(recorder))
	assert.Zero(t, recorderBuffer.Len())
	assert.Zero(t, rowSource.rowCalls)

	networkWriter := newStreamingCaptureWriter(&encodedRows)
	realCtx, _ := gin.CreateTestContext(networkWriter)
	require.NoError(t, recorder.CommitTo(context.Background(), realCtx.Writer))

	assert.Equal(t, len(rows), rowSource.rowCalls)
	assert.GreaterOrEqual(t, encodedRows, len(rows))
	assert.Positive(t, networkWriter.firstWriteEncodedRows)
	assert.Less(t, networkWriter.firstWriteEncodedRows, encodedRows)
	assert.Greater(t, networkWriter.writeCount, 1)
	assert.Zero(t, recorderBuffer.Len())
	var response struct {
		Code int                      `json:"code"`
		Data []map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(networkWriter.body.Bytes(), &response))
	assert.Zero(t, response.Code)
	assert.Len(t, response.Data, len(rows))
}

func TestJSONRenderRowSourceEquivalentToMaterializedJSON(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1), "name": "one", "nullable": nil},
		{"id": int64(2), "name": "two", "vector": []float32{0.1, 0.2}},
	}
	data := gin.H{
		HTTPReturnCode:               int32(0),
		HTTPReturnData:               &testJSONRows{rows: rows},
		HTTPReturnCost:               7,
		HTTPReturnTopks:              []int64{2},
		HTTPReturnRecalls:            []float32{0.5},
		HTTPReturnScannedRemoteBytes: int64(11),
		HTTPReturnScannedTotalBytes:  int64(22),
		HTTPReturnCacheHitRatio:      0.5,
	}

	streamed := httptest.NewRecorder()
	require.NoError(t, (jsonRender{Data: data}).Render(streamed))

	expected, err := json.Marshal(gin.H{
		HTTPReturnCode:               int32(0),
		HTTPReturnData:               rows,
		HTTPReturnCost:               7,
		HTTPReturnTopks:              []int64{2},
		HTTPReturnRecalls:            []float32{0.5},
		HTTPReturnScannedRemoteBytes: int64(11),
		HTTPReturnScannedTotalBytes:  int64(22),
		HTTPReturnCacheHitRatio:      0.5,
	})
	require.NoError(t, err)

	var streamedValue interface{}
	var expectedValue interface{}
	require.NoError(t, json.Unmarshal(streamed.Body.Bytes(), &streamedValue))
	require.NoError(t, json.Unmarshal(expected, &expectedValue))
	assert.Equal(t, expectedValue, streamedValue)
}

func TestJSONRenderRendersEmptyRows(t *testing.T) {
	response := httptest.NewRecorder()
	require.NoError(t, (jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: &testJSONRows{},
	}}).Render(response))

	var body struct {
		Code int                      `json:"code"`
		Data []map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Zero(t, body.Code)
	assert.Empty(t, body.Data)
}

type failingJSONValue struct {
	err error
}

func (value failingJSONValue) MarshalJSON() ([]byte, error) {
	return nil, value.err
}

type cancelingDeferredRenderWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
	err    error
}

func (writer *cancelingDeferredRenderWriter) DeferRender(_ render.Render) error {
	writer.cancel()
	return writer.err
}

func TestJSONRenderPrefersContextErrorDuringDeferredRegistrationRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	registrationErr := errors.New("response writer closed")
	writer := &cancelingDeferredRenderWriter{
		ResponseRecorder: httptest.NewRecorder(),
		cancel:           cancel,
		err:              registrationErr,
	}

	err := (jsonRender{Context: ctx, Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
	}}).Render(writer)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, registrationErr)
}

func TestHTTPReturnStreamReturnsRenderError(t *testing.T) {
	expectedErr := errors.New("metadata encode failed")
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodGet, "/stream", nil)

	err := HTTPReturnStream(c, http.StatusOK, gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
		"invalid":      failingJSONValue{err: expectedErr},
	})

	require.ErrorIs(t, err, expectedErr)
	assert.True(t, c.IsAborted())
	assert.Empty(t, response.Body.String())
}

func TestJSONRenderKeepsGenericEncodingInHandler(t *testing.T) {
	recorderBuffer := &bytes.Buffer{}
	recorder := newTimeoutResponseRecorder(recorderBuffer)

	require.NoError(t, (jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: []gin.H{{"id": 1}},
	}}).Render(recorder))

	assert.Nil(t, recorder.render)
	assert.NotEmpty(t, recorderBuffer.Bytes())
}

func TestJSONRenderValidatesMetadataBeforeCommit(t *testing.T) {
	expectedErr := errors.New("metadata encode failed")
	for name, rowCount := range map[string]int{
		"small response": 1,
		"large response": 256,
	} {
		t.Run(name, func(t *testing.T) {
			recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
			rowSource := &testJSONRows{rows: make([]map[string]interface{}, rowCount)}
			err := (jsonRender{Data: gin.H{
				HTTPReturnCode: 0,
				HTTPReturnData: rowSource,
				"invalid":      failingJSONValue{err: expectedErr},
			}}).Render(recorder)

			require.ErrorIs(t, err, expectedErr)
			assert.Nil(t, recorder.render)
			assert.Zero(t, recorder.body.Len())
			assert.Zero(t, rowSource.rowCalls)
		})
	}
}

type failAfterJSONWrites struct {
	header http.Header
	writes int
	failAt int
	err    error
}

func (w *failAfterJSONWrites) Header() http.Header {
	return w.header
}

func (w *failAfterJSONWrites) WriteHeader(int) {}

func (w *failAfterJSONWrites) Write(data []byte) (int, error) {
	w.writes++
	if w.writes >= w.failAt {
		return 0, w.err
	}
	return len(data), nil
}

func TestJSONRenderStopsProducingRowsAfterWriteFailure(t *testing.T) {
	rows := make([]map[string]interface{}, 1000)
	for index := range rows {
		rows[index] = map[string]interface{}{
			"id":    index,
			"value": strings.Repeat("x", 2048),
		}
	}
	rowSource := &testJSONRows{rows: rows}
	expectedErr := errors.New("client disconnected")
	w := &failAfterJSONWrites{
		header: make(http.Header),
		failAt: 3,
		err:    expectedErr,
	}

	err := (jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: rowSource,
	}}).Render(w)

	require.ErrorIs(t, err, expectedErr)
	assert.Positive(t, rowSource.rowCalls)
	assert.Less(t, rowSource.rowCalls, len(rows))
}

type deadlineBlockingJSONWriter struct {
	header  http.Header
	body    bytes.Buffer
	ctx     *manuallyExpiredContext
	writes  int
	blockAt int
}

type manuallyExpiredContext struct {
	context.Context
	done chan struct{}
}

func newManuallyExpiredContext() *manuallyExpiredContext {
	return &manuallyExpiredContext{
		Context: context.Background(),
		done:    make(chan struct{}),
	}
}

func (c *manuallyExpiredContext) Deadline() (time.Time, bool) {
	return time.Now().Add(time.Hour), true
}

func (c *manuallyExpiredContext) Done() <-chan struct{} {
	return c.done
}

func (c *manuallyExpiredContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *manuallyExpiredContext) expire() {
	close(c.done)
}

func (w *deadlineBlockingJSONWriter) Header() http.Header {
	return w.header
}

func (w *deadlineBlockingJSONWriter) WriteHeader(int) {}

func (w *deadlineBlockingJSONWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes == w.blockAt {
		w.ctx.expire()
		return 0, w.ctx.Err()
	}
	return w.body.Write(data)
}

func TestJSONRenderStopsProducingRowsAfterDeadlineDuringStreaming(t *testing.T) {
	rows := make([]map[string]interface{}, 100)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"id":    i,
			"value": strings.Repeat("x", streamingJSONBufferSize*2),
		}
	}
	rowSource := &testJSONRows{rows: rows}
	ctx := newManuallyExpiredContext()
	w := &deadlineBlockingJSONWriter{
		header:  make(http.Header),
		ctx:     ctx,
		blockAt: 2,
	}

	err := (jsonRender{Context: ctx, Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: rowSource,
	}}).Render(w)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 2, w.writes)
	assert.Positive(t, w.body.Len(), "the first buffered chunk must be written before the deadline")
	assert.Positive(t, rowSource.rowCalls)
	assert.Less(t, rowSource.rowCalls, len(rows))
}

func BenchmarkHTTPReturn(b *testing.B) {
	// Set Gin to test mode to prevent output to stdout
	gin.SetMode(gin.TestMode)

	b.Run("test HTTPReturn", func(b *testing.B) {
		router := gin.New()
		router.GET("/test1", func(c *gin.Context) {
			HTTPReturn(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test1", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})

	b.Run("test HTTPReturnStream", func(b *testing.B) {
		router := gin.New()
		router.GET("/test2", func(c *gin.Context) {
			HTTPReturnStream(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test2", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})
}

// goos: linux
// goarch: amd64
// pkg: github.com/milvus-io/milvus/internal/distributed/proxy/httpserver
// cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
// BenchmarkHTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn-12         	      87	  13127452 ns/op	27992718 B/op	      34 allocs/op
// BenchmarkHTTPReturn/test_HTTPReturnStream
// BenchmarkHTTPReturn/test_HTTPReturnStream-12   	      87	  12804875 ns/op	14361636 B/op	      31 allocs/op
// PASS
//
// Process finished with the exit code 0
