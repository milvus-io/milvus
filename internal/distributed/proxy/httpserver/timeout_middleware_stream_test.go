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
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTimeoutResponseRecorderDefersStreamRender(t *testing.T) {
	buffer := &bytes.Buffer{}
	recorder := newTimeoutResponseRecorder(buffer)
	recorder.WriteHeader(http.StatusAccepted)

	rows := make([]map[string]interface{}, 3)
	for index := range rows {
		rows[index] = map[string]interface{}{"id": float64(index + 1)}
	}
	responseRender := jsonRender{Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: &testJSONRows{rows: rows},
	}}
	require.NoError(t, responseRender.Render(recorder))
	assert.Zero(t, buffer.Len())
	assert.NotNil(t, recorder.render)

	_, err := recorder.Write([]byte("late body"))
	require.Error(t, err)

	realResponse := httptest.NewRecorder()
	realCtx, _ := gin.CreateTestContext(realResponse)
	require.NoError(t, recorder.CommitTo(context.Background(), realCtx.Writer))
	assert.Equal(t, http.StatusAccepted, realResponse.Code)

	var response struct {
		Code int                      `json:"code"`
		Data []map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(realResponse.Body.Bytes(), &response))
	assert.Zero(t, response.Code)
	assert.Equal(t, rows, response.Data)
}

func TestBufferPoolDropsOversizedBuffers(t *testing.T) {
	pool := &BufferPool{}
	large := bytes.NewBuffer(make([]byte, maxPooledResponseBufferCapacity+1))
	assert.False(t, isReusableResponseBuffer(large))
	pool.Put(large)

	small := bytes.NewBuffer(make([]byte, 128))
	assert.True(t, isReusableResponseBuffer(small))
	pool.Put(small)
	assert.Zero(t, small.Len())
	assert.False(t, isReusableResponseBuffer(nil))
}

type blockingTestRender struct {
	started chan struct{}
	release chan struct{}
}

type contextBoundJSONValue struct {
	ctx         context.Context
	started     chan struct{}
	stopped     chan struct{}
	startedOnce sync.Once
	stoppedOnce sync.Once
}

func (value *contextBoundJSONValue) MarshalJSON() ([]byte, error) {
	// Sonic deliberately marshals values again with encoding/json under the
	// race build, so test notifications must tolerate repeated calls.
	value.startedOnce.Do(func() { close(value.started) })
	for value.ctx.Err() == nil {
		runtime.Gosched()
	}
	value.stoppedOnce.Do(func() { close(value.stopped) })
	return nil, value.ctx.Err()
}

func (r *blockingTestRender) Render(w http.ResponseWriter) error {
	close(r.started)
	<-r.release
	_, err := w.Write([]byte("rendered"))
	return err
}

func (r *blockingTestRender) WriteContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
}

func TestTimeoutResponseRecorderDoesNotHoldLockDuringRender(t *testing.T) {
	recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
	responseRender := &blockingTestRender{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, recorder.DeferRender(responseRender))

	realResponse := httptest.NewRecorder()
	realCtx, _ := gin.CreateTestContext(realResponse)
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- recorder.CommitTo(context.Background(), realCtx.Writer)
	}()
	<-responseRender.started

	lateWriteDone := make(chan error, 1)
	go func() {
		_, err := recorder.Write([]byte("late"))
		lateWriteDone <- err
	}()
	select {
	case err := <-lateWriteDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("late recorder write blocked behind streaming render")
	}

	close(responseRender.release)
	require.NoError(t, <-commitDone)
	assert.Equal(t, "rendered", realResponse.Body.String())
}

func TestTimeoutMiddlewareUsesContentLengthForSmallStreamResponse(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "1000")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	router := gin.New()
	router.GET("/small-stream", timeoutMiddleware(func(c *gin.Context) {
		if err := HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}, {"id": 2}}},
		}); err != nil {
			panic(err)
		}
	}))

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	response, err := server.Client().Get(server.URL + "/small-stream")
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, 1, response.ProtoMajor)
	assert.Equal(t, int64(len(body)), response.ContentLength)
	assert.Empty(t, response.TransferEncoding)
}

func TestTimeoutMiddlewareReturnsTimeoutWhenSmallStreamExpiresBeforeCommit(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "10")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	started := make(chan struct{})
	stopped := make(chan struct{})
	router := gin.New()
	router.POST("/deadline-before-write", timeoutMiddleware(func(c *gin.Context) {
		if err := HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{
				{
					"value": &contextBoundJSONValue{
						ctx:     c.Request.Context(),
						started: started,
						stopped: stopped,
					},
				},
				{"value": "second"},
			}},
		}); err != nil {
			panic(err)
		}
	}))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/deadline-before-write", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusRequestTimeout, response.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, merr.TimeoutCode, body.Code)
	assert.Equal(t, "request timeout", body.Message)
	assert.NotContains(t, response.Body.String(), `"data"`)
	select {
	case <-started:
	default:
		t.Fatal("stream row encoding did not start")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("stream row encoding did not observe request cancellation")
	}
}

func TestTimeoutMiddlewareStopsGenericEncodingBeforeFirstWrite(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "10")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	started := make(chan struct{})
	stopped := make(chan struct{})
	handlerDone := make(chan struct{})
	router := gin.New()
	router.POST("/cpu-before-write", timeoutMiddleware(func(c *gin.Context) {
		defer close(handlerDone)
		_ = HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &contextBoundJSONValue{
				ctx:     c.Request.Context(),
				started: started,
				stopped: stopped,
			},
		})
	}))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/cpu-before-write", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusRequestTimeout, response.Code)
	select {
	case <-started:
	default:
		t.Fatal("generic encoder did not start")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("generic encoder did not observe request cancellation")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler goroutine did not exit after timeout")
	}
}

type deadlineTrackingWriter struct {
	header    http.Header
	body      bytes.Buffer
	deadlines []time.Time
}

type deadlineEnforcingWriter struct {
	*httptest.ResponseRecorder
	deadlineArmed   bool
	deadlineExpired <-chan struct{}
}

type blockingDeadlineWriter struct {
	header           http.Header
	operationStarted chan struct{}
	deadlineSet      chan time.Time
	releaseOperation chan struct{}
	releaseOnce      sync.Once
}

type manualDeadlineContext struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	errMu    sync.RWMutex
	err      error
	once     sync.Once
}

type prepareBlockingDeadlineContext struct {
	context.Context
	deadline          time.Time
	done              chan struct{}
	secondErrEntered  chan struct{}
	releaseSecondErr  chan struct{}
	callbackErrCalled chan struct{}
	errMu             sync.Mutex
	err               error
	errCalls          int
	expireOnce        sync.Once
	secondErrOnce     sync.Once
	callbackErrOnce   sync.Once
	releaseOnce       sync.Once
}

type afterFuncTrackingContext struct {
	context.Context
	done    chan struct{}
	started int
	stopped int
}

type firstWriteSignalWriter struct {
	*httptest.ResponseRecorder
	firstWrite chan struct{}
	once       sync.Once
}

func (w *firstWriteSignalWriter) Write(data []byte) (int, error) {
	w.once.Do(func() {
		close(w.firstWrite)
	})
	return w.ResponseRecorder.Write(data)
}

type requestTimeoutJSONRows struct {
	ctx         context.Context
	requestDone <-chan struct{}
	rowCalls    int
}

func (rows *requestTimeoutJSONRows) Len() int64 {
	return 2
}

func (rows *requestTimeoutJSONRows) Row(index int64) (map[string]interface{}, error) {
	rows.rowCalls++
	if index == 0 {
		return map[string]interface{}{"value": string(bytes.Repeat([]byte{'x'}, streamingJSONBufferSize*2))}, nil
	}
	<-rows.requestDone
	if err := rows.ctx.Err(); err != nil {
		return nil, err
	}
	return map[string]interface{}{"value": "after request timeout"}, nil
}

func TestTimeoutResponseRecorderKeepsRequestTimeoutAfterCommit(t *testing.T) {
	requestContext := newManualDeadlineContext()
	rows := &requestTimeoutJSONRows{
		ctx:         requestContext,
		requestDone: requestContext.Done(),
	}
	recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
	require.NoError(t, (jsonRender{Context: requestContext, Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: rows,
	}}).Render(recorder))

	responseWriter := &firstWriteSignalWriter{
		ResponseRecorder: httptest.NewRecorder(),
		firstWrite:       make(chan struct{}),
	}
	gCtx, _ := gin.CreateTestContext(responseWriter)
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- recorder.CommitTo(requestContext, gCtx.Writer)
	}()

	select {
	case <-responseWriter.firstWrite:
	case <-time.After(time.Second):
		t.Fatal("response was not committed")
	}
	requestContext.expire()
	require.ErrorIs(t, <-commitDone, context.DeadlineExceeded)
	assert.Equal(t, http.StatusOK, responseWriter.Code)
	assert.GreaterOrEqual(t, rows.rowCalls, 1)
	assert.Contains(t, responseWriter.Body.String(), `"data":[`)
	var body map[string]interface{}
	require.Error(t, json.Unmarshal(responseWriter.Body.Bytes(), &body))
}

func (w *deadlineTrackingWriter) Header() http.Header {
	return w.header
}

func (w *deadlineTrackingWriter) WriteHeader(int) {}

func (w *deadlineTrackingWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *deadlineTrackingWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func (w *deadlineEnforcingWriter) Write(data []byte) (int, error) {
	if w.deadlineArmed {
		select {
		case <-w.deadlineExpired:
			return 0, context.DeadlineExceeded
		default:
		}
	}
	return w.ResponseRecorder.Write(data)
}

func (w *deadlineEnforcingWriter) SetWriteDeadline(time.Time) error {
	w.deadlineArmed = true
	return nil
}

func (w *blockingDeadlineWriter) Header() http.Header {
	return w.header
}

func (w *blockingDeadlineWriter) WriteHeader(int) {}

func (w *blockingDeadlineWriter) Write(data []byte) (int, error) {
	w.waitForRelease()
	return len(data), nil
}

func (w *blockingDeadlineWriter) FlushError() error {
	w.waitForRelease()
	return nil
}

func (w *blockingDeadlineWriter) waitForRelease() {
	close(w.operationStarted)
	<-w.releaseOperation
}

func (w *blockingDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlineSet <- deadline
	w.releaseOnce.Do(func() { close(w.releaseOperation) })
	return nil
}

func newManualDeadlineContext() *manualDeadlineContext {
	return &manualDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(time.Hour),
		done:     make(chan struct{}),
	}
}

func (ctx *manualDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func (ctx *manualDeadlineContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *manualDeadlineContext) Err() error {
	ctx.errMu.RLock()
	defer ctx.errMu.RUnlock()
	return ctx.err
}

func (ctx *manualDeadlineContext) expire() {
	ctx.once.Do(func() {
		ctx.errMu.Lock()
		ctx.err = context.DeadlineExceeded
		ctx.errMu.Unlock()
		close(ctx.done)
	})
}

func newPrepareBlockingDeadlineContext() *prepareBlockingDeadlineContext {
	return &prepareBlockingDeadlineContext{
		Context:           context.Background(),
		deadline:          time.Now().Add(time.Hour),
		done:              make(chan struct{}),
		secondErrEntered:  make(chan struct{}),
		releaseSecondErr:  make(chan struct{}),
		callbackErrCalled: make(chan struct{}),
	}
}

func (ctx *prepareBlockingDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func (ctx *prepareBlockingDeadlineContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *prepareBlockingDeadlineContext) Err() error {
	ctx.errMu.Lock()
	ctx.errCalls++
	call := ctx.errCalls
	err := ctx.err
	ctx.errMu.Unlock()

	if call == 2 {
		ctx.secondErrOnce.Do(func() { close(ctx.secondErrEntered) })
		<-ctx.releaseSecondErr
		ctx.errMu.Lock()
		err = ctx.err
		ctx.errMu.Unlock()
		return err
	}
	// The first two Err calls after Done closes propagate cancellation and its
	// cause into the context.AfterFunc child. The following call comes from the
	// registered interruptActiveWrite callback itself.
	if call > 4 {
		ctx.callbackErrOnce.Do(func() { close(ctx.callbackErrCalled) })
	}
	return err
}

func (ctx *prepareBlockingDeadlineContext) expire() {
	ctx.expireOnce.Do(func() {
		ctx.errMu.Lock()
		ctx.err = context.DeadlineExceeded
		ctx.errMu.Unlock()
		close(ctx.done)
	})
}

func (ctx *prepareBlockingDeadlineContext) releasePrepare() {
	ctx.releaseOnce.Do(func() { close(ctx.releaseSecondErr) })
}

func newAfterFuncTrackingContext() *afterFuncTrackingContext {
	return &afterFuncTrackingContext{
		Context: context.Background(),
		done:    make(chan struct{}),
	}
}

func (ctx *afterFuncTrackingContext) Deadline() (time.Time, bool) {
	return time.Now().Add(time.Hour), true
}

func (ctx *afterFuncTrackingContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *afterFuncTrackingContext) Value(key any) any {
	if _, ok := key.(serverWriteTimeoutContextKey); ok {
		return time.Minute
	}
	return ctx.Context.Value(key)
}

func (ctx *afterFuncTrackingContext) AfterFunc(func()) func() bool {
	ctx.started++
	active := true
	return func() bool {
		if !active {
			return false
		}
		active = false
		ctx.stopped++
		return true
	}
}

func TestRequestDeadlineWriterPreservesServerWriteTimeout(t *testing.T) {
	paramtable.Init()

	t.Run("request deadline is not cleared before net/http response finalization", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		writer := &deadlineTrackingWriter{header: make(http.Header)}
		deadlineWriter := newRequestDeadlineWriter(ctx, writer)
		_, err := deadlineWriter.Write([]byte("ok"))
		require.NoError(t, err)

		require.Len(t, writer.deadlines, 1)
		assert.False(t, writer.deadlines[0].IsZero())
	})

	t.Run("configured server timeout is not overwritten", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(withServerWriteTimeout(context.Background(), 5*time.Second), time.Hour)
		defer cancel()
		writer := &deadlineTrackingWriter{header: make(http.Header)}
		deadlineWriter := newRequestDeadlineWriter(ctx, writer)
		_, err := deadlineWriter.Write([]byte("ok"))
		require.NoError(t, err)

		assert.Empty(t, writer.deadlines)
	})

	t.Run("startup snapshot ignores runtime config changes", func(t *testing.T) {
		tests := []struct {
			name            string
			startupTimeout  time.Duration
			runtimeTimeout  string
			expectedUpdates int
		}{
			{
				name:            "server timeout remains enabled",
				startupTimeout:  5 * time.Second,
				runtimeTimeout:  "0s",
				expectedUpdates: 0,
			},
			{
				name:            "server timeout remains disabled",
				startupTimeout:  0,
				runtimeTimeout:  "5s",
				expectedUpdates: 1,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				params := paramtable.Get()
				require.NoError(t, params.Save(params.HTTPCfg.WriteTimeout.Key, test.runtimeTimeout))
				t.Cleanup(func() {
					params.Reset(params.HTTPCfg.WriteTimeout.Key)
				})

				var writeErr error
				writer := &deadlineTrackingWriter{header: make(http.Header)}
				router := gin.New()
				router.Use(serverWriteTimeoutMiddleware(test.startupTimeout))
				router.GET("/write", func(gCtx *gin.Context) {
					ctx, cancel := context.WithTimeout(gCtx.Request.Context(), time.Hour)
					defer cancel()
					deadlineWriter := newRequestDeadlineWriter(ctx, writer)
					_, writeErr = deadlineWriter.Write([]byte("ok"))
					gCtx.Status(http.StatusNoContent)
				})
				router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/write", nil))

				require.NoError(t, writeErr)
				assert.Len(t, writer.deadlines, test.expectedUpdates)
			})
		}
	})

	t.Run("shorter request timeout interrupts active output", func(t *testing.T) {
		tests := []struct {
			name string
			run  func(*requestDeadlineWriter) error
		}{
			{
				name: "write",
				run: func(writer *requestDeadlineWriter) error {
					_, err := writer.Write([]byte("blocked"))
					return err
				},
			},
			{
				name: "flush",
				run: func(writer *requestDeadlineWriter) error {
					return writer.FlushError()
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				requestCtx := newManualDeadlineContext()
				writer := &blockingDeadlineWriter{
					header:           make(http.Header),
					operationStarted: make(chan struct{}),
					deadlineSet:      make(chan time.Time, 1),
					releaseOperation: make(chan struct{}),
				}
				t.Cleanup(func() {
					writer.releaseOnce.Do(func() { close(writer.releaseOperation) })
				})
				deadlineWriter := newRequestDeadlineWriter(withServerWriteTimeout(requestCtx, 2*time.Minute), writer)
				operationDone := make(chan error, 1)
				go func() {
					operationDone <- test.run(deadlineWriter)
				}()

				select {
				case <-writer.operationStarted:
				case <-time.After(time.Second):
					t.Fatal("output operation did not start")
				}
				requestCtx.expire()

				select {
				case deadline := <-writer.deadlineSet:
					assert.WithinDuration(t, time.Now(), deadline, 100*time.Millisecond)
				case <-time.After(time.Second):
					t.Fatal("request deadline did not interrupt active output")
				}

				select {
				case err := <-operationDone:
					require.ErrorIs(t, err, context.DeadlineExceeded)
				case <-time.After(time.Second):
					t.Fatal("output remained blocked after the request deadline")
				}
			})
		}
	})

	t.Run("deadline during prepare does not interrupt inactive output", func(t *testing.T) {
		requestCtx := newPrepareBlockingDeadlineContext()
		t.Cleanup(requestCtx.releasePrepare)
		writer := &deadlineTrackingWriter{header: make(http.Header)}
		deadlineWriter := newRequestDeadlineWriter(withServerWriteTimeout(requestCtx, 2*time.Minute), writer)
		operationDone := make(chan error, 1)
		go func() {
			_, err := deadlineWriter.Write([]byte("late"))
			operationDone <- err
		}()

		select {
		case <-requestCtx.secondErrEntered:
		case <-time.After(time.Second):
			t.Fatal("prepare did not reach its final context check")
		}
		deadlineWriter.writeMu.Lock()
		assert.False(t, deadlineWriter.writeActive)
		deadlineWriter.writeMu.Unlock()

		requestCtx.expire()
		select {
		case <-requestCtx.callbackErrCalled:
		case <-time.After(time.Second):
			t.Fatal("deadline callback did not run")
		}
		// Synchronize with interruptActiveWrite after its context check.
		deadlineWriter.writeMu.Lock()
		assert.False(t, deadlineWriter.writeActive)
		deadlineWriter.writeMu.Unlock()
		assert.Empty(t, writer.deadlines)
		assert.Zero(t, writer.body.Len())

		requestCtx.releasePrepare()
		select {
		case err := <-operationDone:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(time.Second):
			t.Fatal("write did not stop after prepare observed the deadline")
		}
	})
}

func TestRequestDeadlineWriterKeepsTimeoutFallbackWritableBeforeFirstOutput(t *testing.T) {
	requestCtx := newPrepareBlockingDeadlineContext()
	t.Cleanup(requestCtx.releasePrepare)
	writer := &deadlineEnforcingWriter{
		ResponseRecorder: httptest.NewRecorder(),
		deadlineExpired:  requestCtx.Done(),
	}
	gCtx, _ := gin.CreateTestContext(writer)
	deadlineWriter := newRequestDeadlineWriter(requestCtx, gCtx.Writer)

	writeDone := make(chan error, 1)
	go func() {
		_, err := deadlineWriter.Write([]byte("late response"))
		writeDone <- err
	}()

	select {
	case <-requestCtx.secondErrEntered:
	case <-time.After(time.Second):
		t.Fatal("write did not reach its final context check")
	}
	requestCtx.expire()
	requestCtx.releasePrepare()

	select {
	case err := <-writeDone:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("write did not stop after the request deadline")
	}
	require.False(t, gCtx.Writer.Written())

	writeRequestTimeout(gCtx, gCtx.Writer)

	assert.Equal(t, http.StatusRequestTimeout, writer.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &body))
	assert.Equal(t, merr.TimeoutCode, body.Code)
	assert.Equal(t, "request timeout", body.Message)
}

func TestRequestDeadlineWriterStopsCallbackAfterOutputOwnershipEnds(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, context.Context)
	}{
		{
			name: "generic render",
			run: func(t *testing.T, ctx context.Context) {
				render := jsonRender{Context: ctx, Data: gin.H{HTTPReturnCode: 0}}
				require.NoError(t, render.Render(httptest.NewRecorder()))
			},
		},
		{
			name: "direct streaming render",
			run: func(t *testing.T, ctx context.Context) {
				render := jsonRender{Context: ctx, Data: gin.H{
					HTTPReturnCode: 0,
					HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
				}}
				require.NoError(t, render.Render(httptest.NewRecorder()))
			},
		},
		{
			name: "materialized commit",
			run: func(t *testing.T, ctx context.Context) {
				recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
				_, err := recorder.Write([]byte(`{"code":0}`))
				require.NoError(t, err)
				response := httptest.NewRecorder()
				gCtx, _ := gin.CreateTestContext(response)
				require.NoError(t, recorder.CommitTo(ctx, gCtx.Writer))
			},
		},
		{
			name: "deferred streaming commit",
			run: func(t *testing.T, ctx context.Context) {
				recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
				render := jsonRender{Context: ctx, Data: gin.H{
					HTTPReturnCode: 0,
					HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
				}}
				require.NoError(t, render.Render(recorder))
				response := httptest.NewRecorder()
				gCtx, _ := gin.CreateTestContext(response)
				require.NoError(t, recorder.CommitTo(ctx, gCtx.Writer))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newAfterFuncTrackingContext()
			test.run(t, ctx)
			assert.Equal(t, 1, ctx.started)
			assert.Equal(t, 1, ctx.stopped)
		})
	}
}

func TestTimeoutResponseRecorderRenderRegistrationRace(t *testing.T) {
	for iteration := 0; iteration < 500; iteration++ {
		recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
		responseRender := &blockingTestRender{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = recorder.DeferRender(responseRender)
		}()
		go func() {
			defer wg.Done()
			<-start
			recorder.CloseForTimeout()
		}()
		close(start)
		wg.Wait()

		assert.True(t, recorder.closed)
		assert.Nil(t, recorder.body)
		assert.Nil(t, recorder.render)
		_, err := recorder.Write([]byte("late"))
		require.Error(t, err)
	}
}

func TestTimeoutMiddlewareDiscardsLateStreamRenderer(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "5")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	rowSource := &testJSONRows{rows: []map[string]interface{}{{"id": 1}}}
	lateHandlerDone := make(chan struct{}, 1)
	router := gin.New()
	router.POST("/late-stream", timeoutMiddleware(func(c *gin.Context) {
		<-c.Request.Context().Done()
		HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: rowSource,
		})
		lateHandlerDone <- struct{}{}
	}))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/late-stream", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusRequestTimeout, response.Code)
	var timeoutBody ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &timeoutBody))
	assert.Equal(t, merr.TimeoutCode, timeoutBody.Code)
	assert.Equal(t, "request timeout", timeoutBody.Message)
	select {
	case <-lateHandlerDone:
	case <-time.After(time.Second):
		t.Fatal("late handler did not finish")
	}
	assert.Zero(t, rowSource.rowCalls)
	assert.NotContains(t, response.Body.String(), "\"id\"")
}

func TestTimeoutStreamPreservesStatusHeadersAndWriterSize(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, "1000")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key)
	})

	const traceID = "0123456789abcdef0123456789abcdef"
	rowSource := &testJSONRows{rows: []map[string]interface{}{{"id": 1}, {"id": 2}}}
	observedSize := -1
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Next()
		observedSize = c.Writer.Size()
	})
	router.POST("/stream", restfulSizeMiddleware(timeoutMiddleware(func(c *gin.Context) {
		c.Set("traceID", traceID)
		c.Header("X-Test", "stream")
		HTTPReturnStream(c, http.StatusAccepted, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: rowSource,
			HTTPReturnTopks: []int64{
				2,
			},
		})
	}), true))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/stream", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusAccepted, response.Code)
	assert.Equal(t, "stream", response.Header().Get("X-Test"))
	assert.Equal(t, traceID, response.Header().Get(HTTPHeaderMilvusTraceID))
	assert.Equal(t, response.Body.Len(), observedSize)
	assert.Equal(t, len(rowSource.rows), rowSource.rowCalls)
	var body struct {
		Code  int                      `json:"code"`
		Data  []map[string]interface{} `json:"data"`
		Topks []int64                  `json:"topks"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Zero(t, body.Code)
	assert.Len(t, body.Data, 2)
	assert.Equal(t, []int64{2}, body.Topks)
}

func TestTimeoutMiddlewarePreservesPanicPath(t *testing.T) {
	router := gin.New()
	router.Use(gin.RecoveryWithWriter(io.Discard))
	router.POST("/panic", timeoutMiddleware(func(*gin.Context) {
		panic("handler panic")
	}))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/panic", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusInternalServerError, response.Code)
	var body ReturnErrMsg
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, int32(http.StatusInternalServerError), body.Code)
}
