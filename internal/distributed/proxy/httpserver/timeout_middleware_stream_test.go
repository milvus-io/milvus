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
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

func TestStreamIdleControllerLifecycle(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		parent := context.Background()
		ctx, controller := withStreamIdleTimeout(parent, 0)
		assert.Equal(t, parent, ctx)
		assert.Nil(t, controller)
	})

	t.Run("waits for stream start", func(t *testing.T) {
		ctx, controller := withStreamIdleTimeout(context.Background(), time.Hour)
		defer controller.stop()

		controller.mu.Lock()
		assert.Nil(t, controller.timer)
		controller.mu.Unlock()
		assert.NoError(t, ctx.Err())

		controller.arm()
		controller.mu.Lock()
		assert.NotNil(t, controller.timer)
		controller.mu.Unlock()
	})

	t.Run("rearms after progress", func(t *testing.T) {
		_, controller := withStreamIdleTimeout(context.Background(), time.Hour)
		defer controller.stop()

		controller.arm()
		controller.mu.Lock()
		firstDeadline := controller.deadline
		firstTimer := controller.timer
		controller.mu.Unlock()

		time.Sleep(time.Millisecond)
		controller.arm()
		controller.mu.Lock()
		secondDeadline := controller.deadline
		secondTimer := controller.timer
		controller.mu.Unlock()

		assert.Same(t, firstTimer, secondTimer)
		assert.True(t, secondDeadline.After(firstDeadline))
	})

	t.Run("expires with timeout cause", func(t *testing.T) {
		ctx, controller := withStreamIdleTimeout(context.Background(), 10*time.Millisecond)
		defer controller.stop()
		controller.arm()

		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("stream idle timer did not expire")
		}
		require.ErrorIs(t, context.Cause(ctx), errStreamIdleTimeout)
		require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
	})

	t.Run("publishes cancellation before stopped state", func(t *testing.T) {
		ctx, controller := withStreamIdleTimeout(context.Background(), time.Hour)
		controller.deadline = time.Now().Add(-time.Second)

		originalCancel := controller.cancel
		cancelStarted := make(chan struct{})
		releaseCancel := make(chan struct{})
		released := false
		defer func() {
			if !released {
				close(releaseCancel)
			}
		}()
		controller.cancel = func(cause error) {
			close(cancelStarted)
			<-releaseCancel
			originalCancel(cause)
		}

		expireDone := make(chan struct{})
		go func() {
			controller.expire()
			close(expireDone)
		}()
		<-cancelStarted

		armDone := make(chan time.Time, 1)
		go func() { armDone <- controller.arm() }()
		select {
		case <-armDone:
			t.Fatal("arm observed stopped state before timeout cancellation")
		case <-time.After(20 * time.Millisecond):
		}

		close(releaseCancel)
		released = true
		<-expireDone
		assert.True(t, (<-armDone).IsZero())
		require.ErrorIs(t, context.Cause(ctx), errStreamIdleTimeout)
	})

	t.Run("stop prevents expiry", func(t *testing.T) {
		ctx, controller := withStreamIdleTimeout(context.Background(), time.Hour)
		controller.arm()
		controller.stop()
		controller.expire()

		assert.NoError(t, ctx.Err())
	})
}

func TestTimeoutMiddlewareSnapshotsStreamIdleTimeoutPerRequest(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.HTTPCfg.RequestTimeoutMs.Key, "1000"))
	require.NoError(t, params.Save(params.HTTPCfg.StreamIdleTimeout.Key, "1h"))
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.RequestTimeoutMs.Key)
		params.Reset(params.HTTPCfg.StreamIdleTimeout.Key)
	})

	observed := make(chan *streamIdleController, 2)
	release := make(chan struct{})
	router := gin.New()
	router.GET("/snapshot", timeoutMiddleware(func(c *gin.Context) {
		observed <- streamIdleControllerFromContext(c.Request.Context())
		<-release
		c.Status(http.StatusNoContent)
	}))

	firstResponse := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		router.ServeHTTP(firstResponse, httptest.NewRequest(http.MethodGet, "/snapshot", nil))
		close(firstDone)
	}()
	firstController := <-observed
	require.NotNil(t, firstController)
	assert.Equal(t, time.Hour, firstController.timeout)

	require.NoError(t, params.Save(params.HTTPCfg.StreamIdleTimeout.Key, "0s"))
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not finish")
	}
	assert.Equal(t, http.StatusNoContent, firstResponse.Code)

	secondResponse := httptest.NewRecorder()
	router.ServeHTTP(secondResponse, httptest.NewRequest(http.MethodGet, "/snapshot", nil))
	assert.Nil(t, <-observed)
	assert.Equal(t, http.StatusNoContent, secondResponse.Code)
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

type singleConnListener struct {
	conn      net.Conn
	mu        sync.Mutex
	accepted  bool
	closed    chan struct{}
	closeOnce sync.Once
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

type failingJSONRows struct {
	err error
}

func (rows *failingJSONRows) Len() int64 {
	return 1
}

func (rows *failingJSONRows) Row(int64) (map[string]interface{}, error) {
	return nil, rows.err
}

type failingNetworkResponseWriter struct {
	header    http.Header
	status    int
	writes    int
	attempted bytes.Buffer
	err       error
}

func (writer *failingNetworkResponseWriter) Header() http.Header {
	return writer.header
}

func (writer *failingNetworkResponseWriter) WriteHeader(status int) {
	writer.status = status
}

func (writer *failingNetworkResponseWriter) Write(data []byte) (int, error) {
	writer.writes++
	_, _ = writer.attempted.Write(data)
	return 0, writer.err
}

func (w *firstWriteSignalWriter) Write(data []byte) (int, error) {
	w.once.Do(func() {
		close(w.firstWrite)
	})
	return w.ResponseRecorder.Write(data)
}

func (listener *singleConnListener) Accept() (net.Conn, error) {
	listener.mu.Lock()
	if !listener.accepted {
		listener.accepted = true
		conn := listener.conn
		listener.mu.Unlock()
		return conn, nil
	}
	listener.mu.Unlock()
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *singleConnListener) Close() error {
	listener.closeOnce.Do(func() {
		close(listener.closed)
	})
	return nil
}

func (listener *singleConnListener) Addr() net.Addr {
	return listener.conn.LocalAddr()
}

func TestTimeoutMiddlewareReturnsInvalidSearchResultForUnwrittenDeferredRenderError(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.HTTPCfg.RequestTimeoutMs.Key, "1000"))
	require.NoError(t, params.Save(params.HTTPCfg.StreamIdleTimeout.Key, "0s"))
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.RequestTimeoutMs.Key)
		params.Reset(params.HTTPCfg.StreamIdleTimeout.Key)
	})

	for _, test := range []struct {
		name    string
		newRows func(error) jsonRowSource
		err     error
	}{
		{
			name: "row materialization",
			newRows: func(err error) jsonRowSource {
				return &failingJSONRows{err: err}
			},
			err: errors.New("row materialization failed"),
		},
		{
			name: "row encoding",
			newRows: func(err error) jsonRowSource {
				return &testJSONRows{rows: []map[string]interface{}{{
					"invalid": failingJSONValue{err: err},
				}}}
			},
			err: errors.New("row encoding failed"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/render-error", timeoutMiddleware(func(c *gin.Context) {
				if err := HTTPReturnStream(c, http.StatusOK, gin.H{
					HTTPReturnCode: 0,
					HTTPReturnData: test.newRows(test.err),
				}); err != nil {
					panic(err)
				}
			}))

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/render-error", nil))

			assert.Equal(t, http.StatusOK, response.Code)
			var body ReturnErrMsg
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
			assert.Equal(t, merr.Code(merr.ErrInvalidSearchResult), body.Code)
			assert.Contains(t, body.Message, test.err.Error())
			assert.NotContains(t, response.Body.String(), `"data"`)
		})
	}
}

func TestTimeoutMiddlewareDoesNotRelabelCommittedConnectionError(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.HTTPCfg.RequestTimeoutMs.Key, "1000"))
	require.NoError(t, params.Save(params.HTTPCfg.StreamIdleTimeout.Key, "0s"))
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.RequestTimeoutMs.Key)
		params.Reset(params.HTTPCfg.StreamIdleTimeout.Key)
	})

	networkErr := errors.New("connection closed")
	writer := &failingNetworkResponseWriter{
		header: make(http.Header),
		err:    networkErr,
	}
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	failureCounter := metrics.RestfulStreamDeliveryFailure.WithLabelValues(
		nodeID,
		"/connection-error",
		streamTerminationCauseTransportError,
	)
	failureCountBefore := testutil.ToFloat64(failureCounter)
	t.Cleanup(func() {
		metrics.RestfulStreamDeliveryFailure.DeleteLabelValues(
			nodeID,
			"/connection-error",
			streamTerminationCauseTransportError,
		)
	})
	var termination interface{}
	var terminationCause interface{}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Next()
		termination, _ = c.Get(ContextStreamTermination)
		terminationCause, _ = c.Get(ContextStreamTerminationCause)
	})
	router.POST("/connection-error", timeoutMiddleware(func(c *gin.Context) {
		if err := HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
		}); err != nil {
			panic(err)
		}
	}))

	router.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/connection-error", nil))

	assert.Equal(t, http.StatusOK, writer.status)
	assert.Equal(t, 1, writer.writes)
	assert.Contains(t, writer.attempted.String(), `"code":0`)
	assert.NotContains(t, writer.attempted.String(), merr.ErrInvalidSearchResult.Error())
	assert.Equal(t, streamTerminationFailed, termination)
	assert.Equal(t, streamTerminationCauseTransportError, terminationCause)
	assert.Equal(t, failureCountBefore+1, testutil.ToFloat64(failureCounter))
}

func TestStreamTerminationCause(t *testing.T) {
	renderErr := errors.New("render failed")
	transportErr := errors.New("transport failed")
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{name: "idle timeout", err: errStreamIdleTimeout, expected: streamTerminationCauseIdleTimeout},
		{name: "request timeout", err: context.DeadlineExceeded, expected: streamTerminationCauseRequestTimeout},
		{name: "client cancel", err: context.Canceled, expected: streamTerminationCauseClientCancel},
		{name: "transport", err: &responseTransportError{cause: transportErr}, expected: streamTerminationCauseTransportError},
		{name: "transport deadline", err: &responseTransportError{cause: context.DeadlineExceeded}, expected: streamTerminationCauseTransportError},
		{name: "transport inside renderer", err: &deferredRenderError{cause: &responseTransportError{cause: transportErr}}, expected: streamTerminationCauseTransportError},
		{name: "renderer", err: &deferredRenderError{cause: renderErr}, expected: streamTerminationCauseRenderError},
		{name: "unknown post-commit", err: errors.New("unknown"), expected: streamTerminationCauseTransportError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, streamTerminationCause(test.err))
		})
	}
}

type requestTimeoutJSONRows struct {
	ctx         context.Context
	requestDone <-chan struct{}
	rowCalls    int
}

type streamIdleJSONRows struct {
	ctx      context.Context
	rowCalls int
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

func (rows *streamIdleJSONRows) Len() int64 {
	return 2
}

func (rows *streamIdleJSONRows) Row(index int64) (map[string]interface{}, error) {
	rows.rowCalls++
	if index == 0 {
		return map[string]interface{}{"value": string(bytes.Repeat([]byte{'x'}, streamingJSONBufferSize*2))}, nil
	}
	<-rows.ctx.Done()
	return nil, responseContextError(rows.ctx)
}

func TestStreamIdleTimeoutAfterCommitReturnsPartialJSON(t *testing.T) {
	requestContext, controller := withStreamIdleTimeout(context.Background(), 50*time.Millisecond)
	defer controller.stop()
	rows := &streamIdleJSONRows{ctx: requestContext}
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
		t.Fatal("stream response was not committed")
	}
	select {
	case err := <-commitDone:
		require.ErrorIs(t, err, errStreamIdleTimeout)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("stream idle timeout did not stop rendering")
	}

	assert.Equal(t, http.StatusOK, responseWriter.Code)
	assert.Equal(t, 2, rows.rowCalls)
	assert.Contains(t, responseWriter.Body.String(), `"data":[`)
	var body map[string]interface{}
	require.Error(t, json.Unmarshal(responseWriter.Body.Bytes(), &body))
}

func TestStreamIdleTimeoutInterruptsFirstBlockedOutput(t *testing.T) {
	for _, serverWriteTimeout := range []time.Duration{0, 2 * time.Minute} {
		serverWriteTimeout := serverWriteTimeout
		t.Run(serverWriteTimeout.String(), func(t *testing.T) {
			requestContext, controller := withStreamIdleTimeout(context.Background(), 50*time.Millisecond)
			defer controller.stop()
			writer := &blockingDeadlineWriter{
				header:           make(http.Header),
				operationStarted: make(chan struct{}),
				deadlineSet:      make(chan time.Time, 1),
				releaseOperation: make(chan struct{}),
			}
			t.Cleanup(func() {
				writer.releaseOnce.Do(func() { close(writer.releaseOperation) })
			})
			renderDone := make(chan error, 1)
			go func() {
				renderDone <- (jsonRender{
					Context: withServerWriteTimeout(requestContext, serverWriteTimeout),
					Data: gin.H{
						HTTPReturnCode: 0,
						HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
					},
				}).Render(writer)
			}()
			select {
			case <-writer.operationStarted:
			case <-time.After(time.Second):
				t.Fatal("output operation did not start")
			}
			select {
			case deadline := <-writer.deadlineSet:
				assert.WithinDuration(t, time.Now(), deadline, 250*time.Millisecond)
			case <-time.After(time.Second):
				t.Fatal("stream idle timeout did not interrupt active output")
			}
			select {
			case err := <-renderDone:
				require.ErrorIs(t, err, errStreamIdleTimeout)
			case <-time.After(time.Second):
				t.Fatal("output remained blocked after stream idle timeout")
			}
		})
	}
}

func TestSuccessfulStreamingRenderStopsStreamIdleTimer(t *testing.T) {
	requestContext, controller := withStreamIdleTimeout(context.Background(), time.Hour)
	response := httptest.NewRecorder()
	require.NoError(t, (jsonRender{Context: requestContext, Data: gin.H{
		HTTPReturnCode: 0,
		HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{
			"value": string(bytes.Repeat([]byte{'x'}, streamingJSONBufferSize*2)),
		}}},
	}}).Render(response))

	controller.mu.Lock()
	stopped := controller.stopped
	timerStarted := controller.timer != nil
	controller.mu.Unlock()
	assert.True(t, timerStarted)
	assert.True(t, stopped)
	controller.expire()
	assert.NoError(t, requestContext.Err())
}

func TestStreamIdleDeadlineBoundsHTTP1ResponseFinalization(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.HTTPCfg.RequestTimeoutMs.Key, "2000"))
	require.NoError(t, params.Save(params.HTTPCfg.StreamIdleTimeout.Key, "100ms"))
	t.Cleanup(func() {
		params.Reset(params.HTTPCfg.RequestTimeoutMs.Key)
		params.Reset(params.HTTPCfg.StreamIdleTimeout.Key)
	})

	serverConn, clientConn := net.Pipe()
	listener := &singleConnListener{
		conn:   serverConn,
		closed: make(chan struct{}),
	}
	handlerReturned := make(chan struct{})
	connectionClosed := make(chan struct{})
	var connectionClosedOnce sync.Once
	router := gin.New()
	router.Use(serverWriteTimeoutMiddleware(0))
	router.POST("/finalize", timeoutMiddleware(func(c *gin.Context) {
		if err := HTTPReturnStream(c, http.StatusOK, gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
		}); err != nil {
			panic(err)
		}
	}))
	server := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			router.ServeHTTP(writer, request)
			close(handlerReturned)
		}),
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				connectionClosedOnce.Do(func() { close(connectionClosed) })
			}
		},
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = server.Close()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
		}
	})

	_, err := io.WriteString(clientConn, "POST /finalize HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\n\r\n")
	require.NoError(t, err)
	select {
	case <-handlerReturned:
	case <-time.After(time.Second):
		t.Fatal("handler did not return before response finalization")
	}
	select {
	case <-connectionClosed:
	case <-time.After(time.Second):
		t.Fatal("stream idle deadline did not bound HTTP/1 response finalization")
	}
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

	t.Run("stream idle deadline remains for response finalization", func(t *testing.T) {
		requestCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		streamCtx, controller := withStreamIdleTimeout(requestCtx, 2*time.Second)
		writer := &deadlineTrackingWriter{header: make(http.Header)}
		start := time.Now()
		render := jsonRender{Context: withServerWriteTimeout(streamCtx, 0), Data: gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
		}}
		require.NoError(t, render.Render(writer))

		require.NotEmpty(t, writer.deadlines)
		lastDeadline := writer.deadlines[len(writer.deadlines)-1]
		assert.WithinDuration(t, start.Add(2*time.Second), lastDeadline, 500*time.Millisecond)
		requestDeadline, ok := requestCtx.Deadline()
		require.True(t, ok)
		assert.True(t, lastDeadline.Before(requestDeadline))
		controller.mu.Lock()
		stopped := controller.stopped
		controller.mu.Unlock()
		assert.True(t, stopped)
	})

	t.Run("native server timeout keeps deadline ownership", func(t *testing.T) {
		requestCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		streamCtx, controller := withStreamIdleTimeout(requestCtx, 2*time.Second)
		writer := &deadlineTrackingWriter{header: make(http.Header)}
		render := jsonRender{Context: withServerWriteTimeout(streamCtx, time.Minute), Data: gin.H{
			HTTPReturnCode: 0,
			HTTPReturnData: &testJSONRows{rows: []map[string]interface{}{{"id": 1}}},
		}}
		require.NoError(t, render.Render(writer))

		assert.Empty(t, writer.deadlines)
		controller.mu.Lock()
		stopped := controller.stopped
		controller.mu.Unlock()
		assert.True(t, stopped)
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
