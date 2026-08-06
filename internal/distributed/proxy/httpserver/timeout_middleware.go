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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// BufferPool represents a pool of buffers.
type BufferPool struct {
	pool sync.Pool
}

const maxPooledResponseBufferCapacity = 1024 * 1024

// Get returns a buffer from the buffer pool.
// If the pool is empty, a new buffer is created and returned.
func (p *BufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get()
	if buf == nil {
		return &bytes.Buffer{}
	}
	return buf.(*bytes.Buffer)
}

// Put adds a buffer back to the pool.
func (p *BufferPool) Put(buf *bytes.Buffer) {
	if !isReusableResponseBuffer(buf) {
		return
	}
	buf.Reset()
	p.pool.Put(buf)
}

func isReusableResponseBuffer(buf *bytes.Buffer) bool {
	return buf != nil && buf.Cap() <= maxPooledResponseBufferCapacity
}

// Timeout struct
type Timeout struct {
	handler gin.HandlerFunc
}

const timeoutRecorderNoWritten = -1

type timeoutResponseRecorder struct {
	body        *bytes.Buffer
	headers     http.Header
	mu          sync.Mutex
	closed      bool
	status      int
	size        int
	closeNotify chan bool
	render      render.Render
}

type timeoutResponseCommit struct {
	body    *bytes.Buffer
	headers http.Header
	status  int
	written bool
	render  render.Render
}

func newTimeoutResponseRecorder(buf *bytes.Buffer) *timeoutResponseRecorder {
	return &timeoutResponseRecorder{
		body:        buf,
		headers:     make(http.Header),
		status:      http.StatusOK,
		size:        timeoutRecorderNoWritten,
		closeNotify: make(chan bool),
	}
}

func (w *timeoutResponseRecorder) Header() http.Header {
	return w.headers
}

func (w *timeoutResponseRecorder) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.body == nil {
		return 0, merr.WrapErrServiceInternalMsg("response writer closed")
	}
	if w.render != nil {
		return 0, merr.WrapErrServiceInternalMsg("response renderer already deferred")
	}
	if !w.written() {
		w.size = 0
	}
	n, err := w.body.Write(data)
	w.size += n
	return n, err
}

func (w *timeoutResponseRecorder) WriteString(s string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.body == nil {
		return 0, merr.WrapErrServiceInternalMsg("response writer closed")
	}
	if w.render != nil {
		return 0, merr.WrapErrServiceInternalMsg("response renderer already deferred")
	}
	if !w.written() {
		w.size = 0
	}
	n, err := w.body.WriteString(s)
	w.size += n
	return n, err
}

func (w *timeoutResponseRecorder) WriteHeader(code int) {
	if code == -1 {
		return
	}
	checkWriteHeaderCode(code)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.written() {
		return
	}
	w.status = code
}

func (w *timeoutResponseRecorder) WriteHeaderNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.written() {
		return
	}
	w.size = 0
}

func (w *timeoutResponseRecorder) Status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (w *timeoutResponseRecorder) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *timeoutResponseRecorder) Written() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written()
}

func (w *timeoutResponseRecorder) Flush() {}

func (w *timeoutResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, merr.WrapErrServiceInternalMsg("response writer does not support hijack")
}

func (w *timeoutResponseRecorder) CloseNotify() <-chan bool {
	return w.closeNotify
}

func (w *timeoutResponseRecorder) Pusher() http.Pusher {
	return nil
}

func (w *timeoutResponseRecorder) DeferRender(responseRender render.Render) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.body == nil {
		return merr.WrapErrServiceInternalMsg("response writer closed")
	}
	if responseRender == nil {
		return merr.WrapErrServiceInternalMsg("response renderer is nil")
	}
	if w.render != nil || w.body.Len() != 0 {
		return merr.WrapErrServiceInternalMsg("response body already configured")
	}
	w.render = responseRender
	if !w.written() {
		w.size = 0
	}
	return nil
}

type requestDeadlineWriter struct {
	http.ResponseWriter
	ctx                context.Context
	controller         *http.ResponseController
	flushController    *http.ResponseController
	serverWriteTimeout time.Duration
	prepared           bool
	writeDeadlineSet   bool
	stopWriteInterrupt func() bool
	writeMu            sync.Mutex
	writeActive        bool
}

type serverWriteTimeoutContextKey struct{}

func serverWriteTimeoutMiddleware(writeTimeout time.Duration) gin.HandlerFunc {
	return func(gCtx *gin.Context) {
		ctx := withServerWriteTimeout(gCtx.Request.Context(), writeTimeout)
		gCtx.Request = gCtx.Request.WithContext(ctx)
		gCtx.Next()
	}
}

func withServerWriteTimeout(ctx context.Context, writeTimeout time.Duration) context.Context {
	return context.WithValue(renderContext(ctx), serverWriteTimeoutContextKey{}, writeTimeout)
}

func serverWriteTimeoutFromContext(ctx context.Context) time.Duration {
	writeTimeout, _ := renderContext(ctx).Value(serverWriteTimeoutContextKey{}).(time.Duration)
	return writeTimeout
}

func newRequestDeadlineWriter(ctx context.Context, writer http.ResponseWriter) *requestDeadlineWriter {
	if deadlineWriter, ok := writer.(*requestDeadlineWriter); ok {
		return deadlineWriter
	}
	ctx = renderContext(ctx)
	flushWriter := writer
	if _, ok := writer.(gin.ResponseWriter); ok {
		if unwrapper, ok := writer.(interface{ Unwrap() http.ResponseWriter }); ok {
			if unwrapped := unwrapper.Unwrap(); unwrapped != nil {
				flushWriter = unwrapped
			}
		}
	}
	return &requestDeadlineWriter{
		ResponseWriter:     writer,
		ctx:                ctx,
		controller:         http.NewResponseController(writer),
		flushController:    http.NewResponseController(flushWriter),
		serverWriteTimeout: serverWriteTimeoutFromContext(ctx),
	}
}

func (w *requestDeadlineWriter) prepare() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.prepared {
		return nil
	}
	w.prepared = true
	_, ok := w.ctx.Deadline()
	if !ok {
		return nil
	}
	w.stopWriteInterrupt = context.AfterFunc(w.ctx, func() {
		w.interruptActiveWrite()
	})
	// net/http already owns the connection or stream deadline when the startup
	// WriteTimeout snapshot is positive. Do not replace that deadline because it
	// may be earlier than the request deadline. Instead, if the request deadline
	// wins while a write is blocked, force the active write to expire immediately.
	// Runtime config changes must not alter this ownership for an existing server.
	if w.serverWriteTimeout > 0 {
		return w.ctx.Err()
	}
	// Keep the transport deadline untouched until the first output operation has
	// passed its final context check. This preserves the unwritten timeout fallback
	// if the request expires during preparation. The active-write callback still
	// interrupts a first output operation that blocks past the request deadline.
	return w.ctx.Err()
}

func (w *requestDeadlineWriter) stopDeadlineInterrupt() {
	if w.stopWriteInterrupt == nil {
		return
	}
	w.stopWriteInterrupt()
	w.stopWriteInterrupt = nil
}

func (w *requestDeadlineWriter) beginWrite() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if err := w.ctx.Err(); err != nil {
		return err
	}
	w.writeActive = true
	return nil
}

func (w *requestDeadlineWriter) endWrite() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.writeActive = false
	if w.serverWriteTimeout > 0 || w.writeDeadlineSet {
		return nil
	}
	deadline, ok := w.ctx.Deadline()
	if !ok {
		return nil
	}
	w.writeDeadlineSet = true
	w.stopDeadlineInterrupt()
	// Leave the request deadline in place until net/http finishes the response.
	// The server performs its final buffered flush after the handler returns and
	// then clears the HTTP/1 connection deadline or closes the HTTP/2 stream.
	if err := w.controller.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func (w *requestDeadlineWriter) interruptActiveWrite() {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if errors.Is(w.ctx.Err(), context.DeadlineExceeded) && w.writeActive {
		_ = w.controller.SetWriteDeadline(time.Now())
	}
}

func (w *requestDeadlineWriter) runOutput(operation func() error) (err error) {
	if err := w.prepare(); err != nil {
		return err
	}
	if err := w.beginWrite(); err != nil {
		return err
	}
	defer func() {
		deadlineErr := w.endWrite()
		if ctxErr := w.ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else if err == nil {
			err = deadlineErr
		}
	}()
	return operation()
}

func (w *requestDeadlineWriter) Write(data []byte) (int, error) {
	var n int
	err := w.runOutput(func() error {
		var err error
		n, err = w.ResponseWriter.Write(data)
		return err
	})
	return n, err
}

func (w *requestDeadlineWriter) FlushError() error {
	flushUnsupported := false
	err := w.runOutput(func() error {
		if writer, ok := w.ResponseWriter.(interface{ WriteHeaderNow() }); ok {
			writer.WriteHeaderNow()
		}
		err := w.flushController.Flush()
		flushUnsupported = errors.Is(err, http.ErrNotSupported)
		return err
	})
	if flushUnsupported {
		return nil
	}
	return err
}

func (w *requestDeadlineWriter) writeHeaderNow() error {
	return w.runOutput(func() error {
		if writer, ok := w.ResponseWriter.(interface{ WriteHeaderNow() }); ok {
			writer.WriteHeaderNow()
		}
		return nil
	})
}

func (w *requestDeadlineWriter) Flush() {
	_ = w.FlushError()
}

func (w *requestDeadlineWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *timeoutResponseRecorder) CommitTo(ctx context.Context, realWriter gin.ResponseWriter) error {
	ctx = renderContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	if w.closed || w.body == nil {
		w.mu.Unlock()
		return merr.WrapErrServiceInternalMsg("response writer closed")
	}
	commit := timeoutResponseCommit{
		body:    w.body,
		headers: w.headers.Clone(),
		status:  w.status,
		written: w.written(),
		render:  w.render,
	}
	// Detach recorder-owned state before any network I/O. Late handler writes
	// must fail immediately instead of blocking behind a slow client, and the
	// middleware remains the sole owner of the real response writer.
	w.body = nil
	w.render = nil
	w.closed = true
	w.mu.Unlock()

	dst := realWriter.Header()
	for k, vv := range commit.headers {
		dst[k] = append([]string(nil), vv...)
	}
	realWriter.WriteHeader(commit.status)
	deadlineWriter := newRequestDeadlineWriter(ctx, realWriter)
	defer deadlineWriter.stopDeadlineInterrupt()
	if commit.render != nil {
		return commit.render.Render(deadlineWriter)
	}
	if commit.body.Len() == 0 {
		if commit.written || commit.status != http.StatusOK {
			if err := deadlineWriter.writeHeaderNow(); err != nil {
				return err
			}
		}
		return nil
	}
	_, err := deadlineWriter.Write(commit.body.Bytes())
	return err
}

func (w *timeoutResponseRecorder) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.body != nil {
		w.body.Reset()
		w.body = nil
	}
	w.render = nil
	w.closed = true
}

func (w *timeoutResponseRecorder) CloseForTimeout() {
	w.Close()
}

func (w *timeoutResponseRecorder) written() bool {
	return w.size != timeoutRecorderNoWritten
}

func checkWriteHeaderCode(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid http status code: %d", code))
	}
}

var timeoutContextKeysToPropagate = []string{
	HTTPReturnCode,
	HTTPReturnMessage,
	ContextRequest,
	ContextResponse,
	"traceID",
}

func propagateTimeoutContextKeys(dst *gin.Context, src *gin.Context) {
	for _, key := range timeoutContextKeysToPropagate {
		if value, ok := src.Get(key); ok {
			dst.Set(key, value)
		}
	}
}

func writeRequestTimeout(gCtx *gin.Context, realWriter gin.ResponseWriter) {
	gCtx.Abort()
	gCtx.Set(HTTPReturnCode, merr.TimeoutCode)
	gCtx.Set(HTTPReturnMessage, "request timeout")

	realWriter.Header().Set("Content-Type", "application/json; charset=utf-8")
	if traceID, ok := getTraceID(gCtx); ok {
		setTraceIDHeaderTo(realWriter.Header(), traceID)
	}
	realWriter.WriteHeader(http.StatusRequestTimeout)
	body, _ := json.Marshal(gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
	realWriter.Write(body)
}

func timeoutMiddleware(handler gin.HandlerFunc) gin.HandlerFunc {
	timeoutHandler := &Timeout{
		handler: handler,
	}
	bufPool := &BufferPool{}
	return func(gCtx *gin.Context) {
		timeout := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
		requestTimeout := gCtx.Request.Header.Get(mhttp.HTTPHeaderRequestTimeout)
		if requestTimeout != "" {
			timeoutSecond, err := strconv.ParseInt(requestTimeout, 10, 64)
			if err != nil {
				HTTPAbortReturn(gCtx, http.StatusOK, gin.H{
					mhttp.HTTPReturnCode: merr.Code(merr.ErrParameterInvalid),
					mhttp.HTTPReturnMessage: merr.WrapErrParameterInvalidMsg(
						"%s parse failed, err: %s",
						mhttp.HTTPHeaderRequestTimeout,
						err.Error(),
					).Error(),
				})
				return
			}
			timeout = time.Duration(timeoutSecond) * time.Second
		}
		topCtx, cancel := context.WithTimeout(gCtx.Request.Context(), timeout)
		defer cancel()
		req := gCtx.Request.WithContext(topCtx)
		gCtx.Request = req

		finish := make(chan struct{}, 1)
		panicChan := make(chan interface{}, 1)

		realWriter := gCtx.Writer
		buffer := bufPool.Get()
		buffer.Reset()
		recorder := newTimeoutResponseRecorder(buffer)
		handlerCtx := gCtx.Copy()
		handlerCtx.Request = req
		handlerCtx.Writer = recorder

		go func() {
			defer func() {
				if p := recover(); p != nil {
					panicChan <- p
				}
			}()
			timeoutHandler.handler(handlerCtx)
			finish <- struct{}{}
		}()

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case p := <-panicChan:
			recorder.Close()
			bufPool.Put(buffer)
			gCtx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{mhttp.HTTPReturnCode: http.StatusInternalServerError})
			panic(p)

		case <-finish:
			propagateTimeoutContextKeys(gCtx, handlerCtx)
			if handlerCtx.IsAborted() {
				gCtx.Abort()
			}
			gCtx.Next()
			commitErr := func() error {
				defer bufPool.Put(buffer)
				defer recorder.Close()
				return recorder.CommitTo(topCtx, realWriter)
			}()
			if commitErr != nil {
				if errors.Is(commitErr, context.DeadlineExceeded) && !realWriter.Written() {
					writeRequestTimeout(gCtx, realWriter)
					return
				}
				mlog.Warn(gCtx.Request.Context(), "failed to write response body", mlog.Err(commitErr))
				return
			}

		case <-timer.C:
			cancel()
			recorder.CloseForTimeout()
			bufPool.Put(buffer)
			writeRequestTimeout(gCtx, realWriter)
		}
	}
}
