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

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

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
	p.pool.Put(buf)
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

func (w *timeoutResponseRecorder) CommitTo(realWriter gin.ResponseWriter) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.body == nil {
		return merr.WrapErrServiceInternalMsg("response writer closed")
	}

	dst := realWriter.Header()
	for k, vv := range w.headers {
		dst[k] = append([]string(nil), vv...)
	}
	realWriter.WriteHeader(w.status)
	if w.body.Len() == 0 {
		if w.written() || w.status != http.StatusOK {
			realWriter.WriteHeaderNow()
		}
		return nil
	}
	_, err := realWriter.Write(w.body.Bytes())
	return err
}

func (w *timeoutResponseRecorder) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.body != nil {
		w.body.Reset()
		w.body = nil
	}
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
			if err := recorder.CommitTo(realWriter); err != nil {
				mlog.Warn(context.TODO(), "failed to write response body", zap.Error(err))
				recorder.Close()
				bufPool.Put(buffer)
				return
			}
			recorder.Close()
			bufPool.Put(buffer)

		case <-timer.C:
			cancel()
			gCtx.Abort()
			gCtx.Set(HTTPReturnCode, merr.TimeoutCode)
			gCtx.Set(HTTPReturnMessage, "request timeout")
			recorder.CloseForTimeout()
			bufPool.Put(buffer)

			realWriter.Header().Set("Content-Type", "application/json; charset=utf-8")
			if traceID, ok := getTraceID(gCtx); ok {
				setTraceIDHeaderTo(realWriter.Header(), traceID)
			}
			realWriter.WriteHeader(http.StatusRequestTimeout)
			body, _ := json.Marshal(gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
			realWriter.Write(body)
		}
	}
}
