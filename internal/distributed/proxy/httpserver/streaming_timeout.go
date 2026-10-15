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
	"context"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type streamingAbortKey struct{}

type streamingAbortState struct{ aborted bool }

// StreamingHTTPHandler aborts a failed committed response after Gin's normal
// middleware unwind. Gin Recovery must not swallow net/http's abort signal.
// The state belongs to this ServeHTTP invocation and is never shared by streams.
func StreamingHTTPHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &streamingAbortState{}
		handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), streamingAbortKey{}, state)))
		if state.aborted {
			panic(http.ErrAbortHandler)
		}
	})
}

// streamingResponseWriter has one application owner: the serving goroutine.
// Only its cancellation callback touches the transport deadline. That callback
// is joined before the writer can escape ServeHTTP or Gin can reuse its context.
type streamingResponseWriter struct {
	gin.ResponseWriter
	ctx        context.Context
	controller *http.ResponseController
	deadline   time.Time
	failure    error
	stop       func() bool
	stopped    chan struct{}
}

func (w *streamingResponseWriter) prepare() error {
	if w.failure != nil {
		return w.failure
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.stop == nil {
		// Arm only at first output. HTTP/2 deadlines set during backend work
		// could reset an uncommitted stream before we can send its timeout body.
		if err := w.controller.SetWriteDeadline(w.deadline); err != nil {
			w.failure = err // Unsupported transports fail closed, never unbounded.
			return err
		}
		w.stopped = make(chan struct{})
		w.stop = context.AfterFunc(w.ctx, func() {
			_ = w.controller.SetWriteDeadline(time.Now())
			close(w.stopped)
		})
	}
	return w.ctx.Err()
}

func (w *streamingResponseWriter) Write(data []byte) (int, error) {
	if err := w.prepare(); err != nil {
		return 0, err
	}
	n, err := w.ResponseWriter.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.failure = err
	return n, err
}

func (w *streamingResponseWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

func (w *streamingResponseWriter) WriteHeaderNow() {
	if err := w.prepare(); err != nil {
		panic(http.ErrAbortHandler)
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *streamingResponseWriter) FlushError() error {
	if err := w.prepare(); err != nil {
		return err
	}
	w.ResponseWriter.WriteHeaderNow()
	w.failure = w.controller.Flush()
	return w.failure
}

func (w *streamingResponseWriter) Flush() {
	if err := w.FlushError(); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (w *streamingResponseWriter) join() {
	if w.stop != nil && !w.stop() {
		<-w.stopped
	}
}

func streamingTimeoutMiddleware(handler gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		timeout := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
		if value := c.Request.Header.Get(mhttp.HTTPHeaderRequestTimeout); value != "" {
			seconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
					HTTPReturnMessage: merr.WrapErrParameterInvalidMsg("%s parse failed, err: %s", mhttp.HTTPHeaderRequestTimeout, err.Error()).Error(),
				})
				return
			}
			timeout = time.Duration(seconds) * time.Second
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		realWriter := c.Writer
		// Resolve wrappers on the serving goroutine; callbacks never access Gin.
		var transport http.ResponseWriter = realWriter
		for {
			unwrapper, ok := transport.(interface{ Unwrap() http.ResponseWriter })
			if !ok {
				break
			}
			transport = unwrapper.Unwrap()
		}
		deadline, _ := ctx.Deadline()
		writer := &streamingResponseWriter{ResponseWriter: realWriter, ctx: ctx, controller: http.NewResponseController(transport), deadline: deadline}
		c.Writer = writer
		defer func() {
			p := recover()
			if p == nil && ctx.Err() == nil && writer.failure == nil && writer.Written() {
				_ = writer.FlushError() // Observe final buffered socket errors here.
			}
			writer.join()
			c.Writer = realWriter
			abort := func() {
				cancel()
				c.Abort()
				if state, ok := ctx.Value(streamingAbortKey{}).(*streamingAbortState); ok {
					state.aborted = true
					return
				}
				panic(http.ErrAbortHandler) // Caller must install StreamingHTTPHandler.
			}
			writeError := func(status int, body gin.H) bool {
				// The request may already be expired. Bound this complete error
				// envelope by the configured budget, not Request-Timeout: 0.
				budget := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
				if err := writer.controller.SetWriteDeadline(time.Now().Add(budget)); err != nil {
					abort()
					return false
				}
				HTTPAbortReturn(c, status, body)
				if err := writer.controller.Flush(); err != nil {
					abort()
					return false
				}
				_ = writer.controller.SetWriteDeadline(time.Time{})
				return true
			}
			if p != nil && p != http.ErrAbortHandler && !writer.Written() {
				if writeError(http.StatusInternalServerError, gin.H{HTTPReturnCode: http.StatusInternalServerError}) {
					panic(p) // Preserve Gin panic logging after a complete envelope.
				}
				return
			}
			if p != nil && p != http.ErrAbortHandler {
				// Committed panics cannot pass through Gin Recovery, which would
				// consume the transport abort. Preserve their original diagnostics
				// and mark the request as a system failure before normal unwind.
				err := merr.WrapErrServiceInternalMsg("REST handler panicked after response commitment")
				c.Set(HTTPReturnCode, merr.Code(err))
				c.Set(HTTPReturnMessage, err.Error())
				_ = c.Error(err).SetType(gin.ErrorTypePrivate)
				mlog.Error(ctx, "REST handler panicked after response commitment", mlog.Err(err), mlog.Any("panic", p), mlog.String("panic_stack", string(debug.Stack())))
			} else if ctx.Err() != nil {
				c.Set(HTTPReturnCode, merr.TimeoutCode)
				c.Set(HTTPReturnMessage, "request timeout")
			}
			if p != nil || writer.failure != nil || (ctx.Err() != nil && writer.Written()) {
				abort()
				return
			}
			if ctx.Err() != nil {
				writeError(http.StatusRequestTimeout, gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
				return
			}
			if writer.stop != nil {
				_ = writer.controller.SetWriteDeadline(time.Time{})
			}
		}()
		handler(c)
	}
}
