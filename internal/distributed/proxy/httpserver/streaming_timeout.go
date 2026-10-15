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
// Only cancellation callbacks touch transport deadlines concurrently. They are
// joined before the writer can escape ServeHTTP or Gin can reuse its context.
type streamingResponseWriter struct {
	gin.ResponseWriter
	ctx          context.Context
	controller   *http.ResponseController
	deadline     time.Time
	failure      error
	stopWrite    func() bool
	writeStopped chan struct{}
}

func (w *streamingResponseWriter) requestError() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	// The transport deadline can wake a body read before the context timer runs.
	// Do not commit a binding error (or success) after the same budget expired.
	if !time.Now().Before(w.deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (w *streamingResponseWriter) prepare() error {
	if w.failure != nil {
		return w.failure
	}
	if err := w.requestError(); err != nil {
		return err
	}
	if w.stopWrite == nil {
		// Arm only at first output. HTTP/2 deadlines set during backend work
		// could reset an uncommitted stream before we can send its timeout body.
		if err := w.controller.SetWriteDeadline(w.deadline); err != nil {
			w.failure = err // Unsupported transports fail closed, never unbounded.
			return err
		}
		w.writeStopped = make(chan struct{})
		w.stopWrite = context.AfterFunc(w.ctx, func() {
			_ = w.controller.SetWriteDeadline(time.Now())
			close(w.writeStopped)
		})
	}
	return w.requestError()
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

func (w *streamingResponseWriter) joinWrite() {
	if w.stopWrite != nil && !w.stopWrite() {
		<-w.writeStopped
	}
}

// protectBodyRead bounds body reads by the request deadline and interrupts them
// on early cancellation. The caller must run cleanup after response finalization.
func (w *streamingResponseWriter) protectBodyRead() (cleanup func(), err error) {
	if err := w.controller.SetReadDeadline(w.deadline); err != nil {
		return nil, err
	}

	callbackDone := make(chan struct{})
	stopCallback := context.AfterFunc(w.ctx, func() {
		// Parent cancellation may precede the configured deadline.
		_ = w.controller.SetReadDeadline(time.Now())
		close(callbackDone)
	})
	return func() {
		// Prevent a future callback, or wait for an already running one to finish.
		// No callback may change the deadline after this request exits.
		if !stopCallback() {
			<-callbackDone
		}
		// Keep the deadline for net/http's unread-body cleanup. The transport
		// resets it before HTTP/1 reuse or removes it when the HTTP/2 stream closes.
	}, nil
}

func streamingTimeoutMiddleware(handler gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Use one request deadline for body reads, handler work and response writes.
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
		writer := &streamingResponseWriter{
			ResponseWriter: realWriter,
			ctx:            ctx,
			controller:     http.NewResponseController(transport),
			deadline:       deadline,
		}
		c.Writer = writer
		var cleanupBodyRead func()
		defer func() {
			if cleanupBodyRead != nil {
				cleanupBodyRead()
			}
		}()

		// These actions run only after the deferred cleanup restores realWriter.
		abortResponse := func() {
			cancel()
			c.Abort()
			if state, ok := ctx.Value(streamingAbortKey{}).(*streamingAbortState); ok {
				state.aborted = true
				return
			}
			panic(http.ErrAbortHandler) // Caller must install StreamingHTTPHandler.
		}
		writeErrorResponse := func(status int, body gin.H) bool {
			// The request may already be expired. Give the complete error envelope
			// a bounded write window from server configuration, not Request-Timeout: 0.
			budget := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
			if err := writer.controller.SetWriteDeadline(time.Now().Add(budget)); err != nil {
				abortResponse()
				return false
			}
			HTTPAbortReturn(c, status, body)
			if err := writer.controller.Flush(); err != nil {
				abortResponse()
				return false
			}
			_ = writer.controller.SetWriteDeadline(time.Time{})
			return true
		}

		defer func() {
			panicValue := recover()

			// Finish pending output, then stop/join the deadline callback before
			// restoring the writer or attempting an independent error response.
			if panicValue == nil && writer.requestError() == nil && writer.failure == nil && writer.Written() {
				_ = writer.FlushError() // Observe final buffered socket errors here.
			}
			writer.joinWrite()
			c.Writer = realWriter

			// An ordinary handler panic can become a complete 500 only before
			// commitment. Afterwards, log it here and abort the partial response.
			if panicValue != nil && panicValue != http.ErrAbortHandler {
				if !writer.Written() {
					if writeErrorResponse(http.StatusInternalServerError, gin.H{HTTPReturnCode: http.StatusInternalServerError}) {
						panic(panicValue) // Let Gin Recovery log the original panic.
					}
					return
				}
				err := merr.WrapErrServiceInternalMsg("REST handler panicked after response commitment")
				c.Set(HTTPReturnCode, merr.Code(err))
				c.Set(HTTPReturnMessage, err.Error())
				_ = c.Error(err).SetType(gin.ErrorTypePrivate)
				mlog.Error(ctx, "REST handler panicked after response commitment", mlog.Err(err), mlog.Any("panic", panicValue), mlog.String("panic_stack", string(debug.Stack())))
				abortResponse()
				return
			}

			requestErr := writer.requestError()
			if requestErr != nil {
				c.Set(HTTPReturnCode, merr.TimeoutCode)
				c.Set(HTTPReturnMessage, "request timeout")
			}
			switch {
			case panicValue != nil || writer.failure != nil:
				// An explicit abort or a failed transport cannot carry an error body.
				abortResponse()
			case requestErr != nil:
				if writer.Written() {
					abortResponse() // Already committed: never append an error document.
				} else {
					writeErrorResponse(http.StatusRequestTimeout, gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
				}
			default:
				// Successful response: do not leave its deadline on a reused connection.
				if writer.stopWrite != nil {
					_ = writer.controller.SetWriteDeadline(time.Time{})
				}
			}
		}()
		if c.Request.Body != nil && c.Request.Body != http.NoBody {
			var err error
			cleanupBodyRead, err = writer.protectBodyRead()
			if err != nil {
				writer.failure = err // Do not enter a potentially unbounded body read.
				return
			}
		}
		handler(c)
	}
}
