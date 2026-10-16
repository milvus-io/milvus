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

func newStreamingResponseWriter(realWriter gin.ResponseWriter, ctx context.Context) *streamingResponseWriter {
	// Resolve wrappers on the serving goroutine; callbacks never access Gin.
	// In particular, Flush must reach the transport's error-returning method.
	var transport http.ResponseWriter = realWriter
	for {
		unwrapper, ok := transport.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		transport = unwrapper.Unwrap()
	}
	deadline, _ := ctx.Deadline()
	return &streamingResponseWriter{
		ResponseWriter: realWriter,
		ctx:            ctx,
		controller:     http.NewResponseController(transport),
		deadline:       deadline,
	}
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
		writer := newStreamingResponseWriter(c.Writer, ctx)
		var result streamingWorkerResult
		var cleanupBodyRead func()
		defer func() {
			if cleanupBodyRead != nil {
				cleanupBodyRead()
			}
		}()
		defer func() {
			writer.finish(c, cancel, recover(), result.stack)
		}()

		if c.Request.Body != nil && c.Request.Body != http.NoBody {
			var err error
			cleanupBodyRead, err = writer.protectBodyRead()
			if err != nil {
				writer.failure = err // Do not enter a potentially unbounded body read.
				return
			}
		}

		// Keep the real writer on c; only the detached worker context replaces it.
		result = writer.serveWorker(c, handler)
		if result.panicValue != nil {
			panic(result.panicValue)
		}
	}
}

// finish runs only on the serving goroutine, before body-read cleanup and cancel.
// Before commitment it can send an error envelope; afterwards it must abort.
func (w *streamingResponseWriter) finish(c *gin.Context, cancel context.CancelFunc, panicValue any, workerPanicStack []byte) {
	// Finish pending output, then stop/join the deadline callback before
	// attempting an independent error response.
	if panicValue == nil && w.requestError() == nil && w.failure == nil && w.Written() {
		_ = w.FlushError() // Observe final buffered socket errors here.
	}
	w.joinWrite()

	// An ordinary handler panic can become a complete 500 only before
	// commitment. Afterwards, log it here and abort the partial response.
	if panicValue != nil && panicValue != http.ErrAbortHandler {
		if !w.Written() {
			if w.writeErrorResponse(c, cancel, http.StatusInternalServerError, gin.H{HTTPReturnCode: http.StatusInternalServerError}) {
				panic(panicValue) // Let Gin Recovery log the original panic.
			}
			return
		}
		err := merr.WrapErrServiceInternalMsg("REST handler panicked after response commitment")
		c.Set(HTTPReturnCode, merr.Code(err))
		c.Set(HTTPReturnMessage, err.Error())
		_ = c.Error(err).SetType(gin.ErrorTypePrivate)
		if workerPanicStack == nil {
			workerPanicStack = debug.Stack()
		}
		mlog.Error(w.ctx, "REST handler panicked after response commitment", mlog.Err(err), mlog.Any("panic", panicValue), mlog.String("panic_stack", string(workerPanicStack)))
		abortStreamingResponse(c, cancel)
		return
	}

	requestErr := w.requestError()
	if requestErr != nil {
		c.Set(HTTPReturnCode, merr.TimeoutCode)
		c.Set(HTTPReturnMessage, "request timeout")
	}
	switch {
	case panicValue != nil || w.failure != nil:
		// An explicit abort or a failed transport cannot carry an error body.
		abortStreamingResponse(c, cancel)
	case requestErr != nil:
		if w.Written() {
			abortStreamingResponse(c, cancel) // Already committed: never append an error document.
		} else {
			w.writeErrorResponse(c, cancel, http.StatusRequestTimeout, gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
		}
	default:
		// Successful response: do not leave its deadline on a reused connection.
		if w.stopWrite != nil {
			_ = w.controller.SetWriteDeadline(time.Time{})
		}
	}
}

func abortStreamingResponse(c *gin.Context, cancel context.CancelFunc) {
	cancel()
	c.Abort()
	if state, ok := c.Request.Context().Value(streamingAbortKey{}).(*streamingAbortState); ok {
		state.aborted = true
		return
	}
	panic(http.ErrAbortHandler) // Caller must install StreamingHTTPHandler.
}

func (w *streamingResponseWriter) writeErrorResponse(c *gin.Context, cancel context.CancelFunc, status int, body gin.H) bool {
	// The request may already be expired. Give the complete error envelope
	// a bounded write window from server configuration, not Request-Timeout: 0.
	budget := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
	if err := w.controller.SetWriteDeadline(time.Now().Add(budget)); err != nil {
		abortStreamingResponse(c, cancel)
		return false
	}
	HTTPAbortReturn(c, status, body)
	if err := w.controller.Flush(); err != nil {
		abortStreamingResponse(c, cancel)
		return false
	}
	_ = w.controller.SetWriteDeadline(time.Time{})
	return true
}
