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
	"time"

	"github.com/gin-gonic/gin"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type dqlStreamKey struct{}

type dqlStreamState struct {
	aborted bool // Owned only by the HTTP handler goroutine, never the worker.
}

// DQLStreamingHTTPHandler must wrap Gin, outside its Recovery middleware.
// After response commitment, a failed DQL response must end as a transport failure,
// not as a successful HTTP response containing truncated JSON.
func DQLStreamingHTTPHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &dqlStreamState{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), dqlStreamKey{}, state)))
		if state.aborted {
			panic(http.ErrAbortHandler)
		}
	})
}

// The worker owns this writer and has no reference to the real response writer.
// Multi-batch flat results are retained for encoding by the HTTP handler after
// the worker finishes. Single-batch and non-row responses use the recorder.
type dqlResponseWriter struct {
	*timeoutResponseRecorder
	ctx            context.Context
	encodeComplete bool
	result         gin.H  // Read by the HTTP handler only after worker completion.
	encoded        []byte // Likewise, only read after worker completion.
	renderErr      error  // Likewise, only read after worker completion.
}

func newDQLResponseWriter(ctx context.Context, buffer *bytes.Buffer) *dqlResponseWriter {
	return &dqlResponseWriter{
		timeoutResponseRecorder: newTimeoutResponseRecorder(buffer),
		ctx:                     ctx,
	}
}

func (w *dqlResponseWriter) renderDQLResult(data any) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.encodeComplete {
		w.encoded, w.renderErr = json.Marshal(data)
		return w.renderErr
	}
	if result, ok := data.(gin.H); ok {
		if rows, ok := result[HTTPReturnData].([]map[string]interface{}); ok && len(rows) > dqlJSONBatchRows {
			w.result = result
			return nil
		}
	}
	// A single batch cannot reuse capacity across batches. Keep the pooled
	// buffered path, including its encoding-time timeout and error handling.
	w.renderErr = json.NewEncoder(w.timeoutResponseRecorder).Encode(data)
	return w.renderErr
}

func dqlTimeoutMiddleware(handler gin.HandlerFunc) gin.HandlerFunc {
	return dqlResponseMiddleware(handler, dqlPreparationTimeout)
}

type dqlTimeoutScope int

const (
	dqlPreparationTimeout dqlTimeoutScope = iota
	dqlExplicitTransportTimeout
)

func dqlResponseMiddleware(handler gin.HandlerFunc, scope dqlTimeoutScope) gin.HandlerFunc {
	bufPool := &BufferPool{}
	return func(c *gin.Context) {
		// DQLStreamingHTTPHandler is required to abort partial responses past Gin Recovery.
		state := c.Request.Context().Value(dqlStreamKey{}).(*dqlStreamState)
		parentCtx := c.Request.Context()
		ctx := parentCtx
		cancel := func() {}
		if scope == dqlPreparationTimeout || c.Request.Header.Get(mhttp.HTTPHeaderRequestTimeout) != "" {
			timeout, ok := requestTimeout(c)
			if !ok {
				return
			}
			ctx, cancel = context.WithTimeout(parentCtx, timeout)
		}
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		buffer := bufPool.Get()
		buffer.Reset()
		writer := newDQLResponseWriter(ctx, buffer)
		writer.encodeComplete = scope == dqlExplicitTransportTimeout
		defer func() {
			writer.Close() // Detach under the recorder lock before reuse by another request.
			bufPool.Put(buffer)
		}()
		child := c.Copy()
		child.Request, child.Writer = c.Request, writer
		finished := make(chan any, 1)
		go func() {
			defer func() { finished <- recover() }()
			handler(child)
		}()

		abort := func() { c.Abort(); state.aborted = true }
		fail := func(status int) {
			c.Abort()
			c.Set(HTTPReturnCode, status)
			if status == http.StatusRequestTimeout {
				c.Set(HTTPReturnCode, merr.TimeoutCode)
				c.Set(HTTPReturnMessage, "request timeout")
			}
			if c.Writer.Written() {
				abort()
				return
			}
			body := gin.H{HTTPReturnCode: status}
			if status == http.StatusRequestTimeout {
				body = gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"}
			}
			HTTPAbortReturn(c, status, body)
		}
		defer func() {
			if p := recover(); p != nil {
				fail(http.StatusInternalServerError)
				panic(p) // Gin logs the panic; the outer wrapper aborts committed responses.
			}
		}()
		stopRequest := func(err error) {
			if err == context.Canceled {
				c.Set(HTTPReturnCode, merr.CanceledCode)
				c.Set(HTTPReturnMessage, err.Error())
				abort() // Cancellation is not a timeout; do not send a 408 envelope.
				return
			}
			fail(http.StatusRequestTimeout)
		}
		select {
		case <-ctx.Done():
			stopRequest(ctx.Err())
			return
		case panicValue := <-finished:
			propagateTimeoutContextKeys(c, child)
			if child.IsAborted() {
				c.Abort()
			}
			if panicValue != nil {
				panic(panicValue)
			}
		}
		// Resolve completion/deadline races before leaving the preparation phase.
		if err := dqlContextErr(ctx); err != nil {
			stopRequest(err)
			return
		}
		if writer.renderErr != nil {
			fail(http.StatusInternalServerError)
			return
		}
		responseCtx := ctx
		if scope == dqlPreparationTimeout {
			// Search and Query stream outside their preparation timeout.
			cancel()
			responseCtx = parentCtx
			c.Request = c.Request.WithContext(parentCtx)
		}
		if writer.result == nil && writer.encoded == nil {
			if err := writer.CommitTo(c.Writer); err != nil {
				abort()
			}
			return
		}
		for key, values := range writer.Header() {
			c.Writer.Header()[key] = values
		}
		c.Writer.WriteHeader(writer.Status())
		var err error
		if writer.encoded != nil {
			err = writeDQLTransportResponse(responseCtx, c.Writer, writer.encoded)
		} else {
			err = writeDQLJSON(responseCtx, c.Writer, writer.result, dqlJSONBatchRows)
		}
		if ctxErr := dqlContextErr(responseCtx); ctxErr != nil {
			stopRequest(ctxErr)
		} else if err != nil {
			mlog.Warn(responseCtx, "failed to encode or write DQL response", mlog.Err(err))
			fail(http.StatusInternalServerError)
		}
	}
}

func dqlContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func writeDQLTransportResponse(ctx context.Context, w http.ResponseWriter, body []byte) error {
	if err := dqlContextErr(ctx); err != nil {
		return err
	}
	// Interrupt a blocked write when the transport context ends. Waiting for
	// the callback keeps it inside the ResponseWriter's request lifetime.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	n, err := w.Write(body)
	if err == nil && n != len(body) {
		return io.ErrShortWrite
	}
	return err
}
