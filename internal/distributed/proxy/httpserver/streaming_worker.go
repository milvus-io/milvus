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
	"context"
	"io"
	"net"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

type streamingIOKind uint8

const (
	streamingWrite streamingIOKind = iota
	streamingFlush
	streamingWriteHeader
	streamingReadBody
	streamingCloseBody
)

type streamingIOResult struct {
	n       int
	err     error
	written bool
}

type streamingIOCall struct {
	kind   streamingIOKind
	data   []byte
	header http.Header
	status int
	done   chan streamingIOResult
}

// streamingExchange borrows one I/O slice at a time, without copying or queuing it.
// Only the serving goroutine accesses the real writer and request body. A late
// worker retains this detached exchange, never the connection or pooled Gin context.
type streamingExchange struct {
	ctx   context.Context
	calls chan streamingIOCall
}

func (x *streamingExchange) call(call streamingIOCall) streamingIOResult {
	if err := x.ctx.Err(); err != nil {
		return streamingIOResult{err: err}
	}
	call.done = make(chan streamingIOResult, 1)
	select {
	case <-x.ctx.Done():
		return streamingIOResult{err: x.ctx.Err()}
	case x.calls <- call:
	}
	// Once accepted, wait for acknowledgement even on cancellation: the serving
	// goroutine may still be using data. Transport deadlines bound that I/O.
	return <-call.done
}

// serve runs only on the HTTP goroutine and always acknowledges borrowed bytes,
// including when a transport panics. No worker can return from Write/Read while
// this method still uses its slice.
func (call streamingIOCall) serve(w *streamingResponseWriter, body io.ReadCloser) {
	result := streamingIOResult{err: http.ErrAbortHandler}
	defer func() {
		result.written = w.Written()
		call.done <- result
	}()
	if err := w.requestError(); err != nil {
		result.err = err
		return
	}
	if call.header != nil && !w.Written() {
		copyStreamingHeader(w.Header(), call.header)
		w.WriteHeader(call.status)
	}
	switch call.kind {
	case streamingWrite:
		result.n, result.err = w.Write(call.data)
	case streamingFlush:
		result.err = w.FlushError()
	case streamingWriteHeader:
		w.WriteHeaderNow()
		result.err = nil
	case streamingReadBody:
		result.n, result.err = body.Read(call.data)
	case streamingCloseBody:
		result.err = body.Close()
	}
}

func copyStreamingHeader(dst, src http.Header) {
	clear(dst)
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
}

type streamingWorkerBody struct{ exchange *streamingExchange }

func (b streamingWorkerBody) Read(p []byte) (int, error) {
	result := b.exchange.call(streamingIOCall{kind: streamingReadBody, data: p})
	return result.n, result.err
}

func (b streamingWorkerBody) Close() error {
	return b.exchange.call(streamingIOCall{kind: streamingCloseBody}).err
}

// streamingWorkerWriter owns only worker-local metadata. Header and status
// changes cannot race with timeout finalization or reach a returned connection.
type streamingWorkerWriter struct {
	exchange *streamingExchange
	header   http.Header
	status   int
	size     int
	failure  error
}

func (w *streamingWorkerWriter) Header() http.Header { return w.header }
func (w *streamingWorkerWriter) Status() int         { return w.status }
func (w *streamingWorkerWriter) Size() int           { return w.size }
func (w *streamingWorkerWriter) Written() bool       { return w.size >= 0 }

func (w *streamingWorkerWriter) WriteHeader(code int) {
	if code > 0 && !w.Written() {
		checkWriteHeaderCode(code)
		w.status = code
	}
}

func (w *streamingWorkerWriter) output(kind streamingIOKind, data []byte) streamingIOResult {
	if w.failure != nil {
		return streamingIOResult{err: w.failure}
	}
	call := streamingIOCall{kind: kind, data: data, status: w.status}
	if !w.Written() {
		call.header = w.header
	}
	result := w.exchange.call(call)
	if result.written && !w.Written() {
		w.size = 0
	}
	w.size += result.n
	w.failure = result.err
	return result
}

func (w *streamingWorkerWriter) Write(p []byte) (int, error) {
	result := w.output(streamingWrite, p)
	return result.n, result.err
}

func (w *streamingWorkerWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func (w *streamingWorkerWriter) WriteHeaderNow() {
	if !w.Written() {
		if result := w.output(streamingWriteHeader, nil); result.err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

func (w *streamingWorkerWriter) FlushError() error { return w.output(streamingFlush, nil).err }

func (w *streamingWorkerWriter) Flush() {
	if err := w.FlushError(); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (w *streamingWorkerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, http.ErrNotSupported
}

func (w *streamingWorkerWriter) CloseNotify() <-chan bool {
	closed := make(chan bool, 1)
	context.AfterFunc(w.exchange.ctx, func() { closed <- true })
	return closed
}

func (w *streamingWorkerWriter) Pusher() http.Pusher { return nil }

type streamingWorkerResult struct {
	panicValue any
	stack      []byte
}

// serveWorker keeps all real I/O on the HTTP goroutine. It stops accepting
// worker calls on timeout without waiting for uncooperative computation.
func (w *streamingResponseWriter) serveWorker(c *gin.Context, handler gin.HandlerFunc) streamingWorkerResult {
	// The worker owns a copied Gin context and detached I/O adapters.
	exchange := &streamingExchange{ctx: w.ctx, calls: make(chan streamingIOCall)}
	workerWriter := &streamingWorkerWriter{
		exchange: exchange, header: w.ResponseWriter.Header().Clone(),
		status: w.ResponseWriter.Status(), size: -1,
	}
	workerCtx := c.Copy()
	workerCtx.Writer = workerWriter
	workerCtx.Request = c.Request.Clone(w.ctx)
	if c.Request.Body != nil && c.Request.Body != http.NoBody {
		workerCtx.Request.Body = streamingWorkerBody{exchange: exchange}
	}
	finished := make(chan streamingWorkerResult, 1)
	go func() {
		defer func() {
			result := streamingWorkerResult{panicValue: recover()}
			if result.panicValue != nil {
				result.stack = debug.Stack()
			}
			finished <- result // Never block a worker finishing after timeout.
		}()
		handler(workerCtx)
	}()
	for {
		select {
		case <-w.ctx.Done():
			return streamingWorkerResult{}
		case call := <-exchange.calls:
			call.serve(w, c.Request.Body)
			if w.failure != nil {
				return streamingWorkerResult{}
			}
		case result := <-finished:
			// Completion establishes exclusive access to worker state.
			propagateTimeoutContextKeys(c, workerCtx)
			c.Errors = append(c.Errors, workerCtx.Errors...)
			if workerCtx.IsAborted() {
				c.Abort()
			}
			if !w.Written() {
				copyStreamingHeader(w.ResponseWriter.Header(), workerWriter.header)
				w.ResponseWriter.WriteHeader(workerWriter.status)
			}
			// Cancellation can make a late renderer fail before we select
			// ctx.Done(). Preserve the precommit timeout envelope in that race.
			if w.requestError() == nil {
				w.failure = workerWriter.failure
			}
			if result.panicValue != nil && !(result.panicValue == http.ErrAbortHandler && w.requestError() != nil) {
				return result
			}
			return streamingWorkerResult{}
		}
	}
}
