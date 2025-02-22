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
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func defaultResponse(c *gin.Context) {
	c.JSON(http.StatusRequestTimeout, gin.H{HTTPReturnCode: merr.TimeoutCode, HTTPReturnMessage: "request timeout"})
}

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
	timeout  time.Duration
	handler  gin.HandlerFunc
	response gin.HandlerFunc
}

// Writer is a writer with memory buffer
type Writer struct {
	gin.ResponseWriter
	body         *bytes.Buffer
	headers      http.Header
	mu           sync.Mutex
	timeout      bool
	wroteHeaders bool
	code         int
}

// NewWriter will return a timeout.Writer pointer
func NewWriter(w gin.ResponseWriter, buf *bytes.Buffer) *Writer {
	return &Writer{ResponseWriter: w, body: buf, headers: make(http.Header)}
}

// Write will write data to response body
func (w *Writer) Write(data []byte) (int, error) {
	if w.timeout || w.body == nil {
		return 0, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.body.Write(data)
}

// WriteHeader sends an HTTP response header with the provided status code.
// If the response writer has already written headers or if a timeout has occurred,
// this method does nothing.
func (w *Writer) WriteHeader(code int) {
	if w.timeout || w.wroteHeaders {
		return
	}

	// gin is using -1 to skip writing the status code
	// see https://github.com/gin-gonic/gin/blob/a0acf1df2814fcd828cb2d7128f2f4e2136d3fac/response_writer.go#L61
	if code == -1 {
		return
	}

	checkWriteHeaderCode(code)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.writeHeader(code)
	w.ResponseWriter.WriteHeader(code)
}

func (w *Writer) writeHeader(code int) {
	w.wroteHeaders = true
	w.code = code
}

// Header will get response headers
func (w *Writer) Header() http.Header {
	return w.headers
}

// WriteString will write string to response body
func (w *Writer) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// FreeBuffer will release buffer pointer
func (w *Writer) FreeBuffer() {
	// if not reset body,old bytes will put in bufPool
	w.body.Reset()
	w.body = nil
}

// Status we must override Status func here,
// or the http status code returned by gin.Context.Writer.Status()
// will always be 200 in other custom gin middlewares.
func (w *Writer) Status() int {
	if w.code == 0 || w.timeout {
		return w.ResponseWriter.Status()
	}
	return w.code
}

func checkWriteHeaderCode(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid http status code: %d", code))
	}
}

func timeoutMiddleware(handler gin.HandlerFunc) gin.HandlerFunc {
	t := &Timeout{
		timeout:  mhttp.HTTPDefaultTimeout,
		handler:  handler,
		response: defaultResponse,
	}
	bufPool := &BufferPool{}
	return func(gCtx *gin.Context) {
		topCtx, cancel := context.WithCancel(gCtx.Request.Context())
		defer cancel()
		gCtx.Request = gCtx.Request.WithContext(topCtx)

		timeoutSecond, err := strconv.ParseInt(gCtx.Request.Header.Get(mhttp.HTTPHeaderRequestTimeout), 10, 64)
		if err == nil {
			t.timeout = time.Duration(timeoutSecond) * time.Second
		}
		finish := make(chan struct{}, 1)
		panicChan := make(chan interface{}, 1)

		w := gCtx.Writer
		buffer := bufPool.Get()
		tw := NewWriter(w, buffer)
		gCtx.Writer = tw
		buffer.Reset()

		go func() {
			defer func() {
				if p := recover(); p != nil {
					panicChan <- p
				}
			}()
			t.handler(gCtx)
			finish <- struct{}{}
		}()

		select {
		case p := <-panicChan:
			tw.FreeBuffer()
			gCtx.Writer = w
			gCtx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{mhttp.HTTPReturnCode: http.StatusInternalServerError})
			panic(p)

		case <-finish:
			gCtx.Next()
			tw.mu.Lock()
			defer tw.mu.Unlock()
			dst := tw.ResponseWriter.Header()
			for k, vv := range tw.Header() {
				dst[k] = vv
			}

			if _, err := tw.ResponseWriter.Write(buffer.Bytes()); err != nil {
				panic(err)
			}
			tw.FreeBuffer()
			bufPool.Put(buffer)

		case <-time.After(t.timeout):
			gCtx.Abort()
			tw.mu.Lock()
			defer tw.mu.Unlock()
			tw.timeout = true
			tw.FreeBuffer()
			bufPool.Put(buffer)

			gCtx.Writer = w
			t.response(gCtx)
			gCtx.Writer = tw
		}
	}
}
