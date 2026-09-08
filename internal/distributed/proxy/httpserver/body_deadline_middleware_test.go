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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A plain httptest.ResponseRecorder does not implement SetReadDeadline/
// SetWriteDeadline (it has no underlying connection at all), so
// http.ResponseController returns http.ErrNotSupported for it. The
// middleware must swallow that instead of failing the request.
func TestBodyDeadlineMiddleware_UnsupportedWriterDoesNotBreakRequest(t *testing.T) {
	paramtable.Get().Save("proxy.http.readTimeout", "1s")
	paramtable.Get().Save("proxy.http.writeTimeout", "1s")
	defer func() {
		paramtable.Get().Reset("proxy.http.readTimeout")
		paramtable.Get().Reset("proxy.http.writeTimeout")
	}()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(BodyDeadlineMiddleware())
	router.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()

	assert.NotPanics(t, func() {
		router.ServeHTTP(w, req)
	})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "pong", w.Body.String())
}

// This is the test both PR reviews asked for: prove the server-side
// resource (the goroutine blocked reading the body) is actually released
// when a client declares a body and then withholds it -- not just that a
// client eventually sees some response.
func TestBodyDeadlineMiddleware_ReleasesHandlerGoroutineOnStalledBody(t *testing.T) {
	paramtable.Get().Save("proxy.http.readTimeout", "150ms")
	paramtable.Get().Save("proxy.http.writeTimeout", "0s")
	defer func() {
		paramtable.Get().Reset("proxy.http.readTimeout")
		paramtable.Get().Reset("proxy.http.writeTimeout")
	}()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(BodyDeadlineMiddleware())

	bodyReadDone := make(chan error, 1)
	router.POST("/echo", func(c *gin.Context) {
		_, err := io.ReadAll(c.Request.Body)
		bodyReadDone <- err
		c.Status(http.StatusOK)
	})

	srv := httptest.NewServer(router)
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Declare a body far larger than what we actually send, then withhold
	// the rest. Without a read deadline, the handler's io.ReadAll would
	// block on this forever.
	request := "POST /echo HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Content-Length: 1000000\r\n" +
		"Content-Type: application/octet-stream\r\n" +
		"Connection: close\r\n\r\n" +
		"only-a-few-bytes-then-nothing"
	_, err = conn.Write([]byte(request))
	require.NoError(t, err)

	// The handler goroutine's body read must be unblocked by readTimeout
	// (150ms) well before this generous upper bound -- proving the
	// goroutine/connection is actually released, not just that some
	// client-visible response eventually shows up.
	select {
	case readErr := <-bodyReadDone:
		assert.Error(t, readErr, "body read should fail once the read deadline fires")
	case <-time.After(3 * time.Second):
		t.Fatal("handler's body read never returned -- readTimeout did not release the blocked goroutine")
	}

	// The handler still writes a normal response after the failed body read
	// (it doesn't return early on the read error), so the client should be
	// able to read a complete, valid HTTP response...
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	_ = resp.Body.Close()

	// ...and then, per "Connection: close", the server should close the
	// socket rather than leaving it open indefinitely.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, readErr := conn.Read(make([]byte, 1))
	assert.True(t, n == 0 && readErr != nil,
		"expected connection to be closed after the response, got n=%d err=%v", n, readErr)
}
