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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// deadlineRecorderWriter lets a unit test observe whether SetReadDeadline/
// SetWriteDeadline were called, without needing a real connection.
// http.ResponseController finds these methods directly on the concrete
// writer, no Unwrap() needed.
type deadlineRecorderWriter struct {
	http.ResponseWriter
	readDeadlineCalls  int
	writeDeadlineCalls int
}

func (w *deadlineRecorderWriter) SetReadDeadline(time.Time) error {
	w.readDeadlineCalls++
	return nil
}

func (w *deadlineRecorderWriter) SetWriteDeadline(time.Time) error {
	w.writeDeadlineCalls++
	return nil
}

func withTimeouts(t *testing.T, read, write string) {
	t.Helper()
	paramtable.Get().Save("proxy.http.readTimeout", read)
	paramtable.Get().Save("proxy.http.writeTimeout", write)
	t.Cleanup(func() {
		paramtable.Get().Reset("proxy.http.readTimeout")
		paramtable.Get().Reset("proxy.http.writeTimeout")
	})
}

// This is the core gRPC-safety property: already-negotiated HTTP/2 (how all
// real gRPC arrives) must never have a deadline armed on it here.
func TestWrapWithBodyDeadline_SkipsAlreadyNegotiatedHTTP2(t *testing.T) {
	withTimeouts(t, "1s", "1s")

	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	handler := WrapWithBodyDeadline(next)

	rec := &deadlineRecorderWriter{ResponseWriter: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.ProtoMajor = 2

	handler.ServeHTTP(rec, req)

	assert.True(t, called)
	assert.Zero(t, rec.readDeadlineCalls)
	assert.Zero(t, rec.writeDeadlineCalls)
}

// Everything still HTTP/1.1 -- plain REST, or an h2c Upgrade attempt that
// hasn't succeeded yet -- must get the deadline.
func TestWrapWithBodyDeadline_AppliesDeadlineForHTTP1(t *testing.T) {
	withTimeouts(t, "1s", "1s")

	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	handler := WrapWithBodyDeadline(next)

	rec := &deadlineRecorderWriter{ResponseWriter: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.ProtoMajor = 1

	handler.ServeHTTP(rec, req)

	assert.True(t, called)
	assert.Equal(t, 1, rec.readDeadlineCalls)
	assert.Equal(t, 1, rec.writeDeadlineCalls)
}

// A plain httptest.ResponseRecorder has no underlying connection, so
// http.ResponseController returns http.ErrNotSupported. The handler must
// swallow that instead of breaking the request.
func TestWrapWithBodyDeadline_UnsupportedWriterDoesNotBreakRequest(t *testing.T) {
	withTimeouts(t, "1s", "1s")

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := WrapWithBodyDeadline(next)

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.ProtoMajor = 1
	w := httptest.NewRecorder()

	assert.NotPanics(t, func() {
		handler.ServeHTTP(w, req)
	})
	assert.Equal(t, http.StatusOK, w.Code)
}

// The original case this whole fix exists for: a handler that tries to read
// a declared-but-withheld body must have that read actually released, not
// just have the client eventually see some response.
func TestWrapWithBodyDeadline_ReleasesHandlerGoroutineOnStalledBody(t *testing.T) {
	withTimeouts(t, "150ms", "0s")

	bodyReadDone := make(chan error, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		bodyReadDone <- err
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(WrapWithBodyDeadline(next))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()

	request := "POST /echo HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Content-Length: 1000000\r\n" +
		"Content-Type: application/octet-stream\r\n" +
		"Connection: close\r\n\r\n" +
		"only-a-few-bytes-then-nothing"
	_, err = conn.Write([]byte(request))
	require.NoError(t, err)

	select {
	case readErr := <-bodyReadDone:
		assert.Error(t, readErr, "body read should fail once the read deadline fires")
	case <-time.After(3 * time.Second):
		t.Fatal("handler's body read never returned -- readTimeout did not release the blocked goroutine")
	}
}

// This is the reproducer for the gin trailing-slash-redirect gap: a handler
// that never reads the body at all (exactly what gin's own redirect does --
// it writes a response and returns without reading anything) must still not
// leave the connection's post-handler body drain unbounded. Before this fix
// moved the deadline outside gin, this case had zero middleware invocations
// and stayed blocked indefinitely.
func TestWrapWithBodyDeadline_BoundsPostHandlerDrainWhenHandlerNeverReadsBody(t *testing.T) {
	withTimeouts(t, "150ms", "0s")

	handlerReturned := make(chan struct{}, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulates gin's own redirect: respond immediately, never touch r.Body.
		w.WriteHeader(http.StatusMovedPermanently)
		handlerReturned <- struct{}{}
	})

	srv := httptest.NewServer(WrapWithBodyDeadline(next))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()

	request := "POST /echo/ HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Content-Length: 1000000\r\n" +
		"Content-Type: application/octet-stream\r\n" +
		"Connection: close\r\n\r\n" +
		"only-a-few-bytes-then-nothing"
	_, err = conn.Write([]byte(request))
	require.NoError(t, err)

	select {
	case <-handlerReturned:
	case <-time.After(time.Second):
		t.Fatal("handler never even ran")
	}

	// net/http's own post-handler drain (finishRequest) now runs, trying to
	// consume the undeclared remainder of the body before it can close the
	// connection for "Connection: close". Without a deadline already armed
	// on the connection -- which is exactly what happens if the deadline is
	// only set inside gin middleware, since gin's redirect never reaches
	// that middleware -- this would block indefinitely.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, readErr := io.ReadAll(conn)
	assert.NoError(t, readErr, "connection should close cleanly well within the read deadline, not hang")
}
