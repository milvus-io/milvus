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

// These are characterization probes, not an implementation of request budgets.
// Client deadlines are hang guards; only explicitly named server deadlines are
// the capability under test. No Milvus/native packages are imported.
package requestbudget

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"golang.org/x/net/http2/hpack"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2/h2c"
)

const probeGuard = 3 * time.Second

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(probeGuard):
		t.Fatal("probe hung")
		var zero T
		return zero
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPendingReadDeadline(t *testing.T)  { pendingDeadline(t, false) }
func TestPendingWriteDeadline(t *testing.T) { pendingDeadline(t, true) }

func pendingDeadline(t *testing.T, write bool) {
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	done := make(chan error, 1)
	go func() {
		var err error
		if write {
			_, err = a.Write([]byte{1})
		} else {
			_, err = a.Read(make([]byte, 1))
		}
		done <- err
	}()
	// The peer never performs the complementary operation. First establish
	// that I/O has not completed, then install its deadline while outstanding.
	select {
	case err := <-done:
		t.Fatalf("I/O unexpectedly completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if write {
		must(t, a.SetWriteDeadline(time.Now().Add(20*time.Millisecond)))
	} else {
		must(t, a.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	}
	if err := receive(t, done); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}

func serveProbe(t *testing.T, s *http.Server) net.Conn {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	go s.Serve(l)
	c, err := net.Dial("tcp", l.Addr().String())
	must(t, err)
	must(t, c.SetDeadline(time.Now().Add(probeGuard)))
	t.Cleanup(func() { c.Close(); s.Close(); l.Close() })
	return c
}

func TestTransportHTTP1HeaderLifecycleAndReuse(t *testing.T) {
	active := make(chan struct{}, 8)
	paths := make(chan string, 8)
	s := &http.Server{
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateActive {
				active <- struct{}{}
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths <- r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	c := serveProbe(t, s)
	reader := bufio.NewReader(c)
	// Repeat on the same connection: active is not a header-start callback.
	for _, path := range []string{"/first", "/reuse"} {
		_, err := io.WriteString(c, "GET "+path+" HTTP/1.1\r\nHost: probe\r\nX-Slow: ")
		must(t, err)
		select {
		case <-active:
			t.Fatal("StateActive fired before headers completed")
		case <-time.After(40 * time.Millisecond):
		}
		_, err = io.WriteString(c, "done\r\n\r\n")
		must(t, err)
		if got := receive(t, paths); got != path {
			t.Fatalf("path %q", got)
		}
		receive(t, active)
		r, err := http.ReadResponse(reader, nil)
		must(t, err)
		r.Body.Close()
	}
	// Two complete pipelined requests in one socket write: application-visible
	// events occur individually, but provide no timestamp of buffered headers.
	_, err := io.WriteString(c, "GET /pipe1 HTTP/1.1\r\nHost: probe\r\n\r\nGET /pipe2 HTTP/1.1\r\nHost: probe\r\n\r\n")
	must(t, err)
	for _, want := range []string{"/pipe1", "/pipe2"} {
		r, err := http.ReadResponse(reader, nil)
		must(t, err)
		r.Body.Close()
		if got := receive(t, paths); got != want {
			t.Fatalf("path %q", got)
		}
	}
}

// Go's HTTP/1 server waits for four bytes of the next keep-alive request
// before arming ReadHeaderTimeout. This is a characterization of the gap, not
// an assertion that the behavior satisfies the REST overall budget.
func TestTransportHTTP1KeepAliveFirstFourBytesPrecedeHeaderGuard(t *testing.T) {
	s := &http.Server{
		ReadHeaderTimeout: 30 * time.Millisecond,
		IdleTimeout:       250 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	c := serveProbe(t, s)
	reader := bufio.NewReader(c)
	_, err := io.WriteString(c, "GET /first HTTP/1.1\r\nHost: probe\r\n\r\n")
	must(t, err)
	first, err := http.ReadResponse(reader, nil)
	must(t, err)
	must(t, first.Body.Close())

	_, err = io.WriteString(c, "G")
	must(t, err)
	time.Sleep(90 * time.Millisecond) // Longer than ReadHeaderTimeout, shorter than IdleTimeout.
	_, err = io.WriteString(c, "ET /reuse HTTP/1.1\r\nHost: probe\r\n\r\n")
	must(t, err)
	second, err := http.ReadResponse(reader, nil)
	must(t, err)
	defer second.Body.Close()
	if second.StatusCode != http.StatusNoContent {
		t.Fatalf("second request status = %d, want %d", second.StatusCode, http.StatusNoContent)
	}
}

func TestTransportCMuxPartialPrefaceFallsThroughAfterTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	m := cmux.New(l)
	m.SetReadTimeout(60 * time.Millisecond)
	m.Match(cmux.HTTP2())
	fallback := m.Match(cmux.Any())
	go m.Serve()
	t.Cleanup(func() { m.Close(); l.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := fallback.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	must(t, err)
	defer c.Close()
	_, err = io.WriteString(c, "PRI * HTTP/2.0\r\n")
	must(t, err)
	a := receive(t, accepted)
	defer a.Close()
	// A matcher timeout is NOT a fail-closed entry budget with HTTP2(), Any().
	// Buffered bytes survive and the public cmux timeout is cleared on match.
	b := make([]byte, len("PRI * HTTP/2.0\r\n"))
	must(t, a.SetReadDeadline(time.Now().Add(probeGuard)))
	_, err = io.ReadFull(a, b)
	must(t, err)
	if string(b) != "PRI * HTTP/2.0\r\n" {
		t.Fatalf("preface %q", b)
	}
}

func headerBlock(t *testing.T, path string) []byte {
	t.Helper()
	var b bytes.Buffer
	e := hpack.NewEncoder(&b)
	for _, h := range []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "probe"}, {Name: ":path", Value: path}} {
		must(t, e.WriteField(h))
	}
	return b.Bytes()
}

func h2Probe(t *testing.T, h http.Handler, config *http2.Server, settings ...http2.Setting) (*http2.Framer, net.Conn) {
	t.Helper()
	c := serveProbe(t, &http.Server{Handler: h2c.NewHandler(h, config), ReadHeaderTimeout: 30 * time.Millisecond})
	return h2Frames(t, c, settings...)
}

func h2Frames(t *testing.T, c net.Conn, settings ...http2.Setting) (*http2.Framer, net.Conn) {
	t.Helper()
	_, err := io.WriteString(c, http2.ClientPreface)
	must(t, err)
	f := http2.NewFramer(c, c)
	must(t, f.WriteSettings(settings...))
	// Synchronize with a complete HTTP/2 connection, before sending requests.
	for {
		frame, err := f.ReadFrame()
		must(t, err)
		if sf, ok := frame.(*http2.SettingsFrame); ok && !sf.IsAck() {
			must(t, f.WriteSettingsAck())
			break
		}
	}
	return f, c
}

func TestTransportH2PartialHeaderCompletesBeforeDeadline(t *testing.T) {
	called := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	})
	c := serveProbe(t, &http.Server{Handler: h2c.NewHandler(handler, &http2.Server{}), ReadHeaderTimeout: 500 * time.Millisecond})
	f, _ := h2Frames(t, c)
	b := headerBlock(t, "/slow")
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndStream: true, EndHeaders: false, BlockFragment: b[:1]}))
	// An incomplete block is invisible to the handler, but can complete before
	// its 500ms header deadline without resetting that deadline.
	select {
	case <-called:
		t.Fatal("handler entered before END_HEADERS")
	case <-time.After(100 * time.Millisecond):
	}
	must(t, f.WriteContinuation(1, true, b[1:]))
	receive(t, called)
}

func TestTransportH2IncompleteHeaderExpiresBeforeHandler(t *testing.T) {
	called := make(chan struct{}, 1)
	f, c := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{})
	b := headerBlock(t, "/never-complete")
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndStream: true, EndHeaders: false, BlockFragment: b[:1]}))
	waitH2ServerCloseBeforeGuard(t, f, c)
	select {
	case <-called:
		t.Fatal("incomplete header reached handler")
	default:
	}
}

func waitH2ServerCloseBeforeGuard(t *testing.T, f *http2.Framer, c net.Conn) {
	t.Helper()
	must(t, c.SetReadDeadline(time.Now().Add(300*time.Millisecond))) // Test guard, not server policy.
	for {
		_, err := f.ReadFrame()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("server did not terminate an incomplete header block before the test guard")
			}
			break
		}
	}
}

func TestTransportH2PartialFrameHeaderExpires(t *testing.T) {
	f, c := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("partial frame header entered handler")
	}), &http2.Server{})
	_, err := c.Write([]byte{0}) // Only the first byte of a nine-byte frame header.
	must(t, err)
	waitH2ServerCloseBeforeGuard(t, f, c)
}

type readDeadlineFailureConn struct{ net.Conn }

func (c readDeadlineFailureConn) SetReadDeadline(time.Time) error {
	return errors.New("read deadlines unsupported")
}

func TestTransportH2HeaderGuardFailsClosedWhenDeadlineUnsupported(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request entered handler without a working header guard")
	})
	go func() {
		serverConn, acceptErr := l.Accept()
		if acceptErr == nil {
			(&http2.Server{}).ServeConn(readDeadlineFailureConn{serverConn}, &http2.ServeConnOpts{
				Handler: handler, BaseConfig: &http.Server{ReadHeaderTimeout: 30 * time.Millisecond},
			})
		}
	}()
	t.Cleanup(func() { l.Close() })
	c, err := net.Dial("tcp", l.Addr().String())
	must(t, err)
	t.Cleanup(func() { c.Close() })
	must(t, c.SetDeadline(time.Now().Add(probeGuard)))
	_, err = io.WriteString(c, http2.ClientPreface)
	must(t, err)
	f := http2.NewFramer(c, c)
	must(t, f.WriteSettings())
	waitH2ServerCloseBeforeGuard(t, f, c)
}

func TestTransportH2HeaderGuardClearsBeforeGRPCHandler(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	f, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/grpc" {
			t.Errorf("Content-Type = %q", got)
		}
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: "probe"},
		{Name: ":path", Value: "/grpc"},
		{Name: "content-type", Value: "application/grpc"},
	} {
		must(t, encoder.WriteField(field))
	}
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndStream: true, EndHeaders: true, BlockFragment: block.Bytes()}))
	receive(t, entered)
	// The shared 30ms header guard must not keep running after the header is
	// complete. A gRPC-like stream is not subject to a REST request budget.
	time.Sleep(80 * time.Millisecond)
	close(release)
	readStreamEnd(t, f, 1)
}

func TestTransportH2HeaderCompletionAvailableInHandler(t *testing.T) {
	type entry struct{ completedAt, handlerAt time.Time }
	entered := make(chan entry, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		completedAt, ok := http2.RequestHeadersCompletedAt(r)
		if !ok {
			t.Error("header completion missing from HTTP/2 handler context")
		}
		entered <- entry{completedAt, time.Now()}
		w.WriteHeader(http.StatusNoContent)
	})
	c := serveProbe(t, &http.Server{Handler: h2c.NewHandler(handler, &http2.Server{}), ReadHeaderTimeout: 500 * time.Millisecond})
	f, _ := h2Frames(t, c)
	b := headerBlock(t, "/cost")
	firstHeaderAt := time.Now()
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndStream: true, EndHeaders: false, BlockFragment: b[:1]}))
	time.Sleep(100 * time.Millisecond)
	must(t, f.WriteContinuation(1, true, b[1:]))
	got := receive(t, entered)
	if elapsed := got.completedAt.Sub(firstHeaderAt); elapsed < 80*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("header completion time does not reflect slow header read: %v", elapsed)
	}
	if got.completedAt.After(got.handlerAt) {
		t.Fatalf("header completion %v occurred after handler entry %v", got.completedAt, got.handlerAt)
	}
}

func TestTransportH2QueuedHandlerIsNotHeaderCompletion(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	f, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.URL.Path
		if r.URL.Path == "/hold" {
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{MaxConcurrentStreams: 1})
	defer close(release)
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/hold")}))
	if receive(t, entered) != "/hold" {
		t.Fatal("wrong first handler")
	}
	// Reset frees the protocol stream slot but leaves an uncooperative handler.
	must(t, f.WriteRSTStream(1, http2.ErrCodeCancel))
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/queued")}))
	// The subsequent PING acknowledgement proves the server processed the
	// complete header block, without relying on a sleep for frame delivery.
	must(t, f.WritePing(false, [8]byte{42}))
	for {
		frame, err := f.ReadFrame()
		must(t, err)
		if p, ok := frame.(*http2.PingFrame); ok && p.IsAck() && p.Data == [8]byte{42} {
			break
		}
	}
	select {
	case p := <-entered:
		t.Fatalf("handler should be queued: %s", p)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTransportH2LargeWriteProgressBeforeWriteReturns(t *testing.T) {
	done := make(chan error, 1)
	f, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write(make([]byte, 1<<20))
		done <- err
	}), &http2.Server{})
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/large")}))
	total := 0
	for {
		frame, err := f.ReadFrame()
		must(t, err)
		if d, ok := frame.(*http2.DataFrame); ok {
			total += len(d.Data())
			if total <= 16384 {
				// Actual payload reached the peer, but the one application
				// Write is still blocked by the stream/connection window.
				select {
				case err := <-done:
					t.Fatalf("large Write returned before flow-control credit: %v", err)
				default:
				}
			}
			if d.StreamEnded() {
				break
			}
			if n := uint32(len(d.Data())); n > 0 {
				must(t, f.WriteWindowUpdate(0, n))
				must(t, f.WriteWindowUpdate(1, n))
			}
		}
	}
	if total != 1<<20 {
		t.Fatalf("payload bytes = %d", total)
	}
	must(t, receive(t, done))
}

func TestTransportH2InterleavedContinuationIsConnectionError(t *testing.T) {
	diagnostics := &protocolDiagnostic{t: t, seen: make(chan struct{}, 1)}
	c := serveProbe(t, &http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), &http2.Server{}),
		// net/http requires *log.Logger for this test-only capture adapter.
		ErrorLog: log.New(diagnostics, "", 0),
	})
	f, _ := h2Frames(t, c)
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: false, BlockFragment: headerBlock(t, "/bad")}))
	// RFC 9113 requires a contiguous header block: a PING before CONTINUATION
	// is a connection protocol error, unlike expiry of an established stream.
	must(t, f.WritePing(false, [8]byte{}))
	for {
		frame, err := f.ReadFrame()
		must(t, err)
		if g, ok := frame.(*http2.GoAwayFrame); ok {
			if g.ErrCode != http2.ErrCodeProtocol {
				t.Fatalf("GOAWAY %v", g.ErrCode)
			}
			receive(t, diagnostics.seen)
			return
		}
	}
}

// Capture only the expected diagnostic from this deliberately corrupt client.
// Unexpected or duplicate diagnostics remain test failures, not discarded logs.
type protocolDiagnostic struct {
	t    *testing.T
	seen chan struct{}
}

func (d *protocolDiagnostic) Write(p []byte) (int, error) {
	message := string(p)
	if !strings.HasPrefix(message, "http2: server connection error from ") || !strings.HasSuffix(message, ": connection error: PROTOCOL_ERROR\n") {
		d.t.Errorf("unexpected server diagnostic: %s", message)
	} else {
		select {
		case d.seen <- struct{}{}:
		default:
			d.t.Errorf("duplicate server diagnostic: %s", message)
		}
	}
	return len(p), nil
}

func TestTransportH2BodyDeadlineIsolatesStreams(t *testing.T) {
	done := make(chan error, 1)
	f, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err == nil {
				_, err = io.ReadAll(r.Body)
			}
			done <- err
		}
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{})
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, BlockFragment: headerBlock(t, "/stall")}))
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/ok")}))
	readStreamEnd(t, f, 3)
	if err := receive(t, done); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("body read: %v", err)
	}
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 5, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/still-ok")}))
	readStreamEnd(t, f, 5)
}

func readStreamEnd(t *testing.T, f *http2.Framer, id uint32) {
	t.Helper()
	for {
		frame, err := f.ReadFrame()
		must(t, err)
		switch frame := frame.(type) {
		case *http2.GoAwayFrame:
			t.Fatalf("unexpected GOAWAY: %v", frame.ErrCode)
		case *http2.RSTStreamFrame:
			if frame.StreamID == id {
				t.Fatalf("unexpected reset: %v", frame.ErrCode)
			}
		case *http2.HeadersFrame:
			if frame.StreamID == id && frame.StreamEnded() {
				return
			}
		case *http2.DataFrame:
			if frame.StreamID == id && frame.StreamEnded() {
				return
			}
		}
	}
}

func TestTransportH2WriteDeadlineIgnoresOtherStreamAndPing(t *testing.T) {
	done := make(chan error, 1)
	f, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			if err == nil {
				_, err = w.Write(make([]byte, 1<<20))
			}
			done <- err
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{}, http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0})
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/stall")}))
	for _, id := range []uint32{3, 5, 7} {
		must(t, f.WritePing(false, [8]byte{byte(id)}))
		must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/ok")}))
		readStreamEnd(t, f, id)
	}
	if err := receive(t, done); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("blocked write: %v", err)
	}
	must(t, f.WriteHeaders(http2.HeadersFrameParam{StreamID: 9, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/after")}))
	readStreamEnd(t, f, 9)
}

func TestTransportH2CUpgradeBodyStallBeforeApplication(t *testing.T) {
	called := make(chan struct{}, 1)
	s := &http.Server{ReadTimeout: 80 * time.Millisecond, Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called <- struct{}{} }), &http2.Server{})}
	c := serveProbe(t, s)
	_, err := io.WriteString(c, "POST / HTTP/1.1\r\nHost: probe\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: \r\nContent-Length: 5\r\n\r\nx")
	must(t, err)
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	must(t, err)
	defer r.Body.Close()
	if r.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %d", r.StatusCode)
	}
	select {
	case <-called:
		t.Fatal("application entered with incomplete upgrade body")
	default:
	}
}

func TestTransportTLSProtocols(t *testing.T) {
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	s.EnableHTTP2 = true
	must(t, http2.ConfigureServer(s.Config, &http2.Server{}))
	s.StartTLS()
	defer s.Close()
	for _, h2 := range []bool{false, true} {
		tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: h2} // Test server certificate only.
		client := &http.Client{Transport: tr, Timeout: probeGuard}
		r, err := client.Get(s.URL)
		must(t, err)
		r.Body.Close()
		tr.CloseIdleConnections()
		want := 1
		if h2 {
			want = 2
		}
		if r.ProtoMajor != want {
			t.Fatalf("want HTTP/%d, got %s", want, r.Proto)
		}
	}
}
