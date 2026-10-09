//go:build ignore

// Diagnostic, file-list-only characterization of the current implementation.
// This file is excluded from normal package builds because it supplies the
// non-timeout constants/helpers normally provided by constant.go and utils.go.
// It compiles the ORIGINAL timeout_middleware.go and json_render.go unchanged.
// Run from this directory:
// LOCAL_STORAGE_SIZE=1 go test -tags dynamic,test -gcflags='all=-N -l' \
//   -count=3 -v timeout_middleware.go json_render.go timeout_baseline_probe_test.go
// PASS means the documented current behavior was reproduced, NOT that an
// end-to-end timeout contract passed. This is not a full Milvus E2E test.

package httpserver

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/requestbudget"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Values and helper implementations are copied from this revision's
// constant.go and utils.go. No timeout, I/O, binder or renderer is stubbed.
const (
	ContextRequest          = "request"
	ContextResponse         = "response"
	HTTPReturnCode          = "code"
	HTTPReturnMessage       = "message"
	HTTPHeaderMilvusTraceID = "X-Milvus-Trace-Id"
)

func HTTPAbortReturn(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	if traceID, ok := getTraceID(c); ok {
		setTraceIDHeaderTo(c.Writer.Header(), traceID)
	}
	c.AbortWithStatusJSON(code, result)
}

func getTraceID(c *gin.Context) (string, bool) {
	if traceID, ok := c.Get("traceID"); ok {
		if s, ok := traceID.(string); ok && s != "" {
			return s, true
		}
	}
	if c.Request == nil {
		return "", false
	}
	id := oteltrace.SpanFromContext(c.Request.Context()).SpanContext().TraceID()
	if !id.IsValid() {
		return "", false
	}
	return id.String(), true
}

func setTraceIDHeaderTo(header http.Header, traceID string) {
	header.Set(HTTPHeaderMilvusTraceID, traceID)
}

const probeBudget = 100 * time.Millisecond

func TestActiveBudgetBypassesLegacyTimerAndBuffer(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.RequestTimeoutMs.Key
	if err := paramtable.Get().Save(key, "10"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/probe", timeoutMiddleware(func(c *gin.Context) {
		if !requestbudget.Active(c.Request.Context()) {
			t.Error("outer budget marker missing")
		}
		time.Sleep(30 * time.Millisecond)
		c.String(http.StatusOK, "done")
	}))
	policy := requestbudget.Policy{OverallTimeoutBudget: 200 * time.Millisecond, ReadHeaderTimeout: 5 * time.Millisecond}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := requestbudget.Run(w, r, policy, time.Now(), engine); err != nil {
			t.Errorf("outer budget: %v", err)
		}
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/probe")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != "done" {
		t.Fatalf("status=%d body=%q, want 200 done", response.StatusCode, body)
	}
}

func configureProbe(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.RequestTimeoutMs.Key
	if err := paramtable.Get().Save(key, strconv.FormatInt(probeBudget.Milliseconds(), 10)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func awaitProbe[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("did not observe %s", label)
		var zero T
		return zero
	}
}

func assertStillBlocked[T any](t *testing.T, ch <-chan T, label string) {
	t.Helper()
	select {
	case result := <-ch:
		t.Fatalf("%s unexpectedly completed: %v", label, result)
	case <-time.After(3 * probeBudget):
	}
}

type probeWriteResult struct {
	n   int
	err error
}

type probeWriter struct {
	http.ResponseWriter
	started chan time.Time
	ended   chan probeWriteResult
}

func (w *probeWriter) Write(data []byte) (int, error) {
	w.started <- time.Now()
	n, err := w.ResponseWriter.Write(data)
	w.ended <- probeWriteResult{n, err}
	return n, err
}

type probeServer struct {
	server       *httptest.Server
	writeStarted chan time.Time
	writeEnded   chan probeWriteResult
	finished     chan struct{}
}

func startProbeServer(t *testing.T, readTimeout, writeTimeout time.Duration, handler gin.HandlerFunc) *probeServer {
	t.Helper()
	engine := gin.New()
	engine.POST("/probe", timeoutMiddleware(handler))
	p := &probeServer{
		writeStarted: make(chan time.Time, 4),
		writeEnded:   make(chan probeWriteResult, 4),
		finished:     make(chan struct{}),
	}
	p.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(p.finished)
		engine.ServeHTTP(&probeWriter{w, p.writeStarted, p.writeEnded}, r)
	}))
	config := &paramtable.Get().HTTPCfg
	p.server.Config.ReadHeaderTimeout = config.ReadHeaderTimeout.GetAsDurationByParse()
	p.server.Config.ReadTimeout = readTimeout
	p.server.Config.WriteTimeout = writeTimeout
	p.server.Config.IdleTimeout = config.IdleTimeout.GetAsDurationByParse()
	p.server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetWriteBuffer(1024)
			}
		}
	}
	p.server.Start()
	t.Cleanup(p.server.Close)
	return p
}

func dialProbe(t *testing.T, p *probeServer) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", p.server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).SetReadBuffer(1024); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func sendProbe(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	if _, err := io.WriteString(conn, data); err != nil {
		t.Fatal(err)
	}
}

func TestBaseline_HeaderTimeIsOutsideRequestBudget(t *testing.T) {
	configureProbe(t)
	remaining := make(chan time.Duration, 1)
	p := startProbeServer(t, 0, 0, func(c *gin.Context) {
		deadline, _ := c.Request.Context().Deadline()
		remaining <- time.Until(deadline)
		c.String(http.StatusOK, "ok")
	})
	conn := dialProbe(t, p)
	started := time.Now()
	sendProbe(t, conn, "POST /probe HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n")
	assertStillBlocked(t, remaining, "handler before completed header")
	sendProbe(t, conn, "\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("got status %d", response.StatusCode)
	}
	left := awaitProbe(t, remaining, "handler deadline")
	if left <= 0 || left > probeBudget {
		t.Fatalf("unexpected remaining budget: %v", left)
	}
	t.Logf("budget=%v, header withheld=%v, total=%v, status=%d, handler received fresh remaining=%v", probeBudget, 3*probeBudget, time.Since(started), response.StatusCode, left)
}

func TestBaseline_BodyRead_DefaultVersusEnabledReadTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		read time.Duration
	}{{"default_zero", 0}, {"enabled_control", 250 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			configureProbe(t)
			requestContext := make(chan context.Context, 1)
			bindDone := make(chan error, 1)
			p := startProbeServer(t, tc.read, 0, func(c *gin.Context) {
				requestContext <- c.Request.Context()
				var payload map[string]any
				bindDone <- c.ShouldBindBodyWith(&payload, binding.JSON)
			})
			conn := dialProbe(t, p)
			started := time.Now()
			sendProbe(t, conn, "POST /probe HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{")
			ctx := awaitProbe(t, requestContext, "body reader entry")
			awaitProbe(t, ctx.Done(), "context deadline")
			// The middleware has two timers for the same duration: WithTimeout
			// and time.NewTimer followed by cancel(). Either may win.
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("unexpected context error: %v", ctx.Err())
			}
			if tc.read == 0 {
				assertStillBlocked(t, bindDone, "body read after context deadline")
				t.Logf("budget=%v, readTimeout=0, context=%v, body read still blocked at %v", probeBudget, ctx.Err(), time.Since(started))
				_ = conn.Close()
				t.Logf("test closed client; body read then returned: %v", awaitProbe(t, bindDone, "read cleanup"))
			} else {
				err := awaitProbe(t, bindDone, "read timeout control")
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("expected network timeout, got %v", err)
				}
				t.Logf("budget=%v, readTimeout=%v, body read ended at %v: %v", probeBudget, tc.read, time.Since(started), err)
				_ = conn.Close()
			}
			awaitProbe(t, p.finished, "outer handler cleanup")
		})
	}
}

func TestBaseline_CommitWrite_DefaultVersusEnabledWriteTimeout(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write time.Duration
	}{{"default_zero", 0}, {"enabled_control", 250 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			configureProbe(t)
			payload := bytes.Repeat([]byte{'x'}, 4<<20)
			requestContext := make(chan context.Context, 1)
			p := startProbeServer(t, 0, tc.write, func(c *gin.Context) {
				requestContext <- c.Request.Context()
				_, _ = c.Writer.Write(payload)
			})
			conn := dialProbe(t, p)
			started := time.Now()
			sendProbe(t, conn, "POST /probe HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
			ctx := awaitProbe(t, requestContext, "response handler entry")
			writeAt := awaitProbe(t, p.writeStarted, "actual response write")
			deadline, _ := ctx.Deadline()
			if !writeAt.Before(deadline) {
				t.Fatal("fixture failed: response must enter CommitTo before the request deadline")
			}
			awaitProbe(t, ctx.Done(), "context deadline")
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("unexpected context error: %v", ctx.Err())
			}
			if tc.write == 0 {
				assertStillBlocked(t, p.writeEnded, "actual response write after context deadline")
				t.Logf("budget=%v, writeTimeout=0, context=%v, actual Write still blocked at %v", probeBudget, ctx.Err(), time.Since(started))
				_ = conn.Close()
				result := awaitProbe(t, p.writeEnded, "write cleanup")
				t.Logf("test closed client; Write then returned n=%d, err=%v", result.n, result.err)
			} else {
				result := awaitProbe(t, p.writeEnded, "write timeout control")
				if !errors.Is(result.err, os.ErrDeadlineExceeded) {
					t.Fatalf("expected network timeout, got %v", result.err)
				}
				t.Logf("budget=%v, writeTimeout=%v, Write ended at %v, n=%d, err=%v", probeBudget, tc.write, time.Since(started), result.n, result.err)
				_ = conn.Close()
			}
			awaitProbe(t, p.finished, "outer handler cleanup")
		})
	}
}

type blockingProbeJSON struct {
	entered chan struct{}
	release chan struct{}
	exited  chan struct{}
}

func (b *blockingProbeJSON) MarshalJSON() ([]byte, error) {
	close(b.entered)
	<-b.release
	close(b.exited)
	return []byte(`{"ok":true}`), nil
}

func TestBaseline_TimeoutResponseDoesNotStopEncodingWork(t *testing.T) {
	configureProbe(t)
	value := &blockingProbeJSON{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(value.release) }) }
	t.Cleanup(release)
	engine := gin.New()
	handlerDone := make(chan struct{})
	engine.POST("/probe", timeoutMiddleware(func(c *gin.Context) {
		defer close(handlerDone)
		c.Render(http.StatusOK, jsonRender{Data: value})
	}))
	response := httptest.NewRecorder()
	outerDone := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(outerDone)
		engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/probe", nil))
	}()
	awaitProbe(t, value.entered, "JSON marshaler entry")
	awaitProbe(t, outerDone, "middleware timeout response")
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("expected timeout response, got %d: %s", response.Code, response.Body.String())
	}
	assertStillBlocked(t, value.exited, "JSON work after timeout response")
	t.Logf("budget=%v, HTTP status=%d, marshaler still running at %v; body=%s", probeBudget, response.Code, time.Since(started), response.Body.String())
	release()
	awaitProbe(t, handlerDone, "late encoding cleanup")
}

func TestProbe_DefaultConfigMatchesReviewedServer(t *testing.T) {
	configureProbe(t)
	config := &paramtable.Get().HTTPCfg
	read := config.ReadTimeout.GetAsDurationByParse()
	write := config.WriteTimeout.GetAsDurationByParse()
	if read != 0 || write != 0 {
		t.Fatal(fmt.Sprintf("reviewed defaults changed: read=%v write=%v", read, write))
	}
	t.Logf("server defaults: ReadHeaderTimeout=%v ReadTimeout=%v WriteTimeout=%v IdleTimeout=%v", config.ReadHeaderTimeout.GetAsDurationByParse(), read, write, config.IdleTimeout.GetAsDurationByParse())
}
