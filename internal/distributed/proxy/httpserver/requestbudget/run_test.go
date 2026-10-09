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

package requestbudget

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2/h2c"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRunBudgetInterruptsPendingBodyRead(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 60 * time.Millisecond, ReadHeaderTimeout: 20 * time.Millisecond}
	type result struct {
		readErr error
		ctxErr  error
	}
	finished := make(chan result, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := Run(w, r, policy, time.Now(), http.HandlerFunc(func(_ http.ResponseWriter, budgetReq *http.Request) {
			_, readErr := io.Copy(io.Discard, budgetReq.Body)
			finished <- result{readErr: readErr, ctxErr: budgetReq.Context().Err()}
		}))
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	})}
	client := serveProbe(t, server)
	_, err := io.WriteString(client, "POST / HTTP/1.1\r\nHost: probe\r\nContent-Length: 100\r\n\r\n{")
	must(t, err)
	got := receive(t, finished)
	if !errors.Is(got.readErr, os.ErrDeadlineExceeded) {
		t.Fatalf("pending body read error = %v, want I/O deadline exceeded", got.readErr)
	}
	// net/http may cancel the request context as soon as the timed-out body
	// read fails, racing with the context's own timer at the same deadline.
	if !errors.Is(got.ctxErr, context.DeadlineExceeded) && !errors.Is(got.ctxErr, context.Canceled) {
		t.Fatalf("request context error = %v, want a canceled context", got.ctxErr)
	}
}

func TestRunBudgetDoesNotPoisonNextKeepAliveRequest(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 500 * time.Millisecond, ReadHeaderTimeout: 100 * time.Millisecond}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Run(w, r, policy, time.Now(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})); err != nil {
			t.Errorf("Run: %v", err)
		}
	})}
	client := serveProbe(t, server)
	reader := bufio.NewReader(client)
	for _, path := range []string{"/first", "/second"} {
		_, err := io.WriteString(client, "GET "+path+" HTTP/1.1\r\nHost: probe\r\n\r\n")
		must(t, err)
		response, err := http.ReadResponse(reader, nil)
		must(t, err)
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("%s status = %d, want 204", path, response.StatusCode)
		}
		must(t, response.Body.Close())
	}
}

func TestHTTP1ShortClientTimeoutIsNotPreexpiredByHeaderGuard(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 120 * time.Second, ReadHeaderTimeout: 5 * time.Second}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Run(w, r, policy, StartedAt(r), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})); err != nil {
			t.Errorf("unexpected pre-handler rejection: %v", err)
			w.WriteHeader(http.StatusRequestTimeout)
		}
	})}
	client := serveProbe(t, server)
	reader := bufio.NewReader(client)
	_, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: probe\r\nRequest-Timeout: 1\r\n\r\n")
	must(t, err)
	response, err := http.ReadResponse(reader, nil)
	must(t, err)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.StatusCode)
	}
	must(t, response.Body.Close())
}

func TestHTTP2HeaderReadDoesNotConsumePostHeaderBudget(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 50 * time.Millisecond, ReadHeaderTimeout: 500 * time.Millisecond}
	called := make(chan bool, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := Run(w, r, policy, StartedAt(r), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called <- true
			w.WriteHeader(http.StatusNoContent)
		}))
		if err != nil {
			called <- false
			w.WriteHeader(http.StatusRequestTimeout)
		}
	})
	server := &http.Server{Handler: h2c.NewHandler(handler, &http2.Server{}), ReadHeaderTimeout: policy.ReadHeaderTimeout}
	client := serveProbe(t, server)
	frames, _ := h2Frames(t, client)
	block := headerBlock(t, "/slow-header")
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndStream: true, EndHeaders: false, BlockFragment: block[:1]}))
	time.Sleep(100 * time.Millisecond)
	must(t, frames.WriteContinuation(1, true, block[1:]))
	if !receive(t, called) {
		t.Fatal("post-header handler was rejected because header time consumed its budget")
	}
	readStreamEnd(t, frames, 1)
}

func TestHTTP2QueuedHandlerConsumesPostHeaderBudget(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 50 * time.Millisecond, ReadHeaderTimeout: 30 * time.Millisecond}
	holding := make(chan struct{}, 1)
	release := make(chan struct{})
	result := make(chan bool, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			holding <- struct{}{}
			<-release
			return
		}
		err := Run(w, r, policy, StartedAt(r), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			result <- false
			w.WriteHeader(http.StatusNoContent)
		}))
		if err != nil {
			result <- errors.Is(err, context.DeadlineExceeded)
			w.WriteHeader(http.StatusRequestTimeout)
		}
	})
	frames, _ := h2Probe(t, handler, &http2.Server{MaxConcurrentStreams: 1})
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/hold")}))
	receive(t, holding)
	must(t, frames.WriteRSTStream(1, http2.ErrCodeCancel))
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/queued")}))
	must(t, frames.WritePing(false, [8]byte{73}))
	for {
		frame, err := frames.ReadFrame()
		must(t, err)
		if ping, ok := frame.(*http2.PingFrame); ok && ping.IsAck() {
			break
		}
	}
	time.Sleep(80 * time.Millisecond)
	close(release)
	if !receive(t, result) {
		t.Fatal("HTTP/2 queued handler received a fresh budget after its headers completed")
	}
	readStreamEnd(t, frames, 3)
}

func TestRunBudgetRejectsExpiredAndInvalidRequestsBeforeHandler(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: 100 * time.Millisecond}
	for _, test := range []struct {
		name           string
		startedAt      time.Time
		requestTimeout string
		want           error
	}{
		{name: "expired", startedAt: time.Now().Add(-2 * time.Second), want: context.DeadlineExceeded},
		{name: "invalid header", startedAt: time.Now(), requestTimeout: "0", want: merr.ErrParameterInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Request-Timeout", test.requestTimeout)
			err := Run(httptest.NewRecorder(), req, policy, test.startedAt, http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				called = true
			}))
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if called {
				t.Fatal("handler ran after request was rejected")
			}
		})
	}
}

func TestRunBudgetDoesNotEnterHandlerAfterParentCancellation(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	called := false
	err := Run(httptest.NewRecorder(), req, policy, time.Now(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if called {
		t.Fatal("handler ran after its parent context was canceled")
	}
}

func TestRunBudgetDoesNotExpireSiblingHTTP2Stream(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 80 * time.Millisecond, ReadHeaderTimeout: 30 * time.Millisecond}
	finished := make(chan error, 1)
	frames, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := Run(w, r, policy, StartedAt(r), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/stall" {
				_, readErr := io.Copy(io.Discard, r.Body)
				finished <- readErr
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		if err != nil {
			t.Errorf("Run %s: %v", r.URL.Path, err)
		}
	}), &http2.Server{})
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, BlockFragment: headerBlock(t, "/stall")}))
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/ok")}))
	readStreamEnd(t, frames, 3)
	if err := receive(t, finished); err == nil {
		t.Fatal("stalled HTTP/2 body read survived its request deadline")
	}
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 5, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/after")}))
	readStreamEnd(t, frames, 5)
}

func TestUpgradeBodyDeadlineDoesNotLeakIntoHTTP2Connection(t *testing.T) {
	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h2c.IsH2CUpgrade(r.Header) {
			startedAt := time.Now()
			if err := ApplyIODeadline(w, startedAt.Add(80*time.Millisecond)); err != nil {
				t.Errorf("upgrade I/O deadline: %v", err)
				return
			}
			r = WithUpgradeStart(r, startedAt)
		}
		handler.ServeHTTP(w, r)
	}), ReadHeaderTimeout: time.Second}
	client := serveProbe(t, server)
	reader := bufio.NewReader(client)
	_, err := io.WriteString(client, "GET /first HTTP/1.1\r\nHost: probe\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: \r\n\r\n")
	must(t, err)
	response, err := http.ReadResponse(reader, nil)
	must(t, err)
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", response.StatusCode)
	}
	must(t, response.Body.Close())
	_, err = io.WriteString(client, http2.ClientPreface)
	must(t, err)
	frames := http2.NewFramer(client, reader)
	must(t, frames.WriteSettings())
	// A fresh stream must still work after the first request's deadline.
	time.Sleep(120 * time.Millisecond)
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/after")}))
	readStreamEnd(t, frames, 3)
}

func TestUpgradeBodyReadStopsAtRequestDeadline(t *testing.T) {
	called := make(chan struct{}, 1)
	returned := make(chan time.Duration, 1)
	handler := h2c.NewHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called <- struct{}{}
	}), &http2.Server{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		if err := ApplyIODeadline(w, startedAt.Add(60*time.Millisecond)); err != nil {
			t.Errorf("upgrade I/O deadline: %v", err)
			return
		}
		handler.ServeHTTP(w, r)
		returned <- time.Since(startedAt)
	})}
	client := serveProbe(t, server)
	_, err := io.WriteString(client, "POST /first HTTP/1.1\r\nHost: probe\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: \r\nContent-Length: 100\r\n\r\nx")
	must(t, err)
	if elapsed := receive(t, returned); elapsed < 40*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("upgrade body read ended after %s, want near 60ms", elapsed)
	}
	select {
	case <-called:
		t.Fatal("application entered with an incomplete Upgrade body")
	default:
	}
}
