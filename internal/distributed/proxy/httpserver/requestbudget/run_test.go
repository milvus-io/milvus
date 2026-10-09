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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestRunInterruptsPendingBodyRead(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 60 * time.Millisecond, ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := Run(w, r, policy, time.Now(), http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
			_, readErr := io.Copy(io.Discard, req.Body)
			done <- readErr
		}))
		if err != nil {
			done <- err
		}
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: probe\r\nContent-Length: 100\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("pending body read = %v, want I/O deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending body read survived the request budget")
	}
}

func TestRunDoesNotPoisonNextKeepAliveRequest(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: 80 * time.Millisecond, ReadHeaderTimeout: time.Second}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Run(w, r, policy, time.Now(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})); err != nil {
			t.Errorf("Run: %v", err)
		}
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	for index := 0; index < 2; index++ {
		if index == 1 {
			time.Sleep(120 * time.Millisecond)
		}
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: probe\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("request %d: status = %d, want 204", index+1, response.StatusCode)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunInstallsHTTP2StreamDeadlines(t *testing.T) {
	policy := Policy{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: time.Second}
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Run(w, r, policy, time.Now(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})); err != nil {
			t.Errorf("Run on HTTP/2 stream: %v", err)
		}
	}), &http2.Server{}))
	defer server.Close()
	client := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(_ context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return net.Dial("tcp", server.Listener.Addr().String())
		},
	}}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 || response.StatusCode != http.StatusNoContent {
		t.Fatalf("response = %s %d, want HTTP/2 204", response.Proto, response.StatusCode)
	}
}

func TestRunSkipsCanceledRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	called := false
	err := Run(httptest.NewRecorder(), req, Policy{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: time.Second}, time.Now(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("error = %v, handler called = %t; want canceled without handler work", err, called)
	}
}

func TestRunRejectsUnsupportedDeadlineWriter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	called := false
	err := Run(httptest.NewRecorder(), req, Policy{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: time.Second}, time.Now(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	if !errors.Is(err, http.ErrNotSupported) || called {
		t.Fatalf("error = %v, handler called = %t; want fail-closed writer", err, called)
	}
}
