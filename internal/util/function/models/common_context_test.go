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

package models

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

func TestPostRequestCanceledContext(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := PostRequest[struct{}](ctx, nil, server.URL, nil, 0)
	if !errors.Is(err, context.Canceled) || requests.Load() != 0 {
		t.Fatalf("expected cancellation without a request, got err=%v requests=%d", err, requests.Load())
	}
}

func TestPostRequestInFlightCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := PostRequest[struct{}](ctx, nil, server.URL, nil, 10000)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not stop after cancellation")
	}
}

func TestPostRequestDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parentTimeout time.Duration
		modelTimeout  int64
	}{
		{"parent_deadline", 50 * time.Millisecond, 10000},
		{"model_timeout", 10 * time.Second, 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), tc.parentTimeout)
			defer cancel()
			start := time.Now()
			_, err := PostRequest[struct{}](ctx, nil, server.URL, nil, tc.modelTimeout)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline exceeded, got %v", err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("request exceeded the earlier deadline")
			}
		})
	}
}

func TestRetrySendCancellation(t *testing.T) {
	for _, maxRetries := range []int{1, 3} {
		t.Run(strconv.Itoa(maxRetries), func(t *testing.T) {
			var requests atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				cancel()
			}))
			defer server.Close()
			_, err := retrySend(ctx, nil, http.MethodPost, server.URL, nil, maxRetries)
			if !errors.Is(err, context.Canceled) || requests.Load() != 1 {
				t.Fatalf("expected canceled retry, got err=%v requests=%d", err, requests.Load())
			}
		})
	}
}

func TestPostRequestCancellationDuringBackoff(t *testing.T) {
	var requests atomic.Int32
	failed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
		close(failed)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := PostRequest[struct{}](ctx, nil, server.URL, nil, 10000)
		done <- err
	}()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	// The first retry waits at least one second. Cancel inside that backoff.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || requests.Load() != 1 {
			t.Fatalf("expected canceled backoff, got err=%v requests=%d", err, requests.Load())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry backoff did not stop after cancellation")
	}
}

func TestPostRequestTransientRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"value":42}`))
	}))
	defer server.Close()
	response, err := PostRequest[struct {
		Value int `json:"value"`
	}](context.Background(), nil, server.URL, nil, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Value != 42 || requests.Load() != 2 {
		t.Fatalf("expected successful retry, got response=%+v requests=%d", response, requests.Load())
	}
}
