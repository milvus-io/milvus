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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestApplyIODeadlineInterruptsPendingBodyRead(t *testing.T) {
	done := make(chan error, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := ApplyIODeadline(w, time.Now().Add(80*time.Millisecond))
		if err == nil {
			_, err = io.Copy(io.Discard, r.Body)
		}
		done <- err
	})}
	client := serveProbe(t, server)
	_, err := io.WriteString(client, "POST / HTTP/1.1\r\nHost: probe\r\nContent-Length: 100\r\n\r\n{")
	must(t, err)
	if err := receive(t, done); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("body read after deadline: %v", err)
	}
}

func TestApplyIODeadlineRejectsUnsupportedWriter(t *testing.T) {
	err := ApplyIODeadline(httptest.NewRecorder(), time.Now().Add(time.Second))
	if !errors.Is(err, http.ErrNotSupported) || !errors.Is(err, merr.ErrServiceInternal) {
		t.Fatalf("unsupported writer error = %v, want typed system error preserving http.ErrNotSupported", err)
	}
}

func TestApplyIODeadlineInterruptsOnlyBlockedHTTP2Stream(t *testing.T) {
	done := make(chan error, 1)
	frames, _ := h2Probe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			err := ApplyIODeadline(w, time.Now().Add(100*time.Millisecond))
			if err == nil {
				_, err = w.Write(make([]byte, 1<<20))
			}
			done <- err
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}), &http2.Server{}, http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0})
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/stall")}))
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/ok")}))
	readStreamEnd(t, frames, 3)
	if err := receive(t, done); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("blocked stream write: %v", err)
	}
	must(t, frames.WriteHeaders(http2.HeadersFrameParam{StreamID: 5, EndHeaders: true, EndStream: true, BlockFragment: headerBlock(t, "/after")}))
	readStreamEnd(t, frames, 5)
}
