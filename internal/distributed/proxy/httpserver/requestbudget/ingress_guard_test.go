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
	"net"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
)

// A timed-out HTTP/2 preface must not be passed to cmux's Any() fallback
// as a live connection that can keep the shared port occupied indefinitely.
func TestIngressGuardClosesPartialCMuxPrefaceOnReadTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	mux := cmux.New(CloseOnReadTimeout(listener))
	mux.SetReadTimeout(40 * time.Millisecond)
	mux.Match(cmux.HTTP2())
	fallback := mux.Match(cmux.Any())
	go mux.Serve()
	t.Cleanup(func() { mux.Close(); listener.Close() })
	go func() {
		for {
			conn, err := fallback.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, conn); _ = conn.Close() }()
		}
	}()

	client, err := net.Dial("tcp", listener.Addr().String())
	must(t, err)
	defer client.Close()
	must(t, client.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	_, err = io.WriteString(client, "PRI * HTTP/2.0\r\n")
	must(t, err)
	_, err = client.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("partial preface connection stayed writable")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("partial preface was not closed before client hang guard: %v", err)
	}
}

func TestIngressGuardPreservesCompletedProtocolMatches(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
		http2   bool
	}{
		{name: "HTTP2", payload: "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", http2: true},
		{name: "HTTP1", payload: "GET / HTTP/1.1\r\nHost: probe\r\n\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			must(t, err)
			mux := cmux.New(CloseOnReadTimeout(listener))
			mux.SetReadTimeout(200 * time.Millisecond)
			h2Listener := mux.Match(cmux.HTTP2())
			h1Listener := mux.Match(cmux.Any())
			go mux.Serve()
			t.Cleanup(func() { mux.Close(); listener.Close() })
			matched := make(chan net.Conn, 1)
			selected := h1Listener
			if test.http2 {
				selected = h2Listener
			}
			go func() {
				conn, err := selected.Accept()
				if err == nil {
					matched <- conn
				}
			}()
			client, err := net.Dial("tcp", listener.Addr().String())
			must(t, err)
			defer client.Close()
			_, err = io.WriteString(client, test.payload)
			must(t, err)
			conn := receive(t, matched)
			defer conn.Close()
			must(t, conn.SetReadDeadline(time.Now().Add(probeGuard)))
			readBack := make([]byte, len(test.payload))
			_, err = io.ReadFull(conn, readBack)
			must(t, err)
			if string(readBack) != test.payload {
				t.Fatalf("replayed payload = %q, want %q", readBack, test.payload)
			}
		})
	}
}
