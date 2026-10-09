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
	"net"
)

// CloseOnReadTimeout ensures that a connection whose protocol-sniffing read
// timed out cannot be handed to cmux's catch-all matcher as a live connection.
// This is a connection-level guard used only before a request/stream exists.
func CloseOnReadTimeout(listener net.Listener) net.Listener {
	return &closeOnReadTimeoutListener{Listener: listener}
}

type closeOnReadTimeoutListener struct{ net.Listener }

func (l *closeOnReadTimeoutListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &closeOnReadTimeoutConn{Conn: conn}, nil
}

type closeOnReadTimeoutConn struct{ net.Conn }

func (c *closeOnReadTimeoutConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		_ = c.Conn.Close()
	}
	return n, err
}
