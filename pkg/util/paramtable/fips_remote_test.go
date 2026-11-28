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

package paramtable

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A rejected Go FIPS check must stop remote configuration before it dials etcd.
func TestRemoteConfigChecksGoFIPSBeforeConnecting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			conn.Close()
		}
	}()

	t.Setenv("ETCD_ENDPOINTS", ln.Addr().String())
	t.Setenv("ETCD_USE_EMBED", "false")
	t.Setenv("ETCD_DIALTIMEOUT", "200")

	origRequire, origExit := requireGoFIPS, exitProcess
	t.Cleanup(func() { requireGoFIPS, exitProcess = origRequire, origExit })

	t.Run("rejected", func(t *testing.T) {
		requireGoFIPS = func() error { return errors.New("Go FIPS 140-3 mode is off") }
		exitCode := -1
		exitProcess = func(code int) { exitCode = code }

		bt := NewBaseTable()

		assert.Equal(t, 1, exitCode)
		assert.Nil(t, bt.etcdClient)
		assert.Never(t, func() bool { return accepted.Load() > 0 }, 300*time.Millisecond, 20*time.Millisecond)
	})

	// Control: with the check passing, the same configuration does dial etcd,
	// so the rejected case above would have observed a connection.
	t.Run("accepted", func(t *testing.T) {
		requireGoFIPS = func() error { return nil }
		exitProcess = func(code int) { t.Fatalf("unexpected exit %d", code) }

		NewBaseTable()

		assert.Eventually(t, func() bool { return accepted.Load() > 0 }, 5*time.Second, 20*time.Millisecond)
	})
}
