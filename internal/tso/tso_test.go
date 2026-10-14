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

package tso

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// stubTxnKV lets tests control Load/Save outcomes independently. All other
// TxnKV methods panic via the nil embedded interface, which is fine: the
// oracle only uses Load and Save.
type stubTxnKV struct {
	kv.TxnKV
	loadErr  error
	loadData string
	saveErr  error
	// The first transientLoadFailures Load calls fail with transientLoadErr,
	// then Load falls through to loadErr/loadData — modeling a backend
	// hiccup (etcd leader change, TiKV region move) during startup.
	transientLoadFailures int
	transientLoadErr      error
}

func (s *stubTxnKV) Load(ctx context.Context, key string) (string, error) {
	if s.transientLoadFailures > 0 {
		s.transientLoadFailures--
		return "", s.transientLoadErr
	}
	if s.loadErr != nil {
		return "", s.loadErr
	}
	return s.loadData, nil
}

func (s *stubTxnKV) Save(ctx context.Context, key, value string) error {
	return s.saveErr
}

// A transient read failure (anything but key-not-found) must fail
// InitTimestamp instead of being swallowed into ZeroTime: swallowing lets a
// restarting coord fall back to time.Now() below an already-persisted
// window, regressing TSO/ID monotonicity.
func TestInitTimestampFailsOnLoadError(t *testing.T) {
	oracle := &timestampOracle{
		key:          "tso-test",
		txnKV:        &stubTxnKV{loadErr: errors.New("io broken")},
		saveInterval: 3 * time.Second,
	}

	err := oracle.InitTimestamp()
	require.Error(t, err, "a failed window read must not silently reset TSO to now()")
}

// A transient backend hiccup during startup (an etcd leader change, a TiKV
// region move) must not fail coordinator initialization outright:
// InitTimestamp retries the window read a bounded number of times before
// surfacing the error.
func TestInitTimestampRetriesTransientLoadError(t *testing.T) {
	oracle := &timestampOracle{
		key: "tso-test",
		txnKV: &stubTxnKV{
			transientLoadFailures: 2,
			transientLoadErr:      errors.New("etcdserver: leader changed"),
		},
		saveInterval: 3 * time.Second,
	}

	require.NoError(t, oracle.InitTimestamp(),
		"bounded retry must absorb a transient window-read failure at init")
}

// A missing key means a fresh deployment with no persisted window: that is
// the one legitimate ZeroTime case and must keep working.
func TestLoadTimestampTreatsKeyNotFoundAsFresh(t *testing.T) {
	oracle := &timestampOracle{
		key:          "tso-test",
		txnKV:        &stubTxnKV{loadErr: merr.WrapErrIoKeyNotFound("tso-test")},
		saveInterval: 3 * time.Second,
	}

	last, err := oracle.loadTimestamp()
	require.NoError(t, err)
	require.Equal(t, typeutil.ZeroTime, last)
}
