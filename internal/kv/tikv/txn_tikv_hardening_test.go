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

package tikv

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func txnLatencySum(t *testing.T) float64 {
	h, ok := metrics.MetaRequestLatency.WithLabelValues(metrics.MetaTxnLabel).(prometheus.Histogram)
	require.True(t, ok)
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram().GetSampleSum()
}

// MultiLoad must not mutate the caller's keys slice: callers may legally
// reuse the same slice across calls (e.g. on retry), and an in-place
// rootPath-prefix rewrite makes every subsequent call double-prefixed.
func TestMultiLoadDoesNotMutateCallerKeys(t *testing.T) {
	rootPath := "/tikv/test/root/multiload_no_mutation"
	kv := NewTiKV(txnClient, rootPath)
	err := kv.RemoveWithPrefix(context.TODO(), "")
	require.NoError(t, err)

	defer kv.Close()
	defer kv.RemoveWithPrefix(context.TODO(), "")

	err = kv.MultiSave(context.TODO(), map[string]string{"key_a": "1", "key_b": "2"})
	require.NoError(t, err)

	keys := []string{"key_a", "key_b"}

	vals, err := kv.MultiLoad(context.TODO(), keys)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, vals)

	// The caller's slice must be untouched after the call.
	require.Equal(t, []string{"key_a", "key_b"}, keys)

	// Reusing the same slice must yield the same result, not a
	// double-prefixed miss.
	vals, err = kv.MultiLoad(context.TODO(), keys)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, vals)

	// A missing key must be reported under the caller's original name,
	// not the rootPath-prefixed lookup path.
	mixed := []string{"key_a", "key_missing"}
	_, err = kv.MultiLoad(context.TODO(), mixed)
	require.Error(t, err)
	require.ErrorContains(t, err, "key_missing")
	require.NotContains(t, err.Error(), rootPath)
	require.Equal(t, []string{"key_a", "key_missing"}, mixed)
}

// The txn latency histogram must measure the commit itself: a commit taking
// N ms must observe at least N ms, not a near-zero pre-commit elapsed.
func TestExecuteTxnLatencyMeasuresCommit(t *testing.T) {
	rootPath := "/tikv/test/root/txn_latency"
	kv := NewTiKV(txnClient, rootPath)
	defer kv.Close()
	defer kv.RemoveWithPrefix(context.TODO(), "")

	origCommit := commitTxn
	commitTxn = func(ctx context.Context, txn *transaction.KVTxn) error {
		time.Sleep(60 * time.Millisecond)
		return origCommit(ctx, txn)
	}
	defer func() { commitTxn = origCommit }()

	before := txnLatencySum(t)
	err := kv.MultiSave(context.TODO(), map[string]string{"lat_key": "v"})
	require.NoError(t, err)
	after := txnLatencySum(t)

	require.GreaterOrEqual(t, after-before, 55.0,
		"txn latency histogram must include commit duration")
}

// Prefix reads must honor caller cancellation: a caller that has already
// given up (timeout, disconnect, shutdown) must not leave an unkillable
// scan running against TiKV. The snapshot iterator does not accept a
// context (its internal backoffer uses context.Background()), so the kv
// layer has to check the caller context itself.
func TestPrefixReadsHonorCanceledContext(t *testing.T) {
	rootPath := "/tikv/test/root/prefix_cancel"
	kv := NewTiKV(txnClient, rootPath)
	err := kv.RemoveWithPrefix(context.TODO(), "")
	require.NoError(t, err)

	defer kv.Close()
	defer kv.RemoveWithPrefix(context.TODO(), "")

	err = kv.MultiSave(context.TODO(), map[string]string{
		"scan/a": "1", "scan/b": "2", "scan/c": "3",
	})
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("LoadWithPrefix", func(t *testing.T) {
		_, _, err := kv.LoadWithPrefix(canceled, "scan")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("HasPrefix", func(t *testing.T) {
		_, err := kv.HasPrefix(canceled, "scan")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WalkWithPrefix", func(t *testing.T) {
		err := kv.WalkWithPrefix(canceled, "scan", 1, func(k, v []byte) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})

	// An empty prefix must not bypass the cancellation check: the scan loop
	// never runs, so the entry check has to catch it.
	t.Run("LoadWithPrefix_empty_prefix", func(t *testing.T) {
		_, _, err := kv.LoadWithPrefix(canceled, "no_such_prefix")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("WalkWithPrefix_empty_prefix", func(t *testing.T) {
		err := kv.WalkWithPrefix(canceled, "no_such_prefix", 1, func(k, v []byte) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})
}

// Regression guard for the write-path prefix scan: a canceled caller must
// get context.Canceled back and must not lose keys. The pre-canceled case
// is already caught by retry.Do's entry check before the scan starts; the
// in-loop poll added alongside mirrors the read-path scans as best-effort
// early abort for cancellation arriving mid-scan (not deterministically
// testable from the public API).
func TestMultiSaveAndRemoveWithPrefixHonorsCanceledContext(t *testing.T) {
	rootPath := "/tikv/test/root/prefix_delete_cancel"
	kv := NewTiKV(txnClient, rootPath)
	err := kv.RemoveWithPrefix(context.TODO(), "")
	require.NoError(t, err)

	defer kv.Close()
	defer kv.RemoveWithPrefix(context.TODO(), "")

	err = kv.MultiSave(context.TODO(), map[string]string{
		"scan/a": "1", "scan/b": "2", "scan/c": "3",
	})
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	err = kv.MultiSaveAndRemoveWithPrefix(canceled, nil, []string{"scan"})
	require.ErrorIs(t, err, context.Canceled)

	// The scan must have been aborted before any delete took effect.
	keys, _, err := kv.LoadWithPrefix(context.TODO(), "scan")
	require.NoError(t, err)
	require.Len(t, keys, 3, "a canceled prefix delete must not remove keys")
}
