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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	tikvutil "github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

// newGCTestClient returns a client of a mock TiKV of its own, so that the
// safe points a test moves, which never go back, do not leak into others.
func newGCTestClient(t *testing.T) *txnkv.Client {
	client := tikvutil.SetupLocalTxn()
	t.Cleanup(func() { client.Close() })
	return client
}

// gcSafePoint returns the GC safe point that PD keeps.
func gcSafePoint(t *testing.T, client *txnkv.Client) uint64 {
	// a safe point of 0 never moves the safe point and returns the current one
	safePoint, err := client.GetPDClient().UpdateGCSafePoint(context.Background(), 0)
	require.NoError(t, err)
	return safePoint
}

func TestGCWorkerAdvance(t *testing.T) {
	Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60")
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	client := newGCTestClient(t)

	before := time.Now()
	safePoint, err := advanceGCSafePoint(context.Background(), client)
	require.NoError(t, err)
	assert.WithinDuration(t, before.Add(-time.Minute), oracle.GetTimeFromTS(safePoint), 5*time.Second)
	assert.Equal(t, safePoint, gcSafePoint(t, client))
	assert.Equal(t, float64(oracle.GetTimeFromTS(safePoint).Unix()), testutil.ToFloat64(metrics.MetaTiKVGCSafePoint))
}

// A service safe point older than the life time, such as one that a backup
// holds, keeps the GC safe point at it.
func TestGCWorkerAdvanceKeepsServiceSafePoint(t *testing.T) {
	Params.Save(Params.TiKVCfg.GCLifeTime.Key, "60")
	defer Params.Reset(Params.TiKVCfg.GCLifeTime.Key)
	client := newGCTestClient(t)
	ctx := context.Background()

	backup := oracle.GoTimeToTS(time.Now().Add(-time.Hour))
	_, err := client.GetPDClient().UpdateServiceGCSafePoint(ctx, "backup", 3600, backup)
	require.NoError(t, err)
	safePoint, err := advanceGCSafePoint(ctx, client)
	require.NoError(t, err)
	assert.Equal(t, backup, safePoint)
	assert.Equal(t, backup, gcSafePoint(t, client))

	// once the backup releases its service safe point, the GC safe point moves on
	_, err = client.GetPDClient().UpdateServiceGCSafePoint(ctx, "backup", 0, 0)
	require.NoError(t, err)
	safePoint, err = advanceGCSafePoint(ctx, client)
	require.NoError(t, err)
	assert.Greater(t, safePoint, backup)
}

func TestGCWorkerLoop(t *testing.T) {
	Params.Save(Params.TiKVCfg.GCInterval.Key, "1")
	defer Params.Reset(Params.TiKVCfg.GCInterval.Key)

	t.Run("enabled", func(t *testing.T) {
		Params.Save(Params.TiKVCfg.GCEnabled.Key, "true")
		defer Params.Reset(Params.TiKVCfg.GCEnabled.Key)
		client := newGCTestClient(t)
		stop := StartGCWorker(context.Background(), client)
		assert.Eventually(t, func() bool { return gcSafePoint(t, client) > 0 }, 10*time.Second, 100*time.Millisecond)
		stop()
	})

	t.Run("disabled", func(t *testing.T) {
		Params.Save(Params.TiKVCfg.GCEnabled.Key, "false")
		defer Params.Reset(Params.TiKVCfg.GCEnabled.Key)
		client := newGCTestClient(t)
		stop := StartGCWorker(context.Background(), client)
		time.Sleep(2500 * time.Millisecond)
		stop()
		assert.Equal(t, uint64(0), gcSafePoint(t, client))
	})
}
