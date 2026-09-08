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

package datacoord

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestStatsReconcileQueueGeneration(t *testing.T) {
	q := newStatsReconcileQueue(16, 2)
	now := time.Now()
	key := statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}
	require.True(t, q.enqueue(1, key, now, true))
	for range 100 {
		require.True(t, q.enqueue(1, key, now, true))
	}
	require.Len(t, q.pending, 1)
	work, ok := q.pop(now)
	require.True(t, ok)
	require.Len(t, q.pending, 1, "in-flight work still occupies capacity")
	require.True(t, q.enqueue(1, key, now, true))
	q.complete(work, false, now, time.Second, time.Minute)
	newer, ok := q.pop(now)
	require.True(t, ok, "completion must not clear a concurrent notification")
	require.Greater(t, newer.generation, work.generation)
	require.Equal(t, work.firstDirty, newer.firstDirty)
	q.complete(newer, false, now, time.Second, time.Minute)
	require.Empty(t, q.pending)
	require.Empty(t, q.perCollection)
}

func TestStatsReconcileQueueRetryAndFairness(t *testing.T) {
	q := newStatsReconcileQueue(64, 4)
	now := time.Now()
	for id := int64(1); id <= 3; id++ {
		q.notifySegment(id, id)
	}
	for n := 0; n < 6; n++ {
		work, ok := q.pop(time.Now())
		require.True(t, ok)
		require.Equal(t, n%2, statsJobSlot(work.key.subjob), "both ready subjobs alternate")
		q.complete(work, true, now, time.Second, 4*time.Second)
	}
	require.Len(t, q.pending, 6)
	_, ok := q.pop(now)
	require.False(t, ok, "a deferred key must not spin")
	require.Positive(t, q.delay(now))
	later := now.Add(time.Hour)
	for range 50 {
		work, ok := q.pop(later)
		require.True(t, ok)
		require.True(t, q.enqueue(work.collectionID, work.key, later, true))
		q.complete(work, true, later, time.Second, 4*time.Second)
		pending := q.pending[work.key]
		require.LessOrEqual(t, pending.notBefore.Sub(later), 4*time.Second)
		require.Equal(t, work.firstDirty, pending.firstDirty)
		later = later.Add(time.Hour)
	}
}

func TestStatsReconcileQueueOverflowAndScanGeneration(t *testing.T) {
	q := newStatsReconcileQueue(8, 1)
	now := time.Now()
	require.True(t, q.enqueue(1, statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, now, true))
	require.False(t, q.enqueue(1, statsReconcileKey{2, indexpb.StatsSubJob_TextIndexJob}, now, true))
	id, gen, ok := q.beginScan()
	require.True(t, ok)
	require.EqualValues(t, 1, id)
	q.requestScan(1, true)
	q.requestScan(2, true) // collection cap includes the active scan; promote to global.
	require.Len(t, q.scopes, 2)
	require.NotNil(t, q.scopes[0])
	q.finishScan(id, gen)
	require.Len(t, q.scanReady, 2, "active scan finishes before the newer generation starts")
	global, globalGen, ok := q.beginScan()
	require.True(t, ok)
	require.Zero(t, global)
	q.requestScan(0, false) // periodic requests must not keep a long scan permanently dirty.
	q.finishScan(global, globalGen)
	require.Nil(t, q.scopes[0])
	id, gen, ok = q.beginScan()
	require.True(t, ok)
	q.finishScan(id, gen)
	require.Empty(t, q.scopes)
	for id := int64(10); id < 100; id++ {
		q.enqueue(id, statsReconcileKey{id, indexpb.StatsSubJob_TextIndexJob}, now, true)
	}
	require.Len(t, q.pending, 8)
	require.LessOrEqual(t, len(q.scopes), 2)
}

func TestStatsReconcileQueueScanBackpressure(t *testing.T) {
	q := newStatsReconcileQueue(1, 1)
	now := time.Now()
	key := statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}
	q.enqueue(1, key, now, true)
	work, _ := q.pop(now)
	require.True(t, q.enqueue(1, key, now, false))
	require.Equal(t, work.generation, q.pending[key].generation, "scanning is not a metadata change")
	require.False(t, q.enqueue(1, statsReconcileKey{2, key.subjob}, now, false))
	require.Empty(t, q.scopes, "scanner backpressure must not recursively request scans")
	q.complete(work, false, now, time.Second, time.Minute)
	require.Empty(t, q.pending)
}

func TestStatsReconcileQueueConcurrentClose(t *testing.T) {
	q := newStatsReconcileQueue(128, 8)
	var wg sync.WaitGroup
	for producer := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := int64(1); id <= 1000; id++ {
				q.notifySegment(int64(producer+1), id)
				q.requestScan(int64(producer+1), true)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 1000 {
			if work, ok := q.pop(time.Now()); ok {
				q.complete(work, false, time.Now(), time.Second, time.Minute)
			}
		}
	}()
	q.close()
	wg.Wait()
	require.Empty(t, q.pending)
	require.Empty(t, q.scopes)
	require.False(t, q.enqueue(1, statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}, time.Now(), true))
}

func BenchmarkStatsReconcileDuplicate(b *testing.B) {
	q := newStatsReconcileQueue(16, 1)
	key := statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob}
	now := time.Now()
	q.enqueue(1, key, now, true)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		q.enqueue(1, key, now, true)
	}
}
