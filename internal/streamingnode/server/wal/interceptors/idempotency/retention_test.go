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

package idempotency

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPChannelRetentionRefreshesTTLAndLRU(t *testing.T) {
	now := time.Now()
	m := newWindowManager(WindowConfig{TTL: time.Hour, MaxSize: 18, Now: func() time.Time { return now }})
	a, b := m.newWindow("a"), m.newWindow("b")
	completeKey(t, a, "a", 1) // Each fact costs 1 byte of key + 8 bytes of TT.
	now = now.Add(10 * time.Minute)
	completeKey(t, b, "b", 2)
	now = now.Add(40 * time.Minute)
	require.Equal(t, BeginDecisionDuplicate, a.Begin("a", nil).Decision)
	completeKey(t, b, "c", 3)
	require.Contains(t, a.entries, IdempotencyKey("a"))
	require.NotContains(t, b.entries, IdempotencyKey("b"), "access refresh must move a behind b in global LRU")
	now = now.Add(59 * time.Minute)
	require.Equal(t, BeginDecisionDuplicate, a.Begin("a", nil).Decision)
	now = now.Add(time.Hour)
	require.Equal(t, BeginDecisionOwner, a.Begin("a", nil).Decision, "TTL expires at the exact idle boundary")
	// Expiring committed records must not discard the new pending owner.
	owner := a.inflight["a"]
	now = now.Add(2 * time.Hour)
	require.Equal(t, BeginDecisionWait, a.Begin("a", nil).Decision)
	require.Same(t, owner, a.inflight["a"])
}

func TestPChannelRetentionSoftMinCount(t *testing.T) {
	m := newWindowManager(WindowConfig{MaxSize: 36, MinCount: 2})
	cold, hot := m.newWindow("cold"), m.newWindow("hot")
	completeKey(t, cold, "a", 1)
	for _, key := range []IdempotencyKey{"b", "c", "d", "e"} {
		completeKey(t, hot, key, 2)
	}
	require.Contains(t, cold.entries, IdempotencyKey("a"), "reclaim an over-floor channel before a protected old channel")
	require.NotContains(t, hot.entries, IdempotencyKey("b"))
	require.Len(t, hot.entries, 3)
	require.Equal(t, 36, m.bytes)
	require.Equal(t, BeginDecisionDuplicate, hot.Begin("c", nil).Decision)
	completeKey(t, hot, "f", 3)
	require.NotContains(t, hot.entries, IdempotencyKey("d"), "the candidate heap must track the new VChannel LRU head")

	m = newWindowManager(WindowConfig{MaxSize: 18, MinCount: 16})
	for _, key := range []string{"a", "b", "c"} {
		completeKey(t, m.newWindow(key), IdempotencyKey(key), 1)
	}
	require.Equal(t, 18, m.bytes, "global capacity overrides unsatisfiable soft floors")
	require.Equal(t, "b", m.order.Front().Value.(*cachedRecord).window.vchannel)

	now := time.Now()
	m = newWindowManager(WindowConfig{TTL: time.Hour, MinCount: 16, Now: func() time.Time { return now }})
	w := m.newWindow("ttl")
	completeKey(t, w, "a", 1)
	now = now.Add(time.Hour)
	require.Equal(t, BeginDecisionOwner, w.Begin("a", nil).Decision, "TTL overrides a soft floor")

	m = newWindowManager(WindowConfig{MaxSize: 1, MinCount: 16})
	w = m.newWindow("oversized")
	completeKey(t, w, "a", 1)
	require.Zero(t, w.Len(), "an indivisible record exceeding the entire budget cannot remain resident")
	require.Zero(t, m.bytes)
}

func TestPChannelRestoreDoesNotPersistAccessRefresh(t *testing.T) {
	now := time.Now()
	config := WindowConfig{TTL: time.Hour, MaxSize: 100, MinCount: 16, Now: func() time.Time { return now }}
	snapshots := map[string]*idempotencyview.Snapshot{
		"a": {VChannel: "a", Records: []*idempotencyview.Record{{IdempotencyKey: "a", SourceTimeTick: tsoutil.ComposeTSByTime(now.Add(-30 * time.Minute))}}},
		"b": {VChannel: "b", Records: []*idempotencyview.Record{{IdempotencyKey: "b", SourceTimeTick: tsoutil.ComposeTSByTime(now.Add(-10 * time.Minute))}}},
	}
	m := newWindowManager(config)
	windows := m.restore(snapshots)
	require.Equal(t, BeginDecisionDuplicate, windows["a"].Begin("a", nil).Decision)
	now = now.Add(40 * time.Minute)
	require.Equal(t, BeginDecisionDuplicate, windows["a"].Begin("a", nil).Decision)
	recovered := newWindowManager(config).restore(snapshots)
	require.Zero(t, recovered["a"].Len(), "reopen must use original commit age, not the process-local last access")
	require.Equal(t, 1, recovered["b"].Len())

	config.MaxSize = 9
	config.TTL = 0
	limited := newWindowManager(config)
	restored := limited.restore(snapshots)
	require.Zero(t, restored["a"].Len())
	require.Equal(t, 1, restored["b"].Len(), "the shared restore budget cannot be multiplied by the number of VChannels")
	require.Equal(t, 9, limited.bytes)
}

func TestPChannelRetentionIdleCleanupAndShutdown(t *testing.T) {
	paramtable.Init()
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	interceptor := newIdempotencyInterceptor(WindowConfig{
		TTL: 20 * time.Millisecond, MaxSize: 1024 * 1024, MinCount: 16,
		Now: func() time.Time { return time.Unix(0, clock.Load()) },
	})
	defer interceptor.Close()
	w := interceptor.window("idle")
	for i := 0; i < 600; i++ {
		completeKey(t, w, IdempotencyKey(fmt.Sprint(i)), uint64(i+1))
	}
	require.Equal(t, 600, w.Len())
	clock.Add(int64(20 * time.Millisecond))
	require.Eventually(t, func() bool { return w.Len() == 0 }, 5*time.Second, 5*time.Millisecond,
		"expired records must be reclaimed without another append")
	interceptor.cache.shutdown()
	select {
	case <-interceptor.cache.done:
	default:
		t.Fatal("shutdown did not join the cleanup worker")
	}
}

func TestPChannelRetentionConcurrentWindows(t *testing.T) {
	m := newWindowManager(WindowConfig{TTL: time.Hour, MaxSize: 512, MinCount: 2})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		w := m.newWindow(fmt.Sprint(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := IdempotencyKey(fmt.Sprintf("%04d", j))
				begin := w.Begin(key, nil)
				if begin.Decision == BeginDecisionOwner {
					w.Complete(begin.Pending, CommitResult{CommitTimeTick: uint64(j)}, nil)
				}
			}
		}()
	}
	wg.Wait()
	require.LessOrEqual(t, m.bytes, 512)
	accounted := 0
	for elem := m.order.Front(); elem != nil; elem = elem.Next() {
		accounted += elem.Value.(*cachedRecord).size
	}
	require.Equal(t, accounted, m.bytes)
}

func BenchmarkPChannelRetentionEviction(b *testing.B) {
	for _, count := range []int{1000, 30000} {
		b.Run(fmt.Sprintf("protected=%d", count), func(b *testing.B) {
			m := newWindowManager(WindowConfig{MaxSize: (count + 32) * 24, MinCount: 16})
			for i := 0; i < count; i++ {
				w := m.newWindow(fmt.Sprint(i))
				begin := w.Begin("0000000000000000", nil)
				w.Complete(begin.Pending, CommitResult{}, nil)
			}
			hot := m.newWindow("hot")
			for i := 0; i < 32; i++ {
				begin := hot.Begin(IdempotencyKey(fmt.Sprintf("%016d", i)), nil)
				hot.Complete(begin.Pending, CommitResult{}, nil)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := IdempotencyKey(fmt.Sprintf("%016d", i+32))
				begin := hot.Begin(key, nil)
				hot.Complete(begin.Pending, CommitResult{}, nil)
			}
		})
	}
}
