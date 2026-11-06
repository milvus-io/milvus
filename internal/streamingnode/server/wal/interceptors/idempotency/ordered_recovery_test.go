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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func orderedRecoveryFixture(count, channels int, now time.Time) *idempotencyview.RecoverySnapshot {
	snapshot := &idempotencyview.RecoverySnapshot{VChannels: make(map[string]*idempotencyview.Snapshot)}
	base := tsoutil.ComposeTSByTime(now.Add(-time.Minute))
	for start := 0; start < count; start += 1024 {
		byVC := make(map[string]*idempotencyview.Snapshot)
		var sections []*idempotencyview.Snapshot
		for i := start; i < min(start+1024, count); i++ {
			vc := fmt.Sprintf("vc-%d", i%channels)
			section := byVC[vc]
			if section == nil {
				section = &idempotencyview.Snapshot{VChannel: vc}
				byVC[vc] = section
				sections = append(sections, section)
			}
			record := &idempotencyview.Record{IdempotencyKey: fmt.Sprint(i), SourceTimeTick: base + uint64(i)}
			section.Records = append(section.Records, record)
			legacy := snapshot.VChannels[vc]
			if legacy == nil {
				legacy = &idempotencyview.Snapshot{VChannel: vc}
				snapshot.VChannels[vc] = legacy
			}
			legacy.Records = append(legacy.Records, record)
		}
		snapshot.Chunks = append(snapshot.Chunks, sections)
	}
	return snapshot
}

func TestOrderedWindowRestoreMatchesLegacy(t *testing.T) {
	now := time.Now()
	snapshot := orderedRecoveryFixture(200, 4, now)
	for _, minCount := range []int{0, 16} {
		config := WindowConfig{TTL: time.Hour, MaxSize: 600, MinCount: minCount, Now: func() time.Time { return now }}
		ordered, legacy := newWindowManager(config), newWindowManager(config)
		windows := ordered.restoreOrdered(snapshot)
		expected := legacy.restore(snapshot.VChannels)
		require.Equal(t, legacy.bytes, ordered.bytes)
		require.Equal(t, legacy.order.Len(), ordered.order.Len())
		left, right := ordered.order.Front(), legacy.order.Front()
		for left != nil {
			a, b := left.Value.(*cachedRecord), right.Value.(*cachedRecord)
			require.Same(t, b.Record, a.Record)
			require.Equal(t, b.at, a.at)
			left, right = left.Next(), right.Next()
		}
		for vc, w := range windows {
			require.Equal(t, expected[vc].Len(), w.Len())
		}
	}
}

func TestOrderedWindowRestorePreservesAge(t *testing.T) {
	now := time.Now()
	section := &idempotencyview.Snapshot{VChannel: "v1", Records: []*idempotencyview.Record{
		{IdempotencyKey: "old", SourceTimeTick: tsoutil.ComposeTSByTime(now.Add(-2 * time.Hour))},
		{IdempotencyKey: "future", SourceTimeTick: tsoutil.ComposeTSByTime(now.Add(time.Minute))},
	}}
	manager := newWindowManager(WindowConfig{TTL: time.Hour, Now: func() time.Time { return now }})
	windows := manager.restoreOrdered(&idempotencyview.RecoverySnapshot{Chunks: [][]*idempotencyview.Snapshot{{section}}})
	require.NotContains(t, windows["v1"].entries, IdempotencyKey("old"))
	require.Equal(t, now, windows["v1"].entries["future"].at)
}

func BenchmarkWindowRestoreOrdering(b *testing.B) {
	for _, count := range []int{20000, 100000} {
		now := time.Now()
		snapshot := orderedRecoveryFixture(count, 64, now)
		config := WindowConfig{TTL: time.Hour, Now: func() time.Time { return now }}
		b.Run(fmt.Sprintf("records=%d/legacy_sort", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				manager := newWindowManager(config)
				manager.restore(snapshot.VChannels)
				if manager.order.Len() != count {
					b.Fatal("lost records")
				}
			}
		})
		b.Run(fmt.Sprintf("records=%d/ordered_merge", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				manager := newWindowManager(config)
				manager.restoreOrdered(snapshot)
				if manager.order.Len() != count {
					b.Fatal("lost records")
				}
			}
		})
	}
}
