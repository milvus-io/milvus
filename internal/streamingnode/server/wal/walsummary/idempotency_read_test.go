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

package walsummary

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestReadIdempotencyEntriesOfVChannelsMemoryTails(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	writes := []struct {
		vchannel string
		key      string
	}{
		{"a", "durable"},
		{"b", ""},
		{"a", ""},
		{"b", "sealed-1"},
		{"a", "sealed-2"},
		{"b", ""},
		{"a", "pending"},
		{"b", "pending"},
		{"unrequested", "other"},
	}
	originals := make(map[string][]message.ImmutableMessage)
	for i, write := range writes {
		tt := uint64(i + 1)
		var pks []int64
		var offsets []uint32
		if write.key != "" {
			pks, offsets = []int64{int64(tt)}, []uint32{0}
		}
		msg := newTestIdempotentInsertMessage(t, write.vchannel, tt, write.key, pks, offsets)
		originals[write.vchannel] = append(originals[write.vchannel], msg)
		m.ObserveMessage(ctx, msg)
		switch tt {
		case 2:
			require.NoError(t, persistSummary(ctx, m))
		case 4, 6:
			require.NotNil(t, m.seal())
		}
	}
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "delete-only", 10, 1, 10))

	for _, bounds := range []struct{ from, to uint64 }{{0, math.MaxUint64}, {2, 7}, {6, 8}, {8, 8}} {
		t.Run(fmt.Sprintf("%d-%d", bounds.from, bounds.to), func(t *testing.T) {
			got, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"a", "b", "absent", "delete-only"}, bounds.from, bounds.to)
			require.NoError(t, err)
			require.Len(t, got, 4)
			for _, vc := range []string{"a", "b"} {
				want := &ChunkSections{}
				for _, msg := range originals[vc] {
					if msg.TimeTick() > bounds.from && msg.TimeTick() <= bounds.to {
						key, insert := insertSummaryOf(msg)
						want.Inserts = append(want.Inserts, insert)
						want.Idempotency = append(want.Idempotency, key)
					}
				}
				// Durable keyed sections preserve empty annotations even when
				// the selected interval contains only their keyless records.
				if len(want.Inserts) == 0 {
					want.Idempotency = nil
				}
				requireIdempotencySectionsEqual(t, want, got[vc])
			}
			require.Empty(t, got["absent"].Inserts)
			require.Empty(t, got["delete-only"].Inserts)
		})
	}
	subset, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"a"}, 0, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, subset, 1, "sealed and pending records of other channels must be excluded")
	require.Len(t, subset["a"].Inserts, 4)
	keyless, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"b"}, 4, 6)
	require.NoError(t, err)
	require.Len(t, keyless["b"].Inserts, 1)
	require.Nil(t, keyless["b"].Idempotency, "keys outside the selected in-memory interval must not create annotations")
	empty, err := m.ReadIdempotencyEntriesOfVChannels(ctx, nil, 0, math.MaxUint64)
	require.NoError(t, err)
	require.Nil(t, empty)
}

func TestReadIdempotencyEntriesSnapshotSurvivesTailTransitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m, _ := newTestManagerWithStore(t)
	observe := func(tt uint64) {
		vc := "a"
		if tt%2 == 0 {
			vc = "b"
		}
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, vc, tt, fmt.Sprint(tt), []int64{int64(tt)}, []uint32{0}))
	}
	for tt := uint64(1); tt <= 8; tt++ {
		observe(tt)
		switch tt {
		case 2:
			require.NoError(t, persistSummary(ctx, m))
		case 4, 6:
			require.NotNil(t, m.seal())
		}
	}
	want, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"a", "b"}, 0, math.MaxUint64)
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	releaseRead := sync.OnceFunc(func() { close(release) })
	defer releaseRead()
	var first sync.Once
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry, *chunkValidation) (*chunkPayload, error)
	patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
		copied := proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		first.Do(func() {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		return original(s, ctx, copied, verified)
	}).Build()
	defer patch.UnPatch()
	type readResult struct {
		sections map[string]*ChunkSections
		err      error
	}
	done := make(chan readResult, 1)
	go func() {
		sections, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"a", "b"}, 0, math.MaxUint64)
		done <- readResult{sections: sections, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("reader did not reach the captured durable chunk")
	}
	// The read has captured both tails. Seal the pending prefix, upload in
	// reverse order, then append beyond the captured prefix while I/O waits.
	require.NotNil(t, m.seal())
	sealed := append([]*SealedChunk(nil), m.pendingSealed...)
	for i := len(sealed) - 1; i >= 0; i-- {
		require.NoError(t, m.writeChunk(ctx, sealed[i]))
	}
	require.Empty(t, m.pendingSealed)
	observe(9)
	releaseRead()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		for vc, sections := range want {
			requireIdempotencySectionsEqual(t, sections, got.sections[vc])
		}
	case <-ctx.Done():
		t.Fatal("reader did not finish")
	}
	after, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{"a", "b"}, 0, 8)
	require.NoError(t, err)
	for vc, sections := range want {
		requireIdempotencySectionsEqual(t, sections, after[vc])
	}
	live, err := m.ReadIdempotencyEntries(ctx, "a", 8, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, live.Inserts, 1)
	require.Equal(t, uint64(9), live.Inserts[0].GetSourceTimetick())
}

func requireIdempotencySectionsEqual(t *testing.T, want, got *ChunkSections) {
	t.Helper()
	require.NotNil(t, got)
	require.Len(t, got.Inserts, len(want.Inserts))
	require.Len(t, got.Idempotency, len(want.Idempotency))
	for i := range want.Inserts {
		require.True(t, proto.Equal(want.Inserts[i], got.Inserts[i]), "insert %d: want %v, got %v", i, want.Inserts[i], got.Inserts[i])
	}
	for i := range want.Idempotency {
		require.True(t, proto.Equal(want.Idempotency[i], got.Idempotency[i]), "annotation %d: want %v, got %v", i, want.Idempotency[i], got.Idempotency[i])
	}
}

// Sparse tails are common during recovery: many requested channels have no
// records in a particular pending span or sealed chunk. Charge only the actual
// returned facts, rather than reserving every span's size for every channel.
func TestReadIdempotencyEntriesSparseTailAllocations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending int
		sealed  int
	}{
		{name: "pending", pending: 4096},
		{name: "sealed", sealed: 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, channels := idempotencyReadFixture(1024, tc.pending, tc.sealed, true)
			result := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := m.ReadIdempotencyEntriesOfVChannels(context.Background(), channels, 0, math.MaxUint64)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			// A generous ceiling includes output maps, section slices and
			// instrumentation. The old Cartesian reservations exceed it by
			// several times even with these small, safe fixtures.
			require.Less(t, result.AllocedBytesPerOp(), int64(2<<20), "%s", result.MemString())
		})
	}
}

func BenchmarkReadIdempotencyEntriesOfVChannels(b *testing.B) {
	for _, tc := range []struct {
		channels int
		pending  int
		sealed   int
		hot      bool
	}{
		{100, 1000, 0, false},
		{1000, 1000, 0, false},
		{1000, 10000, 0, false},
		{1000, 10000, 0, true},
		{1000, 0, 512, true},
		{30000, 100000, 0, false},
	} {
		b.Run(fmt.Sprintf("channels=%d/pending=%d/sealed=%d/hot=%t", tc.channels, tc.pending, tc.sealed, tc.hot), func(b *testing.B) {
			m, channels := idempotencyReadFixture(tc.channels, tc.pending, tc.sealed, tc.hot)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := m.ReadIdempotencyEntriesOfVChannels(context.Background(), channels, 0, math.MaxUint64); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Build source records outside the allocation measurement; no durable chunks
// are included so the benchmark isolates the reader's in-memory tail handling.
func idempotencyReadFixture(channelCount, pendingCount, sealedCount int, hot bool) (*Manager, []string) {
	m := NewManager(ManagerConfig{})
	channels := make([]string, channelCount)
	for i := range channels {
		channels[i] = fmt.Sprintf("v%d", i)
	}
	record := func(i int) stagedRecord {
		vc := channels[i%len(channels)]
		if hot {
			vc = channels[0]
		}
		return stagedRecord{
			vchannel: vc, timeTick: uint64(i + 1),
			idempotency: &streamingpb.VChannelSummaryIdempotencyRecord{Key: "key"},
			insert:      &streamingpb.VChannelSummaryInsertRecord{SourceTimetick: uint64(i + 1)},
		}
	}
	for i := 0; i < sealedCount; i++ {
		r := record(i)
		m.pendingSealed = append(m.pendingSealed, buildSealedChunk(uint64(i), []stagedRecord{r}))
	}
	for i := 0; i < pendingCount; i++ {
		m.pending = append(m.pending, record(sealedCount+i))
	}
	return m, channels
}
