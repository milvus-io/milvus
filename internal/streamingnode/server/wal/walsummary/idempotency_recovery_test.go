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

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestIdempotencyRecoveryReadsOnlySelectedTail(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	m, store := newTestManagerWithStore(t)
	observe := func(vc, key string, at time.Time) int {
		msg := newTestIdempotentInsertMessage(t, vc, tsoutil.ComposeTSByTime(at), key, []int64{1}, []uint32{0})
		m.ObserveMessage(ctx, msg)
		annotation, insert := insertSummaryOf(msg)
		return int(idempotencyEncodedSize(&stagedRecord{idempotency: annotation, insert: insert}))
	}
	observe("a", "x", now.Add(-2*time.Hour))
	require.NoError(t, persistSummary(ctx, m))
	// An expired retained object must not even be fetched by this consumer.
	require.NoError(t, store.DeleteChunk(ctx, 0, 1))
	observe("a", "a", now.Add(-20*time.Minute))
	observe("b", "b", now.Add(-19*time.Minute))
	require.NoError(t, persistSummary(ctx, m))
	observe("a", "c", now.Add(-2*time.Minute))
	size := observe("b", "d", now.Add(-time.Minute))
	require.NoError(t, persistSummary(ctx, m))
	// A newer keyless chunk contributes no local identity and needs no read.
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "a", tsoutil.ComposeTSByTime(now.Add(-30*time.Second)), "", nil, nil))
	require.NoError(t, persistSummary(ctx, m))
	reads := countChunkReads(t)
	policy := idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 2 * size, MinCount: 16}
	snapshots, err := m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.EqualValues(t, 1, reads.Load(), "capacity must stop older object reads, and keyless objects need no payload read")
	require.Len(t, snapshots, 2)
	require.Equal(t, "c", snapshots["a"].Records[0].IdempotencyKey)
	require.Equal(t, "d", snapshots["b"].Records[0].IdempotencyKey)
	reads.Store(0)
	policy.MaxSize = math.MaxInt
	snapshots, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.EqualValues(t, 2, reads.Load(), "TTL must exclude the missing expired chunk")
	require.Len(t, snapshots["a"].Records, 2)
	require.Equal(t, "a", snapshots["a"].Records[0].IdempotencyKey)
	require.Equal(t, "c", snapshots["a"].Records[1].IdempotencyKey)

	reads.Store(0)
	observe("a", "c", now) // The newest occurrence must replace the durable c.
	policy.MaxSize = 3 * size
	snapshots, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Len(t, snapshots["a"].Records, 1, "the latest occurrence of c must replace its older durable result")
	require.Equal(t, tsoutil.ComposeTSByTime(now), snapshots["a"].Records[0].SourceTimeTick)
	require.Len(t, snapshots["b"].Records, 1)
	require.Equal(t, "d", snapshots["b"].Records[0].IdempotencyKey)
	require.EqualValues(t, 1, reads.Load(), "encoded section bytes are charged before duplicate-key filtering")

	observe("b", "e", now.Add(time.Millisecond))
	policy.MaxSize = 2 * size
	reads.Store(0)
	snapshots, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Zero(t, reads.Load(), "a full pending suffix must bypass all durable reads")
	require.Len(t, snapshots, 2)
	require.NotNil(t, m.seal())
	_, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Zero(t, reads.Load(), "sealed records are readable before upload")

	policy.MaxSize = size - 1
	snapshots, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Len(t, snapshots, 2, "the whole newest boundary chunk is selected before decoding")
	require.Zero(t, reads.Load())
	policy.MaxSize = 0
	snapshots, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Nil(t, snapshots)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = m.ReadIdempotencySnapshots(canceled, policy, now)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIdempotencyRecoveryTTLBoundaryAndMissingTail(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	m, store := newTestManagerWithStore(t)
	cutoff := tsoutil.ComposeTSByTime(now.Add(-time.Hour))
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", cutoff+1, "expired", []int64{1}, []uint32{0}))
	require.NoError(t, persistSummary(ctx, m))
	require.NoError(t, store.DeleteChunk(ctx, 0, 1))
	policy := idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}
	got, err := m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	require.Empty(t, got, "logical ticks in the expired physical millisecond do not refresh age")
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", cutoff+(1<<18), "retained", []int64{2}, []uint32{0}))
	require.NoError(t, persistSummary(ctx, m))
	require.NoError(t, store.DeleteChunk(ctx, 1, 1))
	got, err = m.ReadIdempotencySnapshots(ctx, policy, now)
	require.Error(t, err, "a missing object inside the selected suffix must fail recovery")
	require.Nil(t, got)
}

func TestIdempotencyRecoverySnapshotSurvivesUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	base := tsoutil.ComposeTSByTime(now.Add(-time.Minute))
	m, _ := newTestManagerWithStore(t)
	observe := func(tt uint64, key string) {
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", tt, key, []int64{1}, []uint32{0}))
	}
	observe(base, "durable")
	require.NoError(t, persistSummary(ctx, m))
	observe(base+1, "sealed")
	require.NotNil(t, m.seal())
	observe(base+2, "pending")
	policy := idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}
	want, err := m.ReadIdempotencySnapshots(ctx, policy, now)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry, *chunkValidation) (*chunkPayload, error)
	patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
		copied := proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return original(s, ctx, copied, verified)
	}).Build()
	defer patch.UnPatch()
	type result struct {
		value map[string]*idempotencyview.Snapshot
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := m.ReadIdempotencySnapshots(ctx, policy, now)
		done <- result{value, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("reader did not capture its snapshot")
	}
	require.NotNil(t, m.seal())
	sealed := append([]*SealedChunk(nil), m.pendingSealed...)
	for i := len(sealed) - 1; i >= 0; i-- {
		require.NoError(t, m.writeChunk(ctx, sealed[i]))
	}
	observe(base+3, "later")
	unblock()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Len(t, got.value["v1"].Records, 3)
		for i, record := range got.value["v1"].Records {
			expected := want["v1"].Records[i]
			require.Equal(t, expected.IdempotencyKey, record.IdempotencyKey)
			require.Equal(t, expected.SourceTimeTick, record.SourceTimeTick)
			require.True(t, proto.Equal(expected.InsertResult, record.InsertResult))
		}
	case <-ctx.Done():
		t.Fatal("reader did not complete")
	}
}

// Retained objects without any Insert identity need no payload access.
func TestIdempotencyRecoveryWithoutKeyedHistory(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	msg := plainSummaryInsert(tsoutil.ComposeTSByTime(time.Now()))
	m.ObserveMessage(ctx, msg)
	require.NoError(t, persistSummary(ctx, m))
	reads := countChunkReads(t)
	got, err := m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}, time.Now())
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, reads.Load())
}

// Deliberately finish every batch newest first. Reads must be parallel, while
// the final shared-cache residency must still favor the newest generations.
func TestIdempotencyRecoveryParallelReadsWarmNewestChunks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	base := tsoutil.ComposeTSByTime(now.Add(-time.Minute))
	m, store := newTestManagerWithStore(t)
	for i := 0; i < 9; i++ {
		tt := base + uint64(i*2)
		m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", tt, 1, int64(i)))
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", tt+1, fmt.Sprintf("key-%02d", i), []int64{int64(i)}, []uint32{0}))
		require.NoError(t, persistSummary(ctx, m))
	}
	chunks := m.chunkIndex.chunks
	lastPayload, err := store.readChunkPayload(ctx, chunks[8].PChannelSummaryChunkIndexEntry, nil)
	require.NoError(t, err)
	m.chunkIndex.cache.capacity = uint64(2 * cap(lastPayload.bytes))
	selected := chunks[3:]
	maxSize := 0
	for _, chunk := range selected {
		for _, index := range chunk.GetVchannels() {
			maxSize += int(index.GetInserts().GetLength() + index.GetIdempotency().GetLength())
		}
	}
	// Older objects are outside the manifest-planned suffix and must not load.
	for _, chunk := range chunks[:3] {
		require.NoError(t, store.DeleteChunk(ctx, chunk.GetGeneration(), chunk.GetTerm()))
	}
	calls := countChunkReads(t)
	entered := make(chan uint64, len(selected))
	gates := make(map[uint64]chan struct{})
	for _, chunk := range selected {
		gates[chunk.GetGeneration()] = make(chan struct{})
	}
	patch := mockey.Mock((*Store).readChunkPayload).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
		copied := proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		entered <- copied.GetGeneration()
		select {
		case <-gates[copied.GetGeneration()]:
			// Keep the real storage and validator under the scheduling gate,
			// without Origin's concurrently assigned function variable.
			payload, err := s.chunkManager.Read(ctx, buildChunkKey(s.chunkManager, s.pchannel, copied.GetGeneration(), copied.GetTerm()))
			if err != nil {
				return nil, err
			}
			return validateChunkPayload(payload, s.pchannel, copied, verified)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Build()
	defer patch.UnPatch()
	type result struct {
		snapshots map[string]*idempotencyview.Snapshot
		err       error
	}
	done := make(chan result, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		snapshots, err := m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: maxSize}, now)
		done <- result{snapshots, err}
	}()
	defer func() { cancel(); <-finished }()
	for start := 0; start < len(selected); start += chunkReadConcurrency {
		batch := selected[start:min(start+chunkReadConcurrency, len(selected))]
		expected := make(map[uint64]bool)
		for _, chunk := range batch {
			expected[chunk.GetGeneration()] = true
		}
		for range batch {
			select {
			case generation := <-entered:
				require.True(t, expected[generation], "only the next oldest bounded batch may start")
				delete(expected, generation)
			case <-ctx.Done():
				t.Fatal("chunk downloads did not run concurrently")
			}
		}
		for i := len(batch) - 1; i >= 0; i-- {
			chunk := batch[i]
			close(gates[chunk.GetGeneration()])
			require.Eventually(t, func() bool {
				m.chunkIndex.cache.mu.Lock()
				defer m.chunkIndex.cache.mu.Unlock()
				return chunk.loading == nil
			}, 5*time.Second, time.Millisecond)
		}
	}
	got := <-done
	require.NoError(t, got.err)
	require.Len(t, got.snapshots["v1"].Records, len(selected))
	require.Equal(t, "key-03", got.snapshots["v1"].Records[0].IdempotencyKey)
	require.Equal(t, "key-08", got.snapshots["v1"].Records[5].IdempotencyKey)
	for _, chunk := range chunks[:7] {
		require.Nil(t, chunk.payload)
	}
	require.NotNil(t, chunks[7].payload)
	require.NotNil(t, chunks[8].payload)
	require.EqualValues(t, len(selected), calls.Load())
	batch, err := m.ReadTransform(ctx, "v1", base+15, base+17, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 1)
	require.EqualValues(t, len(selected), calls.Load(), "TransformLog must reuse the newest chunk warmed by recovery")
}

func TestIdempotencyRecoveryJoinsFailedBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now()
	m, _ := newTestManagerWithStore(t)
	for i := 0; i < 2; i++ {
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", tsoutil.ComposeTSByTime(now.Add(-time.Minute))+uint64(i), fmt.Sprint(i), nil, nil))
		require.NoError(t, persistSummary(ctx, m))
	}
	peerEntered, peerExited := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
		if index.GetGeneration() == 0 {
			select {
			case <-peerEntered:
				return nil, ErrStoreCorrupted
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		close(peerEntered)
		<-ctx.Done()
		close(peerExited)
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	snapshots, err := m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}, now)
	require.ErrorIs(t, err, ErrStoreCorrupted)
	require.Nil(t, snapshots)
	select {
	case <-peerExited:
	default:
		t.Fatal("failed recovery returned before joining the other download")
	}
	require.True(t, m.readMu.TryLock(), "snapshot pin must be released after downloads exit")
	m.readMu.Unlock()
	for _, chunk := range m.chunkIndex.chunks {
		require.Nil(t, chunk.loading)
	}
}

func TestIdempotencyRecoveryPlansUnpublishedTail(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m, _ := newTestManagerWithStore(t)
	observe := func(vc, key string, at time.Time) {
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, vc, tsoutil.ComposeTSByTime(at), key, nil, nil))
	}
	observe("v1", "expired-chunk", now.Add(-2*time.Hour))
	require.NotNil(t, m.seal())
	observe("expired-section", "old", now.Add(-90*time.Minute))
	observe("mixed", "old", now.Add(-80*time.Minute))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "mixed", tsoutil.ComposeTSByTime(now.Add(-5*time.Minute)), 1, 1))
	observe("keyless", "", now.Add(-4*time.Minute))
	observe("v1", "new", now.Add(-3*time.Minute))
	observe("v1", "", now.Add(-2*time.Minute))
	require.NotNil(t, m.seal())
	observe("v1", "", now.Add(-time.Minute))
	reads := countChunkReads(t)
	snapshots, err := m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}, now)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Len(t, snapshots["v1"].Records, 1)
	require.Equal(t, "new", snapshots["v1"].Records[0].IdempotencyKey)
	require.Zero(t, reads.Load())

	m, _ = newTestManagerWithStore(t)
	observe("v1", "old", now.Add(-2*time.Hour))
	observe("v1", "new", now.Add(-2*time.Minute))
	observe("v1", "", now.Add(-time.Minute))
	snapshots, err = m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}, now)
	require.NoError(t, err)
	require.Len(t, snapshots["v1"].Records, 1)
	require.Equal(t, "new", snapshots["v1"].Records[0].IdempotencyKey)
}

func TestIdempotencyRecoveryDecodeFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m, _ := newTestManagerWithStore(t)
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", tsoutil.ComposeTSByTime(now.Add(-time.Minute)), "key", nil, nil))
	require.NoError(t, persistSummary(ctx, m))
	patch := mockey.Mock(unmarshalIdempotencySections).Return(nil, ErrStoreCorrupted).Build()
	defer patch.UnPatch()
	snapshots, err := m.ReadIdempotencySnapshots(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1024}, now)
	require.ErrorIs(t, err, ErrStoreCorrupted)
	require.Nil(t, snapshots)
}

func TestIdempotencyRecoveryPreservesChunkOrderAndLatestKey(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	base := tsoutil.ComposeTSByTime(now.Add(-time.Minute))
	m, _ := newTestManagerWithStore(t)
	observe := func(vc, key string, offset uint64) {
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, vc, base+offset, key, []int64{int64(offset)}, []uint32{0}))
	}
	observe("a", "x", 10)
	observe("b", "b", 20)
	observe("a", "x", 25)
	observe("a", "a", 30)
	observe("a", "z", 35)
	observe("b", "c", 40)
	require.NoError(t, persistSummary(ctx, m))
	observe("a", "x", 50)
	observe("b", "b", 60)
	observe("a", "d", 70)
	require.NoError(t, persistSummary(ctx, m))
	observe("a", "a", 80)
	observe("b", "q", 90)
	observe("a", "x", 100)
	require.NotNil(t, m.seal())
	observe("a", "d", 110)
	observe("b", "c", 120)
	observe("a", "e", 130)
	snapshot, err := m.ReadIdempotencyRecovery(ctx, idempotencyview.RetentionConfig{TTL: time.Hour, MaxSize: 1 << 20}, now)
	require.NoError(t, err)
	require.Len(t, snapshot.Chunks, 4)
	require.Len(t, snapshot.VChannels["a"].Records, 5)
	require.Len(t, snapshot.VChannels["b"].Records, 3)
	latest := make(map[string]*idempotencyview.Record)
	for vc, s := range snapshot.VChannels {
		var prev uint64
		for _, r := range s.Records {
			require.Greater(t, r.SourceTimeTick, prev, "compatibility view is already ordered without sorting")
			prev = r.SourceTimeTick
			latest[vc+":"+r.IdempotencyKey] = r
		}
	}
	var offsets []uint64
	snapshot.Range(func(vc string, r *idempotencyview.Record) bool {
		require.Same(t, latest[vc+":"+r.IdempotencyKey], r, "both views share the same record")
		offsets = append(offsets, r.SourceTimeTick-base)
		require.EqualValues(t, r.SourceTimeTick-base, r.InsertResult.GetIds().GetIntId().Data[0])
		return true
	})
	require.Equal(t, []uint64{35, 60, 80, 90, 100, 110, 120, 130}, offsets)
	for _, s := range snapshot.Chunks[0] {
		if s.VChannel == "a" {
			require.Len(t, s.Records, 1)
			require.Equal(t, "z", s.Records[0].IdempotencyKey)
			require.Equal(t, 1, cap(s.Records), "a sparse old section must release its oversized backing array")
		} else {
			require.Nil(t, s.Records, "an obsolete section must release its old backing array")
		}
	}
}
