package walsummary

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func countChunkReads(t *testing.T) *atomic.Int64 {
	t.Helper()
	calls := &atomic.Int64{}
	var original func(*storage.LocalChunkManager, context.Context, string) ([]byte, error)
	patch := mockey.Mock((*storage.LocalChunkManager).Read).Origin(&original).To(func(cm *storage.LocalChunkManager, ctx context.Context, key string) ([]byte, error) {
		if strings.Contains(key, "/chunks/") {
			calls.Add(1)
		}
		return original(cm, ctx, key)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return calls
}

func TestChunkCacheWriteAndReadSharing(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	m.chunkIndex = newChunkIndex(1 << 20)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 10, 1, 10))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "b", 20, 1, 20))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 30, 1, 30))
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "a", 40, "key", []int64{40}, []uint32{0}))
	observeReadBarrier(m, 100)
	require.NoError(t, persistSummary(ctx, m))
	calls := countChunkReads(t)
	for _, cold := range []bool{false, true} {
		if cold {
			m = newTestManager(t, nextTermStore(store), 1<<30)
			m.chunkIndex = newChunkIndex(1 << 20)
			require.NoError(t, m.Restore(ctx))
			require.Zero(t, calls.Load(), "restore must not preload retained chunks")
		}
		require.Len(t, m.chunkIndex.chunks, len(m.Manifest().Chunks))
		for _, vc := range []string{"a", "b", "a"} {
			var cursor uint64
			for cursor < 100 {
				batch, err := m.ReadTransform(ctx, vc, cursor, 100, ReadLimits{MaxRows: 1})
				require.NoError(t, err)
				require.Greater(t, batch.CoveredThrough, cursor)
				cursor = batch.CoveredThrough
				for _, entry := range batch.Entries {
					require.Equal(t, int64(entry.GetTimeTick()), entry.GetDelete().Blocks[0].PrimaryKeys.GetIntId().Data[0])
					entry.GetDelete().Blocks[0].PrimaryKeys.GetIntId().Data[0] = -1
				}
			}
		}
		records, err := m.ReadIdempotencyEntries(ctx, "a", 0, 100)
		require.NoError(t, err)
		require.Len(t, records.Inserts, 1)
		require.Equal(t, "key", records.Idempotency[0].GetKey())
		want := int64(0)
		if cold {
			want = 1
		}
		require.Equal(t, want, calls.Load(), "pages, VChannels and consumers share the object")
	}
}

func TestTransformReadSkipsEmptySectionsAndFullPage(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t) // disabled residency exposes every object read
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 10, 1, 1))
	observeReadBarrier(m, 100)
	require.NoError(t, persistSummary(ctx, m))
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 200, 1, 2))
	require.NoError(t, persistSummary(ctx, m))
	calls := countChunkReads(t)
	for _, interval := range [][2]uint64{{0, 9}, {10, 199}, {100, 199}} {
		batch, err := m.ReadTransform(ctx, "a", interval[0], interval[1], ReadLimits{})
		require.NoError(t, err)
		require.Empty(t, batch.Entries)
		require.Equal(t, interval[1], batch.CoveredThrough)
	}
	require.Zero(t, calls.Load())
	batch, err := m.ReadTransform(ctx, "a", 0, 200, ReadLimits{MaxRows: 1})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 1)
	require.Equal(t, uint64(10), batch.CoveredThrough)
	require.Equal(t, int64(1), calls.Load(), "do not fetch next chunk after filling the batch")
	batch, err = m.ReadTransform(ctx, "a", batch.CoveredThrough, 200, ReadLimits{MaxRows: 1})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 1)
	require.Equal(t, uint64(200), batch.CoveredThrough)
	require.Equal(t, int64(2), calls.Load(), "do not reread first chunk's exhausted section")
}

func TestChunkCacheEvictionAndOversizedBypass(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "a", 10, &released)
	flushTransform(t, m, "a", 20, &released)
	calls := countChunkReads(t)
	c := &m.chunkIndex.cache
	first, second := m.chunkIndex.chunks[0], m.chunkIndex.chunks[1]
	one, err := c.read(ctx, m.cfg.Store, first)
	require.NoError(t, err)
	require.Zero(t, c.bytes, "zero disables residency")
	two, err := c.read(ctx, m.cfg.Store, second)
	require.NoError(t, err)
	c.capacity = uint64(max(cap(one.bytes), cap(two.bytes)))
	saved, err := c.read(ctx, m.cfg.Store, first)
	require.NoError(t, err)
	_, err = c.read(ctx, m.cfg.Store, second)
	require.NoError(t, err)
	require.Nil(t, first.payload)
	require.NotNil(t, second.payload)
	require.Equal(t, one.bytes, saved.bytes, "eviction cannot invalidate an acquired buffer")
	require.Len(t, m.chunkIndex.chunks, 2, "eviction retains all metadata")
	// An oversized read must not evict the resident working set.
	oversized := &indexedChunk{PChannelSummaryChunkIndexEntry: first.PChannelSummaryChunkIndexEntry}
	c.mu.Lock()
	c.putLocked(oversized, &chunkPayload{bytes: make([]byte, c.capacity+1)})
	c.mu.Unlock()
	require.Nil(t, oversized.payload)
	require.NotNil(t, second.payload)
	_, err = c.read(ctx, m.cfg.Store, first)
	require.NoError(t, err)
	require.Equal(t, int64(5), calls.Load())
	require.LessOrEqual(t, c.bytes, c.capacity)
	// Touch promotes an entry; the least recently read node is evicted.
	c.capacity *= 2
	_, err = c.read(ctx, m.cfg.Store, second)
	require.NoError(t, err)
	_, err = c.read(ctx, m.cfg.Store, first)
	require.NoError(t, err)
	third := &indexedChunk{}
	c.mu.Lock()
	c.putLocked(third, two)
	c.mu.Unlock()
	require.NotNil(t, first.payload)
	require.Nil(t, second.payload)
}

func TestChunkCacheConcurrentLoadAndCancellation(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "a", 10, &released)
	m.chunkIndex.cache.capacity = 1 << 20
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error)
	patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error) {
		copied := proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return original(s, ctx, copied)
	}).Build()
	defer patch.UnPatch()
	read := func(ctx context.Context) error { _, err := m.ReadTransform(ctx, "a", 0, 10, ReadLimits{}); return err }
	loaderCtx, cancelLoader := context.WithCancel(ctx)
	defer cancelLoader()
	loaderDone := make(chan error, 1)
	go func() { loaderDone <- read(loaderCtx) }()
	<-entered
	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- read(waiterCtx) }()
	cancelWaiter()
	require.ErrorIs(t, <-waiterDone, context.Canceled)
	// Remaining callers survive cancellation of the original loading caller.
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); errors <- read(ctx) }()
	}
	cancelLoader()
	require.ErrorIs(t, <-loaderDone, context.Canceled)
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, int64(2), calls.Load())
	close(resume)
}

func TestChunkCacheRetirementDuringLoad(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "a", 10, &released)
	m.chunkIndex.cache.capacity = 1 << 20
	leaf := m.chunkIndex.chunks[0]
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error)
	patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error) {
		copied := proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		m.AdvanceGCTimeTick("a", 10)
		m.cfg.RetentionMaxBytes = 1
		require.NoError(t, m.GCOnce(ctx))
		require.Empty(t, m.chunkIndex.chunks)
		return original(s, ctx, copied)
	}).Build()
	defer patch.UnPatch()
	batch, err := m.ReadTransform(ctx, "a", 0, 10, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 1)
	require.True(t, leaf.retired)
	require.Nil(t, leaf.payload, "a retiring load must not repopulate cache")
	require.Zero(t, m.chunkIndex.cache.bytes)
	batch, err = m.ReadTransform(ctx, "a", 0, 10, ReadLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(10), batch.FastForwardTimeTick)
	require.Empty(t, batch.Entries)
}

func TestChunkCacheErrorsAndLoadSlots(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "a", 10, &released)
	m.chunkIndex.cache.capacity = 1 << 20
	leaf := m.chunkIndex.chunks[0]
	// Missing objects remain errors and are not negative-cached.
	patch := mockey.Mock((*Store).readChunkPayload).Return(nil, context.DeadlineExceeded).Build()
	_, err := m.chunkIndex.cache.read(ctx, store, leaf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	patch.UnPatch()
	require.Nil(t, leaf.payload)
	require.Nil(t, leaf.loading)
	slots := m.chunkIndex.cache.loadSlots
	for range cap(slots) {
		slots <- struct{}{}
	}
	timeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, err = m.chunkIndex.cache.read(timeout, store, leaf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	for range cap(slots) {
		<-slots
	}
	_, err = m.chunkIndex.cache.read(ctx, store, leaf)
	require.NoError(t, err)
	require.NotNil(t, leaf.payload)
	// Cached history never bypasses a terminal Summary error.
	m.mu.Lock()
	m.setTerminalErrorLocked(context.Canceled)
	m.mu.Unlock()
	_, err = m.ReadTransform(ctx, "a", 0, math.MaxUint64, ReadLimits{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestChunkCacheOutOfOrderWritesAndRetirement(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	m.chunkIndex = newChunkIndex(1 << 20)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 10, 1, 10))
	first := m.seal()
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 20, 1, 20))
	second := m.seal()
	require.NoError(t, m.writeChunk(ctx, second))
	require.Empty(t, m.chunkIndex.chunks, "later uploads cannot enter readable durable index across a gap")
	require.NotNil(t, second.index.payload)
	require.Greater(t, m.chunkIndex.cache.bytes, uint64(0), "out-of-order upload buffers are budgeted immediately")
	require.NoError(t, m.writeChunk(ctx, first))
	require.Len(t, m.chunkIndex.chunks, 2)
	calls := countChunkReads(t)
	batch, err := m.ReadTransform(ctx, "a", 0, 20, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 2)
	require.Zero(t, calls.Load(), "both upload completions seed cache")
	m.AdvanceGCTimeTick("a", 20)
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, m.GCOnce(ctx))
	require.Empty(t, m.chunkIndex.chunks)
	require.Zero(t, m.chunkIndex.cache.bytes)
	require.True(t, first.index.retired)
	require.Nil(t, first.index.payload)
}

func TestChunkCacheUsesActualStoredEncoding(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	m.chunkIndex = newChunkIndex(1 << 20)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", 10, 1, 10))
	sc := m.seal()
	entry := sc.RecordsByVChannel["a"][0].entry
	sections := map[string]*ChunkSections{"a": {Transform: []*streamingpb.VChannelSummaryTransformRecord{{TimeTick: 10, Delete: entry.GetDelete()}}}}
	payload, _, err := marshalChunk(store.PChannel(), sc.Generation, store.Term(), sections, sc.Coverage)
	require.NoError(t, err)
	payload[11] ^= 0xff // equivalent content with a different reserved header byte
	require.NoError(t, store.chunkManager.Write(ctx, store.ChunkKey(sc.Generation), payload))
	require.NoError(t, m.writeChunk(ctx, sc))
	require.Equal(t, payload, m.chunkIndex.chunks[0].payload.bytes)
	calls := countChunkReads(t)
	batch, err := m.ReadTransform(ctx, "a", 0, 10, ReadLimits{})
	require.NoError(t, err)
	require.Len(t, batch.Entries, 1)
	require.Zero(t, calls.Load())
}

func TestChunkCacheValidatesColdObject(t *testing.T) {
	ctx := context.Background()
	for _, damage := range []string{"missing", "header", "index"} {
		t.Run(damage, func(t *testing.T) {
			m, store := newTestManagerWithStore(t)
			var released bool
			flushTransform(t, m, "a", 10, &released)
			leaf := m.chunkIndex.chunks[0]
			switch damage {
			case "missing":
				require.NoError(t, store.DeleteChunk(ctx, 0, store.Term()))
			case "header":
				require.NoError(t, store.chunkManager.Write(ctx, store.ChunkKey(0), []byte("broken")))
			case "index":
				leaf.PChannelSummaryChunkIndexEntry = proto.Clone(leaf.PChannelSummaryChunkIndexEntry).(*streamingpb.PChannelSummaryChunkIndexEntry)
				leaf.ObjectSize++
			}
			_, err := m.chunkIndex.cache.read(ctx, store, leaf)
			require.Error(t, err)
			require.Nil(t, leaf.payload)
			require.Nil(t, leaf.loading)
		})
	}
}
