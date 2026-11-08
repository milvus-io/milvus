package walsummary

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestChunkValidationLifecycle(t *testing.T) {
	for _, capacity := range []uint64{0, 1, 1 << 20} {
		t.Run(stringCapacity(capacity), func(t *testing.T) {
			ctx := context.Background()
			payload, index := footerTestFixture(t)
			retained := newChunkIndex(capacity)
			leaf := retained.newChunk(index, &chunkPayload{bytes: payload, footerStart: chunkHeaderSize})
			retained.chunks = append(retained.chunks, leaf)
			store := &Store{}
			c := &retained.cache
			require.Same(t, leaf, retained.snapshot(0, 100)[0])
			c.mu.Lock()
			c.evictLocked(leaf)
			c.mu.Unlock()
			calls := 0
			current := payload
			patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, _ context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
				calls++
				return validateChunkPayload(current, "p1", index, verified)
			}).Build()
			defer patch.UnPatch()
			require.Nil(t, leaf.verified)
			first, err := c.read(ctx, store, leaf)
			require.NoError(t, err)
			require.NotNil(t, leaf.verified)
			if capacity > 1 {
				// Force actual LRU eviction with a competing resident object.
				c.mu.Lock()
				c.putLocked(&indexedChunk{}, &chunkPayload{bytes: make([]byte, capacity)})
				c.mu.Unlock()
			}
			require.Nil(t, leaf.payload)
			second, err := c.read(ctx, store, leaf)
			require.NoError(t, err)
			require.Same(t, first.verified, second.verified)
			require.Equal(t, 2, calls)
			c.mu.Lock()
			c.evictLocked(leaf)
			c.mu.Unlock()
			current = append([]byte(nil), payload...)
			current[chunkHeaderSize] ^= 1
			_, err = c.read(ctx, store, leaf)
			require.ErrorIs(t, err, ErrStoreCorrupted)
			require.Same(t, first.verified, leaf.verified, "failed reads must not poison validation")
			require.Nil(t, leaf.payload)
			current = payload
			_, err = c.read(ctx, store, leaf)
			require.NoError(t, err)
			retained.remove(index.GetGeneration())
			require.Nil(t, leaf.verified)
			require.Nil(t, leaf.payload)
			require.Empty(t, retained.chunks)
			retained.remove(index.GetGeneration())
		})
	}
}

func stringCapacity(capacity uint64) string {
	switch capacity {
	case 0:
		return "disabled"
	case 1:
		return "oversized"
	default:
		return "evicted"
	}
}

func TestChunkValidationRetireInFlight(t *testing.T) {
	for _, warm := range []bool{false, true} {
		ctx := context.Background()
		payload, index := footerTestFixture(t)
		retained := newChunkIndex(1 << 20)
		leaf := retained.newChunk(index, nil)
		retained.chunks = append(retained.chunks, leaf)
		if warm {
			first, err := validateChunkPayload(payload, "p1", index, nil)
			require.NoError(t, err)
			leaf.verified = first.verified
		}
		entered, resume := make(chan struct{}), make(chan struct{})
		patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, _ context.Context, captured *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
			// Keep a private copy before blocking in the runtime-patched callback.
			copied := proto.Clone(captured).(*streamingpb.PChannelSummaryChunkIndexEntry)
			var validation *chunkValidation
			if verified != nil {
				v := *verified
				validation = &v
			}
			close(entered)
			<-resume
			return validateChunkPayload(payload, "p1", copied, validation)
		}).Build()
		done := make(chan error, 1)
		go func() { _, err := retained.cache.read(ctx, &Store{}, leaf); done <- err }()
		<-entered
		retained.remove(index.GetGeneration())
		close(resume)
		require.NoError(t, <-done)
		require.True(t, leaf.retired)
		require.Nil(t, leaf.verified)
		require.Nil(t, leaf.payload)
		require.Zero(t, retained.cache.bytes)
		patch.UnPatch()
	}
}

func TestChunkValidationCanceledLoader(t *testing.T) {
	payload, index := footerTestFixture(t)
	retained := newChunkIndex(1 << 20)
	leaf := retained.newChunk(index, nil)
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, ctx context.Context, captured *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
		captured = proto.Clone(captured).(*streamingpb.PChannelSummaryChunkIndexEntry)
		calls.Add(1)
		close(entered)
		select {
		case <-resume:
			return validateChunkPayload(payload, "p1", captured, verified)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := retained.cache.read(ctx, &Store{}, leaf); done <- err }()
	<-entered
	// A waiter leaving must neither publish validation nor cancel its loader.
	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWaiter()
	_, err := retained.cache.read(waiterCtx, &Store{}, leaf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, leaf.verified)
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := retained.cache.read(context.Background(), &Store{}, leaf)
			results <- err
		}()
	}
	awaitChunkWaiters(t, &retained.cache, leaf, 17)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Nil(t, leaf.verified)
	close(resume)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), calls.Load())
	require.NotNil(t, leaf.verified)
	require.NotNil(t, leaf.payload)
}

func TestChunkValidationCanceledLoadSlot(t *testing.T) {
	_, index := footerTestFixture(t)
	retained := newChunkIndex(0)
	leaf := retained.newChunk(index, nil)
	for range cap(retained.cache.loadSlots) {
		retained.cache.loadSlots <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := retained.cache.read(ctx, &Store{}, leaf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, leaf.verified)
	require.Nil(t, leaf.loading)
	_, err = retained.cache.read(ctx, &Store{}, leaf)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
