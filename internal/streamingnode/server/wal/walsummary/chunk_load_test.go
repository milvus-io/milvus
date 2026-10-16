package walsummary

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type chunkReadResult struct {
	payload *chunkPayload
	err     error
}

func startChunkRead(ctx context.Context, cache *chunkCache, chunk *indexedChunk) <-chan chunkReadResult {
	done := make(chan chunkReadResult, 1)
	go func() {
		payload, err := cache.read(ctx, &Store{}, chunk)
		done <- chunkReadResult{payload: payload, err: err}
	}()
	return done
}

func awaitChunkRead(t *testing.T, done <-chan chunkReadResult) chunkReadResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("chunk read did not finish")
		return chunkReadResult{}
	}
}

func awaitChunkSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("chunk load did not reach the expected state")
	}
}

func awaitChunkWaiters(t *testing.T, cache *chunkCache, chunk *indexedChunk, count int) *chunkLoad {
	t.Helper()
	var load *chunkLoad
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		load = chunk.loading
		return load != nil && load.waiters == count
	}, 5*time.Second, time.Millisecond)
	return load
}

func TestChunkLoadIndependentWaiters(t *testing.T) {
	for _, capacity := range []uint64{0, 1, 1024} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			index, chunk := newChunkIndex(capacity), &indexedChunk{}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			aCtx, cancelA := context.WithCancel(ctx)
			defer cancelA()
			bCtx, cancelB := context.WithCancel(ctx)
			defer cancelB()
			entered := make(chan context.Context, 1)
			resume := make(chan struct{})
			want := &chunkPayload{bytes: []byte("shared payload")}
			var calls atomic.Int64
			patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, loadCtx context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
				calls.Add(1)
				entered <- loadCtx
				select {
				case <-resume:
					return want, nil
				case <-loadCtx.Done():
					return nil, loadCtx.Err()
				}
			}).Build()
			defer patch.UnPatch()
			a := startChunkRead(aCtx, &index.cache, chunk)
			load := awaitChunkWaiters(t, &index.cache, chunk, 1)
			b := startChunkRead(bCtx, &index.cache, chunk)
			c := startChunkRead(ctx, &index.cache, chunk)
			require.Same(t, load, awaitChunkWaiters(t, &index.cache, chunk, 3))
			loadCtx := <-entered
			_, deadline := loadCtx.Deadline()
			require.False(t, deadline, "first waiter must not impose its deadline on the load")
			cancelA()
			require.ErrorIs(t, awaitChunkRead(t, a).err, context.Canceled)
			require.NoError(t, loadCtx.Err())
			cancelB()
			require.ErrorIs(t, awaitChunkRead(t, b).err, context.Canceled)
			require.NoError(t, loadCtx.Err())
			d := startChunkRead(ctx, &index.cache, chunk)
			require.Same(t, load, awaitChunkWaiters(t, &index.cache, chunk, 2))
			close(resume)
			for _, result := range []chunkReadResult{awaitChunkRead(t, c), awaitChunkRead(t, d)} {
				require.NoError(t, result.err)
				require.Same(t, want, result.payload, "nonresident chunks must also share the completed payload")
			}
			require.Equal(t, int64(1), calls.Load())
			require.Nil(t, chunk.loading)
			require.Empty(t, index.cache.loadSlots)
		})
	}
}

func TestChunkLoadLastWaiterCancelsAndJoins(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "a", 10, 1, 42))
	require.NoError(t, persistSummary(context.Background(), m))
	chunk := m.chunkIndex.chunks[0]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	aCtx, cancelA := context.WithCancel(ctx)
	defer cancelA()
	bCtx, cancelB := context.WithCancel(ctx)
	defer cancelB()
	canceled, cleanup := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, loadCtx context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
		if calls.Add(1) == 1 {
			<-loadCtx.Done()
			close(canceled)
			<-cleanup // Model storage cleanup that finishes after cancellation.
			return nil, loadCtx.Err()
		}
		return nil, io.ErrUnexpectedEOF
	}).Build()
	defer patch.UnPatch()
	start := func(readCtx context.Context) <-chan chunkReadResult {
		done := make(chan chunkReadResult, 1)
		go func() {
			_, err := m.ReadTransform(readCtx, "a", 0, 10, ReadLimits{})
			done <- chunkReadResult{err: err}
		}()
		return done
	}
	a := start(aCtx)
	awaitChunkWaiters(t, &m.chunkIndex.cache, chunk, 1)
	b := start(bCtx)
	old := awaitChunkWaiters(t, &m.chunkIndex.cache, chunk, 2)
	cancelA()
	require.ErrorIs(t, awaitChunkRead(t, a).err, context.Canceled)
	cancelB()
	awaitChunkSignal(t, canceled)
	require.False(t, m.readMu.TryLock(), "last waiter must protect objects until the read actually exits")
	select {
	case <-b:
		t.Fatal("last waiter returned before storage cleanup")
	default:
	}
	lateEntered := make(chan struct{})
	var lateOnce sync.Once
	lateCtx := observedChunkReadContext{Context: ctx, onDone: func() {
		lateOnce.Do(func() { close(lateEntered) })
	}}
	late := start(lateCtx)
	awaitChunkSignal(t, lateEntered)
	// An abandoned load stays installed until cleanup, so it cannot overlap
	// a replacement download or publish into that replacement's state.
	m.chunkIndex.cache.mu.Lock()
	require.Same(t, old, chunk.loading)
	require.Zero(t, old.waiters)
	m.chunkIndex.cache.mu.Unlock()
	require.Equal(t, int64(1), calls.Load())
	close(cleanup)
	require.ErrorIs(t, awaitChunkRead(t, b).err, context.Canceled)
	require.ErrorIs(t, awaitChunkRead(t, late).err, io.ErrUnexpectedEOF)
	require.Equal(t, int64(2), calls.Load())
	require.Nil(t, chunk.loading)
	require.Empty(t, m.chunkIndex.cache.loadSlots)
	require.True(t, m.readMu.TryLock())
	m.readMu.Unlock()
}

func TestChunkLoadRetriesOnceForAllWaiters(t *testing.T) {
	index, chunk := newChunkIndex(0), &indexedChunk{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	aCtx, cancelA := context.WithCancel(ctx)
	defer cancelA()
	backingOff, continueBackoff := make(chan struct{}), make(chan struct{})
	want := &chunkPayload{bytes: []byte("shared payload")}
	var calls, backoffs atomic.Int64
	readPatch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, _ context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
		if calls.Add(1) == 1 {
			return nil, merr.ErrIoTooManyRequests
		}
		return want, nil
	}).Build()
	defer readPatch.UnPatch()
	backoffPatch := mockey.Mock((*backoff.ExponentialBackOff).NextBackOff).To(func(*backoff.ExponentialBackOff) time.Duration {
		backoffs.Add(1)
		close(backingOff)
		<-continueBackoff
		return time.Millisecond
	}).Build()
	defer backoffPatch.UnPatch()
	a := startChunkRead(aCtx, &index.cache, chunk)
	awaitChunkSignal(t, backingOff)
	b := startChunkRead(ctx, &index.cache, chunk)
	c := startChunkRead(ctx, &index.cache, chunk)
	awaitChunkWaiters(t, &index.cache, chunk, 3)
	cancelA()
	require.ErrorIs(t, awaitChunkRead(t, a).err, context.Canceled)
	require.Equal(t, int64(1), calls.Load(), "canceling one waiter must not bypass shared backoff")
	require.Empty(t, index.cache.loadSlots, "backoff must not monopolize download capacity")
	close(continueBackoff)
	for _, result := range []chunkReadResult{awaitChunkRead(t, b), awaitChunkRead(t, c)} {
		require.NoError(t, result.err)
		require.Same(t, want, result.payload)
	}
	require.Equal(t, int64(2), calls.Load())
	require.Equal(t, int64(1), backoffs.Load())
}

func TestChunkLoadTerminalFailureIsShared(t *testing.T) {
	for _, terminal := range []error{io.ErrUnexpectedEOF, ErrStoreCorrupted, merr.ErrDataIntegrity, merr.ErrIoKeyNotFound, merr.ErrIoPermissionDenied} {
		t.Run(terminal.Error(), func(t *testing.T) {
			index, chunk := newChunkIndex(1024), &indexedChunk{}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			aCtx, cancelA := context.WithCancel(ctx)
			defer cancelA()
			resume := make(chan struct{})
			var calls atomic.Int64
			patch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, loadCtx context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
				calls.Add(1)
				select {
				case <-resume:
					return nil, terminal
				case <-loadCtx.Done():
					return nil, loadCtx.Err()
				}
			}).Build()
			defer patch.UnPatch()
			a := startChunkRead(aCtx, &index.cache, chunk)
			b := startChunkRead(ctx, &index.cache, chunk)
			c := startChunkRead(ctx, &index.cache, chunk)
			awaitChunkWaiters(t, &index.cache, chunk, 3)
			cancelA()
			require.ErrorIs(t, awaitChunkRead(t, a).err, context.Canceled)
			close(resume)
			require.ErrorIs(t, awaitChunkRead(t, b).err, terminal)
			require.ErrorIs(t, awaitChunkRead(t, c).err, terminal)
			require.Equal(t, int64(1), calls.Load())
			require.Nil(t, chunk.payload)
			require.Nil(t, chunk.loading)
		})
	}
}

// Observe the real retry.Do's wait after its first failed object request.
// This wrapper belongs to the test's storage operation, not to a subscriber.
type observedChunkReadContext struct {
	context.Context
	onDone func()
}

func (c observedChunkReadContext) Done() <-chan struct{} {
	c.onDone()
	return c.Context.Done()
}

func TestChunkLoadKeepsRemoteReadRetryContext(t *testing.T) {
	index, chunk := newChunkIndex(0), &indexedChunk{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	aCtx, cancelA := context.WithCancel(ctx)
	defer cancelA()
	remote := storage.NewRemoteChunkManagerForTesting(nil, "shared-read-test", "")
	backingOff, resume := make(chan struct{}), make(chan struct{})
	var waitOnce sync.Once
	var gets, reads atomic.Int64
	var failed atomic.Bool
	getPatch := mockey.Mock((*storage.MinioObjectStorage).GetObject).To(func(_ *storage.MinioObjectStorage, _ context.Context, _, _ string, _, _ int64) (storage.FileReader, error) {
		if gets.Add(1) == 1 {
			failed.Store(true)
			return nil, merr.ErrIoTooManyRequests
		}
		return nil, merr.ErrIoKeyNotFound
	}).Build()
	defer getPatch.UnPatch()
	readPatch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, loadCtx context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry, _ *chunkValidation) (*chunkPayload, error) {
		reads.Add(1)
		observed := observedChunkReadContext{Context: loadCtx, onDone: func() {
			if failed.Load() {
				waitOnce.Do(func() {
					close(backingOff)
					<-resume
				})
			}
		}}
		_, err := remote.Read(observed, "chunk")
		return nil, err
	}).Build()
	defer readPatch.UnPatch()
	a := startChunkRead(aCtx, &index.cache, chunk)
	awaitChunkSignal(t, backingOff)
	b := startChunkRead(ctx, &index.cache, chunk)
	c := startChunkRead(ctx, &index.cache, chunk)
	awaitChunkWaiters(t, &index.cache, chunk, 3)
	cancelA()
	require.ErrorIs(t, awaitChunkRead(t, a).err, context.Canceled)
	require.Equal(t, int64(1), reads.Load(), "canceling A must not restart RemoteChunkManager.Read")
	require.Equal(t, int64(1), gets.Load())
	close(resume)
	// Real RemoteChunkManager.Read/retry.Do continues after throttling; its
	// next, permanent error reaches both subscribers, rather than A's cancel.
	require.ErrorIs(t, awaitChunkRead(t, b).err, merr.ErrIoKeyNotFound)
	require.ErrorIs(t, awaitChunkRead(t, c).err, merr.ErrIoKeyNotFound)
	require.Equal(t, int64(1), reads.Load())
	require.Equal(t, int64(2), gets.Load())
}

func TestChunkLoadSharedByIndependentStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, _ := newTestManagerWithStore(t)
	for i, vc := range []string{"a", "b", "c"} {
		m.ObserveMessage(ctx, newTestDeleteMessage(t, vc, uint64(10+i), 1, 42))
	}
	require.NoError(t, persistSummary(ctx, m))
	require.Len(t, m.chunkIndex.chunks, 1)
	chunk := m.chunkIndex.chunks[0]
	resume := make(chan struct{})
	var calls atomic.Int64
	var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry, *chunkValidation) (*chunkPayload, error)
	patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(s *Store, loadCtx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry, verified *chunkValidation) (*chunkPayload, error) {
		index = proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
		calls.Add(1)
		select {
		case <-resume:
			return original(s, loadCtx, index, verified)
		case <-loadCtx.Done():
			return nil, loadCtx.Err()
		}
	}).Build()
	defer patch.UnPatch()
	streams := make([]*Stream, 3)
	handlers := make([]*recordingTransformHandler, 3)
	for i, vc := range []string{"a", "b", "c"} {
		streams[i] = NewStream(m)
		defer streams[i].Close()
		handlers[i] = newRecordingTransformHandler()
		_, err := streams[i].Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: vc, EndTimeTick: 12, Handler: handlers[i]})
		require.NoError(t, err)
		awaitChunkWaiters(t, &m.chunkIndex.cache, chunk, i+1)
	}
	require.NoError(t, streams[0].Close())
	require.ErrorIs(t, nextTransformEvent(t, handlers[0]).Err, context.Canceled)
	close(resume)
	for i, handler := range handlers[1:] {
		event := nextTransformEvent(t, handler)
		require.NoError(t, event.Err)
		require.Equal(t, uint64(11+i), event.Entry.GetTimeTick())
		require.Equal(t, []int64{42}, event.Entry.GetDelete().GetBlocks()[0].GetPrimaryKeys().GetIntId().GetData())
		event = nextTransformEvent(t, handler)
		require.NoError(t, event.Err)
		require.Equal(t, uint64(12), event.SyncUp.TimeTick)
		awaitChunkSignal(t, handler.done)
		require.Empty(t, handler.events)
	}
	require.Equal(t, int64(1), calls.Load())
}
