package walsummary

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryStreamRetriesTransientReads(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, firstRows := range []int{1, 4096} {
			t.Run(fmt.Sprintf("bounded=%t/firstRows=%d", bounded, firstRows), func(t *testing.T) {
				ctx := context.Background()
				m, _ := newTestManagerWithStore(t) // No resident cache: exercise object reads.
				keys := make([]int64, firstRows)
				for i := range keys {
					keys[i] = int64(i)
				}
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, keys...))
				require.NoError(t, persistSummary(ctx, m))
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20, 1, 20))
				require.NoError(t, persistSummary(ctx, m))
				require.Len(t, m.chunkIndex.chunks, 2)
				firstGeneration := m.chunkIndex.chunks[0].GetGeneration()
				secondGeneration := m.chunkIndex.chunks[1].GetGeneration()
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 30, 1, 30))
				observeReadBarrier(m, 40)
				failures := []error{
					merr.WrapErrIoFailed("chunk", errors.New("temporary storage failure")),
					errors.New("unclassified read failure"),
					context.DeadlineExceeded, // An operation timeout, not subscription expiry.
					context.Canceled,         // A canceled lower-level attempt can also be retried.
				}
				var attempts, firstChunkReads atomic.Int64
				blocked, resume := make(chan struct{}), make(chan struct{})
				var original func(*Store, context.Context, *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error)
				patch := mockey.Mock((*Store).readChunkPayload).Origin(&original).To(func(store *Store, ctx context.Context, index *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error) {
					index = proto.Clone(index).(*streamingpb.PChannelSummaryChunkIndexEntry)
					if index.GetGeneration() == firstGeneration {
						firstChunkReads.Add(1)
					}
					if index.GetGeneration() == secondGeneration {
						attempt := int(attempts.Add(1))
						if attempt <= len(failures) {
							return nil, failures[attempt-1]
						}
						close(blocked)
						select {
						case <-resume:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return original(store, ctx, index)
				}).Build()
				defer patch.UnPatch()
				stream := NewStream(m)
				defer stream.Close()
				handler := newRecordingTransformHandler()
				opt := wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler}
				if bounded {
					opt.EndTimeTick = 40
				}
				sub, err := stream.Subscribe(ctx, opt)
				require.NoError(t, err)
				defer sub.Close()
				if firstRows == 4096 {
					// The first page was already delivered; it must not be replayed.
					require.Equal(t, uint64(10), nextTransformEvent(t, handler).Entry.GetTimeTick())
				}
				select {
				case <-blocked:
				case <-time.After(5 * time.Second):
					t.Fatal("subscription did not retry failed reads")
				}
				select {
				case event := <-handler.events:
					t.Fatalf("failed batch must not deliver a partial entry, error or SyncUp: %v", event)
				default:
				}
				close(resume)
				if firstRows == 1 {
					require.Equal(t, uint64(10), nextTransformEvent(t, handler).Entry.GetTimeTick())
				}
				for _, tt := range []uint64{20, 30} {
					event := nextTransformEvent(t, handler)
					require.NoError(t, event.Err)
					require.Equal(t, tt, event.Entry.GetTimeTick())
					require.Equal(t, []int64{int64(tt)}, event.Entry.GetDelete().GetBlocks()[0].GetPrimaryKeys().GetIntId().GetData())
				}
				event := nextTransformEvent(t, handler)
				require.NoError(t, event.Err)
				require.NotNil(t, event.SyncUp)
				require.Equal(t, uint64(40), event.SyncUp.TimeTick)
				if firstRows == 4096 {
					require.Equal(t, int64(1), firstChunkReads.Load(), "retry must retain the accepted page cursor")
				}
				require.Equal(t, int64(len(failures)+1), attempts.Load())
			})
		}
	}
}

func TestQueryStreamReadFailureCancellation(t *testing.T) {
	for _, waiting := range []string{"backoff", "object read"} {
		for _, closeBy := range []string{"context", "subscription", "stream"} {
			t.Run(waiting+"/"+closeBy, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				m, _ := newTestManagerWithStore(t)
				m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
				require.NoError(t, persistSummary(ctx, m))
				waitingCh := make(chan struct{})
				var attempts atomic.Int64
				readPatch := mockey.Mock((*Store).readChunkPayload).To(func(_ *Store, ctx context.Context, _ *streamingpb.PChannelSummaryChunkIndexEntry) (*chunkPayload, error) {
					attempts.Add(1)
					if waiting == "object read" {
						close(waitingCh)
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, merr.ErrIoFailed
				}).Build()
				defer readPatch.UnPatch()
				if waiting == "backoff" {
					patch := mockey.Mock((*backoff.ExponentialBackOff).NextBackOff).To(func(*backoff.ExponentialBackOff) time.Duration {
						close(waitingCh)
						return time.Hour
					}).Build()
					defer patch.UnPatch()
				}
				stream := NewStream(m)
				defer stream.Close()
				handler := newRecordingTransformHandler()
				sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
				require.NoError(t, err)
				defer sub.Close()
				select {
				case <-waitingCh:
				case <-time.After(time.Second):
					t.Fatal("read did not enter the wait")
				}
				if waiting == "backoff" {
					require.True(t, m.readMu.TryLock(), "backoff must not hold the read/GC pin")
					m.readMu.Unlock()
				}
				closed := make(chan error, 1)
				go func() {
					switch closeBy {
					case "context":
						cancel()
						<-handler.done
						closed <- nil
					case "subscription":
						closed <- sub.Close()
					case "stream":
						closed <- stream.Close()
					}
				}()
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("cancellation did not interrupt the read/retry")
				}
				require.ErrorIs(t, nextTransformEvent(t, handler).Err, context.Canceled)
				require.Equal(t, int64(1), attempts.Load(), "canceled subscription must not retry")
				m.mu.Lock()
				require.Empty(t, m.transformNotifiers, "subscription must release its watcher")
				m.mu.Unlock()
			})
		}
	}
}

func TestQueryStreamTerminalReadFailures(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "integrity", "manager terminal"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			m, store := newTestManagerWithStore(t)
			m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
			require.NoError(t, persistSummary(ctx, m))
			index := m.Manifest().GetChunks()[0]
			key := buildChunkKey(store.chunkManager, store.pchannel, index.GetGeneration(), index.GetTerm())
			want := ErrStoreCorrupted
			switch kind {
			case "missing":
				require.NoError(t, store.chunkManager.Remove(ctx, key))
				want = merr.ErrIoKeyNotFound
			case "corrupt":
				require.NoError(t, store.chunkManager.Write(ctx, key, []byte("damaged chunk")))
			case "integrity":
				patch := mockey.Mock((*Store).readChunkPayload).Return(nil, merr.Wrap(merr.ErrDataIntegrity, "bad stored data")).Build()
				defer patch.UnPatch()
				want = merr.ErrDataIntegrity
			case "manager terminal":
				m.mu.Lock()
				m.setTerminalErrorLocked(storeCorruptedf("summary stopped"))
				m.mu.Unlock()
			}
			stream := NewStream(m)
			defer stream.Close()
			handler := newRecordingTransformHandler()
			sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", EndTimeTick: 10, Handler: handler})
			require.NoError(t, err)
			defer sub.Close()
			event := nextTransformEvent(t, handler)
			require.ErrorIs(t, event.Err, want)
			require.Nil(t, event.Entry)
			require.Nil(t, event.SyncUp)
			select {
			case <-handler.done:
			case <-time.After(time.Second):
				t.Fatal("terminal error did not close subscription")
			}
			require.Empty(t, handler.events)
		})
	}
}

func TestQueryStreamDoesNotRetryHandlerFailure(t *testing.T) {
	m := NewManager(ManagerConfig{})
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 10, 1, 10))
	var calls atomic.Int64
	var original func(*recordingTransformHandler, wal.TransformLogStreamEvent) error
	patch := mockey.Mock((*recordingTransformHandler).Handle).Origin(&original).To(func(h *recordingTransformHandler, event wal.TransformLogStreamEvent) error {
		if event.Entry != nil {
			calls.Add(1)
			return merr.ErrIoFailed
		}
		return original(h, event)
	}).Build()
	defer patch.UnPatch()
	stream := NewStream(m)
	defer stream.Close()
	handler := newRecordingTransformHandler()
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", EndTimeTick: 10, Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	require.ErrorIs(t, nextTransformEvent(t, handler).Err, merr.ErrIoFailed)
	require.NoError(t, sub.Close())
	require.Equal(t, int64(1), calls.Load())
	require.Empty(t, handler.events, "failed delivery must not emit SyncUp")
}
