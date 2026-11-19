package ack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type allocationResult struct {
	acker *Acker
	err   error
}

func TestAckDoesNotWaitForTimestampRefill(t *testing.T) {
	for _, cancelRefill := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "canceled"}[cancelRefill], func(t *testing.T) {
			paramtable.Init()
			ctx, cancel := context.WithCancel(t.Context())
			refillStarted := make(chan struct{})
			refillAllowed := make(chan struct{})
			releaseRefill := sync.OnceFunc(func() { close(refillAllowed) })
			var workers sync.WaitGroup
			defer func() {
				cancel()
				releaseRefill()
				workers.Wait()
			}()

			var rpcCalls atomic.Int32
			var timestamps atomic.Uint64
			mix := mocks.NewMockMixCoordClient(t)
			mix.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, _ ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
					if rpcCalls.Add(1) == 2 {
						close(refillStarted)
						select {
						case <-refillAllowed:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return &rootcoordpb.AllocTimestampResponse{
						Status:    merr.Success(),
						Timestamp: timestamps.Add(uint64(req.Count)) - uint64(req.Count) + 1,
						Count:     req.Count,
					}, nil
				})
			future := syncutil.NewFuture[types.MixCoordClient]()
			future.Set(mix)
			resource.InitForTest(t, resource.OptMixCoordClient(future))
			t.Cleanup(resource.Resource().TimeTickInspector().Close)
			metrics := metricsutil.NewTimeTickMetrics(t.Name())
			t.Cleanup(metrics.Close)
			initialMessageID := walimplstest.NewTestMessageID(0)
			manager := NewAckManager(0, initialMessageID, metrics)

			first, err := manager.Allocate(ctx)
			require.NoError(t, err)
			persistedFlush := message.NewFlushMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.FlushMessageHeader{CollectionId: 1, PartitionId: 2, SegmentId: 3}).
				WithBody(&message.FlushMessageBody{}).MustBuildMutable().
				WithTimeTick(first.Timestamp()).WithLastConfirmed(initialMessageID).
				IntoImmutableMessage(walimplstest.NewTestMessageID(1))
			// Exercise the real shared allocator, including its mutex and RPC refill.
			resource.Resource().TSOAllocator().Sync()
			allocated := make(chan allocationResult, 1)
			workers.Go(func() {
				acker, err := manager.Allocate(ctx)
				allocated <- allocationResult{acker, err}
			})
			select {
			case <-refillStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timestamp refill did not start")
			}

			acked := make(chan struct{})
			workers.Go(func() {
				first.Ack(OptImmutableMessage(persistedFlush))
				close(acked)
			})
			select {
			case <-acked:
				require.Equal(t, first.Timestamp(), first.detail.EndTimestamp)
			case <-time.After(time.Second):
				t.Error("completed append could not acknowledge while timestamp refill was blocked")
			}

			if cancelRefill {
				cancel()
			} else {
				releaseRefill()
			}
			result := <-allocated
			<-acked
			if cancelRefill {
				require.ErrorIs(t, result.err, context.Canceled)
				require.Nil(t, result.acker)
				require.Zero(t, manager.notAckHeap.Len())
			} else {
				require.NoError(t, result.err)
				result.acker.Ack(OptSync())
			}
			// Both successful and failed refills must release allocation serialization.
			next, err := manager.Allocate(t.Context())
			require.NoError(t, err)
			next.Ack(OptSync())
			require.Equal(t, next.Timestamp(), manager.lastConfirmedTimeTick)
			require.True(t, persistedFlush.MessageID().EQ(manager.lastConfirmedManager.GetLastConfirmedMessageID()))
			require.Zero(t, manager.notAckHeap.Len())
		})
	}
}

func TestTimestampSyncDoesNotOvertakeUnregisteredAllocation(t *testing.T) {
	manager := newAllocationRetryManager(t)
	t.Cleanup(resource.Resource().TimeTickInspector().Close)
	firstAllocated := make(chan struct{})
	registrationAllowed := make(chan struct{})
	allowRegistration := sync.OnceFunc(func() { close(registrationAllowed) })
	var workers sync.WaitGroup
	defer func() {
		allowRegistration()
		workers.Wait()
	}()

	var timestamps atomic.Uint64
	allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, _ context.Context) (uint64, error) {
		ts := timestamps.Add(1)
		if ts == 1 {
			// A timestamp exists but its acker has not been registered yet.
			close(firstAllocated)
			<-registrationAllowed
		}
		return ts, nil
	}).Build()
	t.Cleanup(func() { allocate.UnPatch() })
	syncStarted := make(chan struct{})
	notifySync := sync.OnceFunc(func() { close(syncStarted) })
	syncExpired := mockey.Mock((*retryTestTSOAllocator).SyncIfExpired).To(func(_ *retryTestTSOAllocator, _ time.Duration) {
		notifySync()
	}).Build()
	t.Cleanup(func() { syncExpired.UnPatch() })

	allocated := make(chan allocationResult, 1)
	workers.Go(func() {
		acker, err := manager.Allocate(t.Context())
		allocated <- allocationResult{acker, err}
	})
	<-firstAllocated
	type syncResult struct {
		details []*AckDetail
		err     error
	}
	synced := make(chan syncResult, 1)
	workers.Go(func() {
		details, err := manager.SyncAndGetAcknowledged(t.Context())
		synced <- syncResult{details, err}
	})
	<-syncStarted
	select {
	case result := <-synced:
		t.Fatalf("sync overtook an unregistered timestamp: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	allowRegistration()
	first := <-allocated
	require.NoError(t, first.err)
	result := <-synced
	require.NoError(t, result.err)
	require.Empty(t, result.details, "sync must not cross the oldest unacknowledged timestamp")

	first.acker.Ack(OptSync())
	details, err := manager.SyncAndGetAcknowledged(t.Context())
	require.NoError(t, err)
	require.Len(t, details, 3)
	for i, detail := range details {
		require.EqualValues(t, i+1, detail.BeginTimestamp)
	}
}

func TestTimestampBarrierAllowsOtherAllocations(t *testing.T) {
	manager := newAllocationRetryManager(t)
	t.Cleanup(resource.Resource().TimeTickInspector().Close)
	ctx, cancel := context.WithCancel(t.Context())
	barrierStarted := make(chan struct{})
	ordinaryAllocated := make(chan struct{})
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()

	var timestamps atomic.Uint64
	allocate := mockey.Mock((*retryTestTSOAllocator).Allocate).To(func(_ *retryTestTSOAllocator, _ context.Context) (uint64, error) {
		return 100 + timestamps.Add(1), nil
	}).Build()
	t.Cleanup(func() { allocate.UnPatch() })
	barrier := mockey.Mock((*retryTestTSOAllocator).BarrierUntil).To(func(_ *retryTestTSOAllocator, ctx context.Context, _ uint64) error {
		close(barrierStarted)
		select {
		case <-ordinaryAllocated:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Build()
	t.Cleanup(func() { barrier.UnPatch() })

	barrierResult := make(chan allocationResult, 1)
	workers.Go(func() {
		acker, err := manager.AllocateWithBarrier(ctx, 100)
		barrierResult <- allocationResult{acker, err}
	})
	<-barrierStarted
	ordinaryResult := make(chan allocationResult, 1)
	workers.Go(func() {
		acker, err := manager.Allocate(ctx)
		ordinaryResult <- allocationResult{acker, err}
		close(ordinaryAllocated)
	})
	select {
	case <-ordinaryAllocated:
	case <-time.After(time.Second):
		t.Fatal("barrier prevented the allocations needed to advance it")
	}
	ordinary := <-ordinaryResult
	withBarrier := <-barrierResult
	require.NoError(t, ordinary.err)
	require.NoError(t, withBarrier.err)
	require.Less(t, ordinary.acker.Timestamp(), withBarrier.acker.Timestamp())
	withBarrier.acker.Ack(OptSync())
	require.Zero(t, manager.lastConfirmedTimeTick)
	ordinary.acker.Ack(OptSync())
	require.Equal(t, withBarrier.acker.Timestamp(), manager.lastConfirmedTimeTick)
}
