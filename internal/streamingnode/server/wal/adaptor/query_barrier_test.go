package adaptor

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryPlanBarrierPreservesRequestedSnapshot(t *testing.T) {
	resource.InitForTest(t)
	mockey.PatchConvey("barrier establishes coverage but never changes read time", t, func() {
		manager := mvcc.NewMVCCManager(0)
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(streamingtypes.PChannelInfo{AccessMode: streamingtypes.AccessModeRW}).Build()
		var barrierErr error
		ensured := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "EnsureBarrier")).To(func(ctx context.Context, _ streamingtypes.PChannelInfo, channel string, tt uint64) error {
			require.Equal(t, "v", channel)
			require.EqualValues(t, 100, tt)
			return barrierErr
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{}, param: &interceptors.InterceptorBuildParam{MVCCManager: manager}}
		req := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 100}}
		mvcc, err := w.resolveQueryPlanMVCC(t.Context(), req, "v")
		require.NoError(t, err)
		require.EqualValues(t, 100, mvcc.GrowingTimetick)
		require.EqualValues(t, 100, mvcc.TransformingTimetick)
		require.Equal(t, 1, ensured.Times())
		barrierErr = context.DeadlineExceeded
		mvcc, err = w.resolveQueryPlanMVCC(t.Context(), req, "v")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, mvcc)
		// A confirmed pair of frontiers satisfies the snapshot without Inspector.
		manager.ApplyRecoveryBarrier("v", 150)
		mvcc, err = w.resolveQueryPlanMVCC(t.Context(), req, "v")
		require.NoError(t, err)
		require.EqualValues(t, 100, mvcc.GetGrowingTimetick())
		require.Equal(t, 2, ensured.Times())
	})
}

func TestQueryPlanFutureBarrierDoesNotBlockSharedSync(t *testing.T) {
	mockey.PatchConvey("future targets wait outside Inspector and preserve fixed snapshots", t, func() {
		client := &mocks.MockMixCoordClient{}
		var allocated atomic.Uint64
		var allocations atomic.Int32
		mockey.Mock((*mocks.MockMixCoordClient).AllocTimestamp).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *rootcoordpb.AllocTimestampRequest, _ ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			start := allocated.Add(uint64(req.GetCount())) - uint64(req.GetCount()) + 1
			allocations.Add(1)
			return &rootcoordpb.AllocTimestampResponse{Status: merr.Success(), Timestamp: start, Count: req.GetCount()}, nil
		}).Build()
		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(client)
		resource.InitForTest(t, resource.OptMixCoordClient(future))
		inspector := resource.Resource().TimeTickInspector()
		defer inspector.Close()
		allocator := resource.Resource().TSOAllocator()
		channel := streamingtypes.PChannelInfo{Name: "future-barrier", Term: 1, AccessMode: streamingtypes.AccessModeRW}
		operator := &mock_inspector.MockTimeTickSyncOperator{}
		mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).Channel).Return(channel).Build()
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(channel).Build()
		var normalSyncs, durableSyncs atomic.Int32
		mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).Sync).To(func(_ *mock_inspector.MockTimeTickSyncOperator, _ context.Context, persisted bool) {
			if persisted {
				durableSyncs.Add(1)
			} else {
				normalSyncs.Add(1)
			}
		}).Build()
		var scopedSyncs atomic.Int32
		mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).SyncWithQueryBarriers).To(func(_ *mock_inspector.MockTimeTickSyncOperator, ctx context.Context, _ bool, targets map[string]uint64) (map[string]uint64, error) {
			scopedSyncs.Add(1)
			// Keep the operator's real allocator prerequisite: without request-side
			// admission an impossible target would occupy the shared working slot.
			for _, target := range targets {
				if err := allocator.BarrierUntil(ctx, target); err != nil {
					return nil, err
				}
			}
			return targets, nil
		}).Build()
		inspector.RegisterSyncOperator(operator)
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{}, param: &interceptors.InterceptorBuildParam{MVCCManager: mvcc.NewMVCCManager(0)}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := w.resolveQueryPlanMVCC(ctx, &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: math.MaxUint64}}, "v")
			done <- err
		}()
		require.Eventually(t, func() bool { return allocations.Load() >= 2 }, time.Second, time.Millisecond)
		assertSyncsProgress := func() {
			normal, durable := normalSyncs.Load(), durableSyncs.Load()
			require.Eventually(t, func() bool {
				inspector.TriggerSync(channel, false)
				return normalSyncs.Load() > normal
			}, time.Second, time.Millisecond)
			require.Eventually(t, func() bool {
				inspector.TriggerSync(channel, true)
				return durableSyncs.Load() > durable
			}, time.Second, time.Millisecond)
		}
		assertSyncsProgress()
		require.Zero(t, scopedSyncs.Load(), "future target must not enter shared sync")
		select {
		case err := <-done:
			t.Fatalf("unallocated target unexpectedly completed: %v", err)
		default:
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("future target outlived its request")
		}
		assertSyncsProgress()

		// Emulate B allocated by another node, ahead of this node's cached batch.
		ctx, cancelValid := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancelValid()
		external, err := client.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{Count: 1})
		require.NoError(t, err)
		req := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: external.GetTimestamp()}}
		for range 2 { // Subsequent iterator pages must retain the same B.
			plan, err := w.resolveQueryPlanMVCC(ctx, req, "v")
			require.NoError(t, err)
			require.Equal(t, external.GetTimestamp(), plan.GetGrowingTimetick())
			require.Equal(t, external.GetTimestamp(), plan.GetTransformingTimetick())
		}
		require.Positive(t, scopedSyncs.Load())
	})
}

func TestQueryPlanCancellationDoesNotHoldWALShutdown(t *testing.T) {
	for _, ownerCanceled := range []bool{true, false} {
		mockey.PatchConvey("owner cancellation releases the pending plan and lease", t, func() {
			resource.InitForTest(t)
			defer resource.Resource().TimeTickInspector().Close()
			caller, cancelCaller := context.WithCancel(t.Context())
			defer cancelCaller()
			owner, cancel := context.WithCancel(t.Context())
			defer cancel()
			meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v", ReplicaId: 1}
			released := make(chan struct{})
			lease := &snview.QueryViewLease{Meta: meta, Release: func() { close(released) }}
			mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
			entered := make(chan struct{})
			mockey.Mock((*roWALAdaptorImpl).Channel).Return(streamingtypes.PChannelInfo{AccessMode: streamingtypes.AccessModeRW}).Build()
			mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "BarrierUntil")).To(func(ctx context.Context, _ uint64) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			ensured := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "EnsureBarrier")).Return(nil).Build()
			w := &walAdaptorImpl{
				roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: owner, lifetime: typeutil.NewLifetime()},
				queryViewHandler: &snview.SNQueryViewHandler{},
				param:            &interceptors.InterceptorBuildParam{MVCCManager: mvcc.NewMVCCManager(0)},
			}
			done := make(chan error, 1)
			go func() {
				_, err := w.GetQueryPlan(caller, &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 100}})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("plan did not start waiting")
			}
			if ownerCanceled {
				cancel()
			} else {
				cancelCaller()
			}
			select {
			case err := <-done:
				if ownerCanceled {
					require.True(t, viewerror.AsViewError(err).IsOnShutdown())
					wire := viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
					require.True(t, viewerror.AsViewError(viewerror.ConvertViewError("GetQueryPlan", wire)).IsRetryable())
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("plan kept waiting after WAL shutdown")
			}
			<-released
			require.Zero(t, ensured.Times(), "canceled allocator wait must not enqueue shared sync work")
		})
	}
}
