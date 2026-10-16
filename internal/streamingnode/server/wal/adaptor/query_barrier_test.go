package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryPlanBarrierPreservesRequestedSnapshot(t *testing.T) {
	resource.InitForTest(t)
	mockey.PatchConvey("barrier establishes coverage but never changes read time", t, func() {
		manager := mvcc.NewMVCCManager(0)
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		var barrierErr error
		ensured := mockey.Mock(mockey.GetMethod(resource.Resource().TimeTickInspector(), "EnsureBarrier")).To(func(ctx context.Context, _ types.PChannelInfo, channel string, tt uint64) error {
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

func TestQueryPlanCancellationDoesNotHoldWALShutdown(t *testing.T) {
	for _, ownerCanceled := range []bool{true, false} {
		mockey.PatchConvey("owner cancellation releases the pending plan and lease", t, func() {
			caller, cancelCaller := context.WithCancel(t.Context())
			defer cancelCaller()
			owner, cancel := context.WithCancel(t.Context())
			defer cancel()
			meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v", ReplicaId: 1}
			released := make(chan struct{})
			lease := &snview.QueryViewLease{Meta: meta, Release: func() { close(released) }}
			mockey.Mock((*snview.SNQueryViewHandler).AcquireLatestUpView).Return(lease, nil).Build()
			entered := make(chan struct{})
			mockey.Mock((*walAdaptorImpl).resolveQueryPlanMVCC).To(func(_ *walAdaptorImpl, ctx context.Context, _ *viewpb.GetQueryPlanRequest, _ string) (*viewpb.QueryPlanMVCC, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}).Build()
			w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: owner, lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}}
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
		})
	}
}
