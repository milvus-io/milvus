package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryMVCCShutdownPreservesViewError(t *testing.T) {
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}}
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	_, err := w.GetLatestQueryPlanMVCC(context.Background(), "p_1v0")
	require.True(t, viewerror.AsViewError(err).IsOnShutdown())
	wire := viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
	require.True(t, viewerror.AsViewError(viewerror.ConvertViewError("GetMVCCTimestamp", wire)).IsRetryable())
}

func TestQueryPlanPrunesSNUsingSelectedViewDataVersion(t *testing.T) {
	sealedAt := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 2}
	patch := mockey.Mock((*queryresource.QueryRuntime).MayHaveVisibleGrowingSegments).To(func(_ *queryresource.QueryRuntime, version qviews.DataVersion, growing uint64, transform uint64, partitions []int64) bool {
		require.Equal(t, uint64(100), growing)
		require.Equal(t, uint64(90), transform)
		require.Equal(t, []int64{100}, partitions)
		return sealedAt.GT(version)
	}).Build()
	defer patch.UnPatch()
	options := queryPlanWorkNodeOptions{runtime: &queryresource.QueryRuntime{}, partitionIDs: []int64{100}, mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{Vchannel: "p_1v0", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 9}}}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
	require.True(t, queryPlanIncludesStreamingNode(view, options))
	view.Meta.Version.DataVersion = sealedAt.IntoProto()
	require.False(t, queryPlanIncludesStreamingNode(view, options))
}

func TestQueryPlanRenewsLeaseOnlyOnSuccessfulReturn(t *testing.T) {
	patchQueryReadRuntime(t)
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}}
	for _, valid := range []bool{false, true} {
		renewed, released := false, false
		lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { require.False(t, released); renewed = true }, Release: func() { released = true }}
		patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
		req := &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}}}
		if valid {
			req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1}}
		}
		_, err := w.GetQueryPlan(context.Background(), req)
		patch.UnPatch()
		if valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.Equal(t, valid, renewed)
		require.True(t, released)
	}
}

func TestQueryPlanCannotUseOldSchemaView(t *testing.T) {
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}}
	lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { t.Fatal("incompatible view must not renew a serving lease") }, Release: func() {}}
	patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
	defer patch.UnPatch()
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
	req := &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}}}
	req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{SchemaVersion: 2}}
	_, err := w.GetQueryPlan(t.Context(), req)
	require.True(t, viewerror.AsViewError(err).IsRetryable())
	req.Request = &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{SchemaVersion: 2}}
	_, err = w.GetQueryPlan(t.Context(), req)
	require.True(t, viewerror.AsViewError(err).IsRetryable())
	req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{SchemaVersion: 0}}
	_, err = w.GetQueryPlan(t.Context(), req)
	require.True(t, viewerror.AsViewError(err).IsRetryable(), "newer schema cannot satisfy an old request")
}

func TestQueryPlanRejectsUnloadedPartitionBeforePruning(t *testing.T) {
	patchQueryReadRuntime(t)
	mockey.PatchConvey("even an empty shard must validate the leased scope", t, func() {
		view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, LoadInfoVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}}
		lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { t.Fatal("invalid partition must not renew lease") }, Release: func() {}}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
		mockey.Mock((*queryresource.QueryRuntime).ResolvePartitions).To(func(_ *queryresource.QueryRuntime, version uint64, partitions []int64) ([]int64, error) {
			require.Equal(t, uint64(2), version)
			require.Equal(t, []int64{20}, partitions)
			return nil, merr.WrapErrPartitionNotLoaded(20)
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
		req := &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}}}
		req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{PartitionIDs: []int64{20}}}
		_, err := w.GetQueryPlan(t.Context(), req)
		require.ErrorIs(t, err, merr.ErrPartitionNotLoaded)
		req.Request = &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{PartitionIDs: []int64{20}}}
		_, err = w.GetQueryPlan(t.Context(), req)
		require.ErrorIs(t, err, merr.ErrPartitionNotLoaded)
	})
}

func patchQueryReadRuntime(t *testing.T) *mockey.Mocker {
	t.Helper()
	runtime := &queryresource.ReadRuntime{QueryRuntime: queryresource.NewQueryRuntime()}
	patch := mockey.Mock((*vchannel.PChannelRecoveryManager).GetQueryReadRuntime).Return(runtime, true).Build()
	t.Cleanup(func() { patch.UnPatch() })
	optimizer := mockey.Mock((*queryresource.ReadRuntime).GlobalOptimizer).Return(queryresource.NewGlobalOptimizer(nil, "test")).Build()
	t.Cleanup(func() { optimizer.UnPatch() })
	schema := mockey.Mock((*queryresource.ReadRuntime).Schema).Return(&schemapb.CollectionSchema{}).Build()
	t.Cleanup(func() { schema.UnPatch() })
	validate := mockey.Mock((*walAdaptorImpl).validateQuerySchemaFields).Return(nil).Build()
	t.Cleanup(func() { validate.UnPatch() })
	wait := mockey.Mock((*queryresource.QueryRuntime).WaitMVCCVisible).Return(nil).Build()
	t.Cleanup(func() { wait.UnPatch() })
	return wait
}

func TestQueryPlanWaitsForFixedMVCCBeforeSchemaValidation(t *testing.T) {
	wait := patchQueryReadRuntime(t)
	wait.UnPatch()
	calls := 0
	waiting := mockey.Mock((*queryresource.QueryRuntime).WaitMVCCVisible).To(func(_ *queryresource.QueryRuntime, ctx context.Context, growing, transform uint64) error {
		calls++
		require.EqualValues(t, 123, growing)
		require.EqualValues(t, 122, transform)
		return context.DeadlineExceeded
	}).Build()
	defer waiting.UnPatch()
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}}}
	released := false
	lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { t.Fatal("waiting for WAL coverage cannot issue a plan") }, Release: func() { released = true }}
	patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
	defer patch.UnPatch()
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
	request := &viewpb.GetQueryPlanRequest{
		ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(),
		Mvcc:    &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 123, TransformingTimetick: 122}},
		Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{}},
	}
	plan, err := w.GetQueryPlan(t.Context(), request)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, plan)
	require.Equal(t, 1, calls)
	require.True(t, released)
}

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

func TestQueryPlanBarrierDoesNotHoldWALShutdown(t *testing.T) {
	mockey.PatchConvey("owner cancellation releases the pending plan and lease", t, func() {
		owner, cancel := context.WithCancel(t.Context())
		defer cancel()
		meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v", ReplicaId: 1}
		released := make(chan struct{})
		lease := &snview.QueryViewLease{Meta: meta, Release: func() { close(released) }}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
		entered := make(chan struct{})
		mockey.Mock((*walAdaptorImpl).resolveQueryPlanMVCC).To(func(_ *walAdaptorImpl, ctx context.Context, _ *viewpb.GetQueryPlanRequest, _ string) (*viewpb.QueryPlanMVCC, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: owner, lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}}
		done := make(chan error, 1)
		go func() {
			_, err := w.GetQueryPlan(t.Context(), &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 100}})
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("plan did not start waiting")
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("plan kept waiting after WAL shutdown")
		}
		<-released
	})
}
