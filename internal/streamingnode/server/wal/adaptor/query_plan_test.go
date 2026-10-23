package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
		plan, err := w.GetQueryPlan(context.Background(), req)
		patch.UnPatch()
		if valid {
			require.NoError(t, err)
			require.Empty(t, plan.GetWorkNodes())
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
	wait := mockey.Mock((*queryresource.QueryRuntime).WaitMVCCVisible).Return(nil).Build()
	t.Cleanup(func() { wait.UnPatch() })
	return wait
}

func TestQueryPlanWaitsForFixedMVCCBeforePlanning(t *testing.T) {
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

func TestQueryPlanRequiresServingViewBeforeResolvingMVCC(t *testing.T) {
	for _, unavailable := range []error{
		viewerror.NewViewNotFound("view released"),
		viewerror.NewViewInvalidated("view is down"),
	} {
		mockey.PatchConvey("MVCC cannot grant access without a serving View", t, func() {
			mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(nil, unavailable).Build()
			resolve := mockey.Mock((*walAdaptorImpl).resolveQueryPlanMVCC).Return(&viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 120}, nil).Build()
			w := &walAdaptorImpl{
				roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()},
				queryViewHandler: &snview.SNQueryViewHandler{},
			}
			req := &viewpb.GetQueryPlanRequest{
				ShardId: &viewpb.ShardID{Vchannel: "v", ReplicaId: 1},
				Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
			}
			req.Request = &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{SchemaVersion: 1}}
			plan, err := w.GetQueryPlan(t.Context(), req)
			require.ErrorIs(t, err, unavailable)
			require.Nil(t, plan)
			req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{SchemaVersion: 1}}
			plan, err = w.GetQueryPlan(t.Context(), req)
			require.ErrorIs(t, err, unavailable)
			require.Nil(t, plan)
			require.Zero(t, resolve.Times())
		})
	}
}
