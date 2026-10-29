package adaptor

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/optimizer"
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

func TestStatisticsPlanCountsSNAndReturnsOnlySelectedQNs(t *testing.T) {
	patchQueryReadRuntime(t)
	mockey.PatchConvey("statistics samples the latest View without a logical schema request", t, func() {
		view := &viewpb.QueryViewOfShard{
			Meta:          &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 3, SchemaVersion: 7, LoadInfoVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 8}, QueryVersion: 9}},
			StreamingNode: &viewpb.QueryViewOfStreamingNode{},
			QueryNode: []*viewpb.QueryViewOfQueryNode{
				{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}}},
				{NodeId: 12, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 20, SegmentIds: []int64{200}}}},
			},
		}
		counted, released := false, false
		lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { require.True(t, counted); require.False(t, released) }, Release: func() { released = true }}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireLatestUpView).Return(lease, nil).Build()
		mockey.Mock((*queryresource.QueryRuntime).ResolvePartitions).Return([]int64{10}, nil).Build()
		mockey.Mock((*snview.SNQueryViewHandler).Statistics).To(func(_ *snview.SNQueryViewHandler, _ context.Context, req *viewpb.StatisticsOnViewRequest) (int64, error) {
			require.Equal(t, lease.Version.IntoProto(), req.GetVersion())
			require.EqualValues(t, 3, req.GetShardId().GetReplicaId())
			require.Equal(t, []int64{10}, req.GetPartitionIds())
			require.EqualValues(t, 99, req.GetMvcc().GetGrowingTimetick())
			require.EqualValues(t, 99, req.GetMvcc().GetTransformingTimetick())
			counted = true
			return 23, nil
		}).Build()
		manager := mvcc.NewMVCCManager(100)
		manager.ApplyRecoveryBarrier("p_1v0", 100)
		mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		w := &walAdaptorImpl{param: &interceptors.InterceptorBuildParam{MVCCManager: manager}, roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
		plan, err := w.GetQueryPlan(t.Context(), &viewpb.GetQueryPlanRequest{
			CollectionId: 1, ShardId: &viewpb.ShardID{Vchannel: "p_1v0"},
			Mvcc:    &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 99},
			Request: &viewpb.GetQueryPlanRequest_StatisticsRequest{StatisticsRequest: &viewpb.StatisticsRequest{PartitionIds: []int64{10}}},
		})
		require.NoError(t, err)
		require.True(t, released)
		require.EqualValues(t, 23, plan.GetGrowingInlineResponse().GetStatisticsResponse().GetRowCount())
		require.Len(t, plan.GetPlan().GetWorkNodes(), 1)
		require.EqualValues(t, 11, plan.GetPlan().GetWorkNodes()[0].GetQueryNode().GetNodeId())
	})
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
			require.Empty(t, plan.GetPlan().GetWorkNodes())
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
	optimizer := mockey.Mock((*queryresource.ReadRuntime).GlobalOptimizer).Return(optimizer.NewNoopGlobalOptimizer()).Build()
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

func TestQueryPlanReturnsOptimizerSummarySeparately(t *testing.T) {
	patchQueryReadRuntime(t)
	view := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}}},
			{NodeId: 12, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 20, SegmentIds: []int64{200}}}},
		},
	}
	lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() {}, Release: func() {}}
	patch := mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
	defer patch.UnPatch()
	partitions := mockey.Mock((*queryresource.QueryRuntime).ResolvePartitions).To(func(_ *queryresource.QueryRuntime, _ uint64, ids []int64) ([]int64, error) {
		return ids, nil
	}).Build()
	defer partitions.UnPatch()
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
	for _, ids := range [][]int64{{10}, {10, 20}} {
		response, err := w.GetQueryPlan(t.Context(), &viewpb.GetQueryPlanRequest{
			ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(),
			Mvcc:    &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 1}},
			Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1, PartitionIDs: ids}},
		})
		require.NoError(t, err)
		require.Len(t, response.GetPlan().GetWorkNodes(), len(ids))
		if len(ids) == 1 {
			require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByPartitionFiltering), response.GetExecutionSummary().GetGlobalOptimizer().GetFlags())
		} else {
			require.Nil(t, response.GetExecutionSummary())
		}
	}
}

func TestQueryPlanPartitionSelectionAndSummary(t *testing.T) {
	view := &viewpb.QueryViewOfShard{QueryNode: []*viewpb.QueryViewOfQueryNode{
		{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: []int64{11}}, {PartitionId: 2, SegmentIds: []int64{12}}}},
		{NodeId: 2, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{22}}}},
		{NodeId: 3, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 3, SegmentIds: []int64{33}}}},
		{NodeId: 4, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 99}}},
	}}
	for _, tc := range []struct {
		name     string
		req      *internalpb.SearchRequest
		nodes    []int64
		filtered bool
	}{
		{"unrestricted", &internalpb.SearchRequest{}, []int64{1, 2, 3}, false},
		{"all_selected", &internalpb.SearchRequest{PartitionIDs: []int64{1, 2, 3}}, []int64{1, 2, 3}, false},
		{"partial_worker", &internalpb.SearchRequest{PartitionIDs: []int64{1}}, []int64{1}, true},
		{"shared_partition", &internalpb.SearchRequest{PartitionIDs: []int64{2}}, []int64{1, 2}, true},
		{"all_excluded", &internalpb.SearchRequest{PartitionIDs: []int64{99}}, nil, true},
		{"hybrid_union_all", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{1, 2}}, {PartitionIDs: []int64{2, 3}}}}, []int64{1, 2, 3}, true},
		{"hybrid_identical_all", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{1, 2, 3}}, {PartitionIDs: []int64{1, 2, 3}}}}, []int64{1, 2, 3}, false},
		{"hybrid_unrestricted_and_bounded", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{}, {PartitionIDs: []int64{2}}}}, []int64{1, 2, 3}, true},
		{"hybrid_skipped_and_unrestricted", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{2}, Skip: true}, {}}}, []int64{1, 2, 3}, false},
		{"hybrid_disjoint", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{1}}, {PartitionIDs: []int64{3}}}}, []int64{1, 3}, true},
		{"hybrid_duplicate_ids", &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{2, 2}}, {PartitionIDs: []int64{2}}}}, []int64{1, 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := searchQueryPlanWorkNodeOptions(tc.req, nil, nil)
			workers, filtered := buildQueryPlanWorkNodes(view, options)
			var ids []int64
			for _, worker := range workers {
				ids = append(ids, worker.GetQueryNode().GetNodeId())
			}
			require.Equal(t, tc.nodes, ids)
			require.Equal(t, tc.filtered, filtered)
			// Empty partitions/views are not proof of any candidate exclusion.
			workers, filtered = buildQueryPlanWorkNodes(&viewpb.QueryViewOfShard{QueryNode: view.QueryNode[3:]}, options)
			require.Empty(t, workers)
			require.False(t, filtered)
		})
	}
}

func BenchmarkQueryPlanPartitionSelection(b *testing.B) {
	for _, count := range []int{32, 1024, 8192} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			ids := make([]int64, count)
			partitions := make([]*viewpb.QueryViewOfPartition, count)
			for i := range ids {
				ids[i] = int64(i)
				partitions[i] = &viewpb.QueryViewOfPartition{PartitionId: ids[i], SegmentIds: []int64{ids[i]}}
			}
			view := &viewpb.QueryViewOfShard{QueryNode: []*viewpb.QueryViewOfQueryNode{{NodeId: 1, Partitions: partitions}}}
			options := queryPlanWorkNodeOptions{partitionIDs: ids}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				workers, filtered := buildQueryPlanWorkNodes(view, options)
				if len(workers) != 1 || filtered {
					b.Fatal("all partitions must remain selected")
				}
			}
		})
	}
}

func TestQueryPlanMergesOptimizerAndPartitionSummary(t *testing.T) {
	patchQueryReadRuntime(t)
	global := optimizer.NewNoopGlobalOptimizer()
	for _, flags := range []uint32{
		1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering,
		1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering,
		1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering | 1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering,
	} {
		t.Run(strconv.FormatUint(uint64(flags), 10), func(t *testing.T) {
			mockey.PatchConvey("partition diagnostics augment existing optimizer facts", t, func() {
				view := &viewpb.QueryViewOfShard{
					Meta: &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}},
					QueryNode: []*viewpb.QueryViewOfQueryNode{
						{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}}},
						{NodeId: 12, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 20, SegmentIds: []int64{200}}}},
					},
				}
				lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() {}, Release: func() {}}
				mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
				mockey.Mock((*queryresource.QueryRuntime).ResolvePartitions).To(func(_ *queryresource.QueryRuntime, _ uint64, ids []int64) ([]int64, error) { return ids, nil }).Build()
				mockey.Mock(mockey.GetMethod(global, "OptimizePlan")).To(func(context.Context, *viewpb.QueryPlan, *viewpb.QueryViewOfShard) (*viewpb.GlobalOptimizerExecutionSummary, error) {
					return &viewpb.GlobalOptimizerExecutionSummary{Flags: flags}, nil
				}).Build()
				w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
				for _, search := range []bool{false, true} {
					req := &viewpb.GetQueryPlanRequest{ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}}
					if search {
						req.Request = &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{CollectionID: 1, PartitionIDs: []int64{10}}}
					} else {
						req.Request = &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1, PartitionIDs: []int64{10}}}
					}
					response, err := w.GetQueryPlan(t.Context(), req)
					require.NoError(t, err)
					require.Equal(t, flags|1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByPartitionFiltering, response.GetExecutionSummary().GetGlobalOptimizer().GetFlags())
					require.Len(t, response.Plan.WorkNodes, 1)
					require.EqualValues(t, 11, response.Plan.WorkNodes[0].GetQueryNode().GetNodeId())
					require.True(t, proto.Equal(req.GetQueryPlanMvcc(), response.Plan.Mvcc))
				}
			})
		})
	}
}

func TestQueryPlanAllSubqueriesSkipped(t *testing.T) {
	patchQueryReadRuntime(t)
	mockey.PatchConvey("a fully skipped optimization still returns the bound plan", t, func() {
		view := &viewpb.QueryViewOfShard{
			Meta:          &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "p_1v0", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}},
			StreamingNode: &viewpb.QueryViewOfStreamingNode{},
			QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}}}},
		}
		renewed, released := false, false
		lease := &snview.QueryViewLease{Meta: view.Meta, View: view, Version: qviews.FromProtoQueryViewVersion(view.Meta.Version), Renew: func() { require.False(t, released); renewed = true }, Release: func() { released = true }}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtSchema).Return(lease, nil).Build()
		mockey.Mock((*queryresource.QueryRuntime).ResolvePartitions).Return([]int64{10}, nil).Build()
		mockey.Mock(mockey.GetMethod(optimizer.NewNoopGlobalOptimizer(), "OptimizeSearch")).To(func(_ context.Context, req *internalpb.SearchRequest) (optimizer.SearchOptimization, error) {
			for _, sub := range req.SubReqs {
				sub.Skip = true
			}
			return optimizer.SearchOptimization{Skip: true}, nil
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}, viewResourceManager: &vchannel.PChannelRecoveryManager{}}
		req := &viewpb.GetQueryPlanRequest{
			ShardId: qviews.NewShardIDFromQVMeta(view.Meta).IntoProto(),
			Mvcc:    &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}},
			Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{CollectionID: 1, IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{Nq: 2, Topk: 3}, {Nq: 2, Topk: 4}}}},
		}
		before := proto.Clone(req)
		response, err := w.GetQueryPlan(t.Context(), req)
		require.NoError(t, err)
		require.True(t, renewed)
		require.True(t, released)
		require.Empty(t, response.Plan.WorkNodes)
		require.True(t, proto.Equal(req.GetQueryPlanMvcc(), response.Plan.Mvcc))
		require.True(t, proto.Equal(view.Meta.Version, response.Plan.Version))
		require.True(t, proto.Equal(req.ShardId, response.Plan.ShardId))
		require.Nil(t, response.ExecutionSummary, "skipping is not evidence of partition/statistics pruning")
		require.NoError(t, qviews.RestoreQueryPlanPayloads(response, req))
		subs := response.Plan.GetLegacySearchRequest().GetSubReqs()
		require.Len(t, subs, 2)
		for i, sub := range subs {
			require.True(t, sub.Skip)
			require.Equal(t, req.GetLegacySearchRequest().SubReqs[i].Topk, sub.Topk)
			require.Equal(t, []int64{10}, sub.PartitionIDs)
		}
		require.True(t, proto.Equal(before, req), "planning must not mutate the input")
	})
}

func TestQueryPlanSealedSegmentCountRespectsPartitionAndPruning(t *testing.T) {
	view := &viewpb.QueryViewOfShard{QueryNode: []*viewpb.QueryViewOfQueryNode{
		{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10, 11, 12}}, {PartitionId: 2, SegmentIds: []int64{20, 21}}}},
		{NodeId: 12, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: []int64{30, 31}}}},
	}}
	for _, tc := range []struct {
		name  string
		mode  viewpb.SegmentSelection_Mode
		ids   []int64
		count uint32
	}{
		{"all", viewpb.SegmentSelection_All, nil, 3},
		{"include", viewpb.SegmentSelection_Include, []int64{11}, 1},
		{"include_other_partition", viewpb.SegmentSelection_Include, []int64{11, 20}, 1},
		{"exclude", viewpb.SegmentSelection_Exclude, []int64{11}, 2},
		{"exclude_other_partition", viewpb.SegmentSelection_Exclude, []int64{20, 21}, 3},
		{"exclude_both_partitions", viewpb.SegmentSelection_Exclude, []int64{11, 20, 21}, 2},
		{"none", viewpb.SegmentSelection_None, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &viewpb.QueryPlan{Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{PartitionIDs: []int64{1}}}, WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}, SegmentSelection: &viewpb.SegmentSelection{Mode: tc.mode, SegmentIds: tc.ids}}}}
			require.Equal(t, tc.count, queryPlanSealedSegmentCount(plan, view))
		})
	}
}

func TestQueryPlanSealedSegmentCountWithFullNodeSelection(t *testing.T) {
	view := &viewpb.QueryViewOfShard{QueryNode: []*viewpb.QueryViewOfQueryNode{
		{NodeId: 11, Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 1, SegmentIds: []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}},
			{PartitionId: 2, SegmentIds: []int64{20, 21}},
		}},
	}}
	// OptimizePlan encodes worker selections against full node membership,
	// including excluded segments outside the search's partition scope.
	selection := qviews.SelectSegments(
		[]int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
		map[int64]struct{}{11: {}, 20: {}, 21: {}},
	)
	require.Equal(t, viewpb.SegmentSelection_Exclude, selection.GetMode())
	for _, tc := range []struct {
		name       string
		partitions []int64
		count      uint32
	}{
		{"requested_partition", []int64{1}, 9},
		{"all_partitions", nil, 9},
		{"excluded_partition", []int64{2}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &viewpb.QueryPlan{
				Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{PartitionIDs: tc.partitions}},
				WorkNodes: []*viewpb.QueryPlanWorkNode{{
					Node:             &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}},
					SegmentSelection: selection,
				}},
			}
			require.Equal(t, tc.count, queryPlanSealedSegmentCount(plan, view))
		})
	}
}
