package streamingquery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The Coord scheduler is outside this extraction. Drive two concurrent Up views
// explicitly to verify SN membership independently from resource retention.
func verifyConcurrentSNViews(t *testing.T, ctx, rpcctx context.Context, proxy milvuspb.MilvusServiceClient, sn *grpc.ClientConn, etcd *clientv3.Client, namespace, name string, oldView *viewpb.QueryViewOfShard, oldPlan *viewpb.QueryPlan, searchReq *internalpb.SearchRequest) {
	t.Helper()
	flushed, err := proxy.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	require.NoError(t, err)
	require.NoError(t, merr.Error(flushed.GetStatus()))
	var newer *viewpb.DataVersion
	require.Eventually(t, func() bool {
		response, err := etcd.Get(ctx, namespace+"/meta/coord/dv/", clientv3.WithPrefix())
		require.NoError(t, err)
		for _, kv := range response.Kvs {
			var candidate viewpb.DataViewOfCollection
			require.NoError(t, proto.Unmarshal(kv.Value, &candidate))
			if candidate.GetCollectionId() == oldView.GetMeta().GetCollectionId() && candidate.GetDataVersion().GetStreamingVersion() > oldView.GetMeta().GetVersion().GetDataVersion().GetStreamingVersion() {
				newer = candidate.GetDataVersion()
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "flush must publish a newer sealed DataView")
	next := proto.Clone(oldView).(*viewpb.QueryViewOfShard)
	next.Meta.Version = &viewpb.QueryViewVersion{DataVersion: newer, QueryVersion: 1}
	stream, err := viewpb.NewViewSyncServiceClient(sn).SyncQueryView(rpcctx)
	require.NoError(t, err)
	defer stream.CloseSend()
	apply := func(state viewpb.QueryViewState) {
		next.Meta.State = state
		require.NoError(t, stream.Send(&viewpb.SyncRequest{Request: &viewpb.SyncRequest_Views{Views: &viewpb.SyncQueryViewsRequest{QueryViews: []*viewpb.QueryViewOfShard{next}}}}))
	}
	await := func(state viewpb.QueryViewState) {
		for {
			response, err := stream.Recv()
			require.NoError(t, err)
			for _, reported := range response.GetViews().GetQueryViews() {
				require.NotEqual(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, reported.GetMeta().GetState())
				if proto.Equal(reported.GetMeta().GetVersion(), next.Meta.Version) && reported.GetMeta().GetState() == state {
					return
				}
			}
		}
	}
	apply(viewpb.QueryViewState_QueryViewStatePreparing)
	await(viewpb.QueryViewState_QueryViewStateReady)
	apply(viewpb.QueryViewState_QueryViewStateUp)
	await(viewpb.QueryViewState_QueryViewStateUp)
	plans := viewpb.NewQueryPlanServiceClient(sn)
	queries := viewpb.NewViewQueryServiceClient(sn)
	response, err := plans.GetQueryPlan(rpcctx, &viewpb.GetQueryPlanRequest{CollectionId: next.Meta.CollectionId, ShardId: oldPlan.ShardId, Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: oldPlan.GetLegacyRetrieveRequest()}})
	require.NoError(t, err)
	newPlan := response.GetPlan()
	require.True(t, proto.Equal(next.Meta.Version, newPlan.GetVersion()))
	require.Empty(t, newPlan.GetWorkNodes(), "Ready must already fence sealed handoff; do not poll away an early Ready")
	require.True(t, qviews.FromProtoDataVersion(newer).GT(qviews.FromProtoDataVersion(oldPlan.GetVersion().GetDataVersion())))
	for i := 0; i < 3; i++ {
		// Use the same post-flush MVCC for both views: only DataVersion differs.
		for _, tc := range []struct {
			version *viewpb.QueryViewVersion
			rows    int
		}{
			{oldPlan.Version, 19}, {newPlan.Version, 0},
		} {
			response, err := queries.QueryOnView(rpcctx, &viewpb.QueryOnViewRequest{ShardId: oldPlan.ShardId, Version: tc.version, Mvcc: newPlan.Mvcc, LegacyReq: oldPlan.GetLegacyRetrieveRequest()})
			require.NoError(t, err)
			require.NoError(t, merr.Error(response.GetLegacyResults().GetStatus()))
			ids := response.GetLegacyResults().GetIds().GetIntId().GetData()
			require.Len(t, ids, tc.rows)
			require.NotContains(t, ids, int64(3))
		}
	}
	emptySearch, err := queries.SearchOnView(rpcctx, &viewpb.SearchOnViewRequest{ShardId: newPlan.ShardId, Version: newPlan.Version, Mvcc: newPlan.Mvcc, LegacyReq: searchReq})
	require.NoError(t, err)
	require.NoError(t, merr.Error(emptySearch.GetLegacyResults().GetStatus()))
	require.Empty(t, emptySearch.GetLegacyResults().GetSlicedBlob())
	t.Logf("concurrent Up views verified: old=%s returns 19 rows; new=%s returns 0 SN rows and empty search; Phase 1 prunes SN", oldPlan.Version, newPlan.Version)
	apply(viewpb.QueryViewState_QueryViewStateDown)
	await(viewpb.QueryViewState_QueryViewStateDown)
	apply(viewpb.QueryViewState_QueryViewStateDropped)
	await(viewpb.QueryViewState_QueryViewStateDropped)
	verifyCrossReplicaAdmission(t, rpcctx, sn, oldView, newer)
}

// Admission cannot regress even after the highest-version reference is dropped.
func verifyCrossReplicaAdmission(t *testing.T, ctx context.Context, sn *grpc.ClientConn, oldView *viewpb.QueryViewOfShard, newest *viewpb.DataVersion) {
	t.Helper()
	stream, err := viewpb.NewViewSyncServiceClient(sn).SyncQueryView(ctx)
	require.NoError(t, err)
	defer stream.CloseSend()
	for i, tc := range []struct {
		version  *viewpb.DataVersion
		expected viewpb.QueryViewState
	}{
		{oldView.GetMeta().GetVersion().GetDataVersion(), viewpb.QueryViewState_QueryViewStateUnrecoverable},
		{newest, viewpb.QueryViewState_QueryViewStateReady},
	} {
		view := proto.Clone(oldView).(*viewpb.QueryViewOfShard)
		view.Meta.ReplicaId += int64(i + 1)
		view.Meta.Version.DataVersion = proto.Clone(tc.version).(*viewpb.DataVersion)
		apply := func(state viewpb.QueryViewState) {
			view.Meta.State = state
			require.NoError(t, stream.Send(&viewpb.SyncRequest{Request: &viewpb.SyncRequest_Views{Views: &viewpb.SyncQueryViewsRequest{QueryViews: []*viewpb.QueryViewOfShard{view}}}}))
		}
		await := func(expected viewpb.QueryViewState) {
			for {
				response, err := stream.Recv()
				require.NoError(t, err)
				for _, report := range response.GetViews().GetQueryViews() {
					if report.GetMeta().GetReplicaId() != view.Meta.ReplicaId {
						continue
					}
					state := report.GetMeta().GetState()
					if state == expected {
						return
					}
					if expected != viewpb.QueryViewState_QueryViewStateDropped {
						require.NotEqual(t, viewpb.QueryViewState_QueryViewStateReady, state, "stale replica view was accepted")
						require.NotEqual(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, state, "equal-version replica view was rejected")
					}
				}
			}
		}
		apply(viewpb.QueryViewState_QueryViewStatePreparing)
		await(tc.expected)
		apply(viewpb.QueryViewState_QueryViewStateDropped)
		await(viewpb.QueryViewState_QueryViewStateDropped)
		t.Logf("cross-replica admission: replica=%d version=%s state=%s", view.Meta.ReplicaId, tc.version, tc.expected)
	}
}
