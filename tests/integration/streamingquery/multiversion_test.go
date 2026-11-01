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

// Drive SN resources directly; Coord's planning order is tested separately.
// The returned check must run after the caller drops oldView. It retains the
// newer view and another old-base reference until then.
func verifyConcurrentSNViews(t *testing.T, ctx, rpcctx context.Context, proxy milvuspb.MilvusServiceClient, sn *grpc.ClientConn, etcd *clientv3.Client, namespace, name string, oldView *viewpb.QueryViewOfShard, oldPlan *viewpb.QueryPlan, searchReq *internalpb.SearchRequest) func() {
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
	// Separate replicas keep the caller's old view available for its remaining
	// query/lease checks while the newer view stays Up and pins this runtime.
	next.Meta.ReplicaId++
	next.Meta.Version = &viewpb.QueryViewVersion{DataVersion: newer, QueryVersion: 1}
	stream, err := viewpb.NewViewSyncServiceClient(sn).SyncQueryView(rpcctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
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
				if reported.GetMeta().GetReplicaId() == next.Meta.ReplicaId && proto.Equal(reported.GetMeta().GetVersion(), next.Meta.Version) && reported.GetMeta().GetState() == state {
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
	nextShard := &viewpb.ShardID{ReplicaId: next.Meta.ReplicaId, Vchannel: next.Meta.Vchannel}
	response := getQueryPlanWithPayloads(t, rpcctx, plans, &viewpb.GetQueryPlanRequest{CollectionId: next.Meta.CollectionId, ShardId: nextShard, Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: oldPlan.GetLegacyRetrieveRequest()}})
	newPlan := response.GetPlan()
	require.True(t, proto.Equal(next.Meta.Version, newPlan.GetVersion()))
	require.Empty(t, newPlan.GetWorkNodes(), "Ready must already fence sealed handoff; do not poll away an early Ready")
	require.True(t, qviews.FromProtoDataVersion(newer).GT(qviews.FromProtoDataVersion(oldPlan.GetVersion().GetDataVersion())))
	for i := 0; i < 3; i++ {
		// Use the same post-flush MVCC for both views: only DataVersion differs.
		for _, tc := range []struct {
			shard   *viewpb.ShardID
			version *viewpb.QueryViewVersion
			rows    int
		}{
			{oldPlan.ShardId, oldPlan.Version, 19}, {newPlan.ShardId, newPlan.Version, 0},
		} {
			response, err := queries.QueryOnView(rpcctx, &viewpb.QueryOnViewRequest{ShardId: tc.shard, Version: tc.version, Mvcc: newPlan.Mvcc, LegacyReq: oldPlan.GetLegacyRetrieveRequest()})
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
	finishRetention := verifyCrossReplicaRetention(t, rpcctx, sn, oldView, newPlan, oldPlan.GetLegacyRetrieveRequest())
	return func() {
		// The loaded fixture also has Coord-owned views. Wait for their old
		// references to finish teardown, not just for a newer Up to appear.
		// Durable removal follows SN Dropped, which follows resource release.
		require.Eventually(t, func() bool {
			response, err := etcd.Get(ctx, namespace+"/meta/coord/qv/", clientv3.WithPrefix())
			require.NoError(t, err)
			hasNewUp := false
			for _, kv := range response.Kvs {
				var view viewpb.QueryViewOfShard
				require.NoError(t, proto.Unmarshal(kv.Value, &view))
				meta := view.GetMeta()
				if meta.GetCollectionId() != oldView.Meta.CollectionId || meta.GetVchannel() != oldView.Meta.Vchannel {
					continue
				}
				if qviews.FromProtoDataVersion(newer).GT(qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())) {
					return false
				}
				hasNewUp = hasNewUp || meta.GetState() == viewpb.QueryViewState_QueryViewStateUp
			}
			return hasNewUp
		}, time.Minute, 100*time.Millisecond, "fixture handoff must release all Coord-owned old bases")
		finishRetention()
		apply(viewpb.QueryViewState_QueryViewStateDown)
		await(viewpb.QueryViewState_QueryViewStateDown)
		apply(viewpb.QueryViewState_QueryViewStateDropped)
		await(viewpb.QueryViewState_QueryViewStateDropped)
	}
}

// Existing references protect their exact base across replicas. Once the last
// old reference is released, a new acquisition cannot rewind reclamation.
func verifyCrossReplicaRetention(t *testing.T, ctx context.Context, sn *grpc.ClientConn, oldView *viewpb.QueryViewOfShard, newerPlan *viewpb.QueryPlan, retrieve *internalpb.RetrieveRequest) func() {
	t.Helper()
	stream, err := viewpb.NewViewSyncServiceClient(sn).SyncQueryView(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	newReplica := func(offset int64, version *viewpb.DataVersion) *viewpb.QueryViewOfShard {
		view := proto.Clone(oldView).(*viewpb.QueryViewOfShard)
		view.Meta.ReplicaId += offset
		view.Meta.Version.DataVersion = proto.Clone(version).(*viewpb.DataVersion)
		return view
	}
	transition := func(view *viewpb.QueryViewOfShard, state, expected viewpb.QueryViewState) {
		view.Meta.State = state
		require.NoError(t, stream.Send(&viewpb.SyncRequest{Request: &viewpb.SyncRequest_Views{Views: &viewpb.SyncQueryViewsRequest{QueryViews: []*viewpb.QueryViewOfShard{view}}}}))
		for {
			response, err := stream.Recv()
			require.NoError(t, err)
			for _, report := range response.GetViews().GetQueryViews() {
				if report.GetMeta().GetReplicaId() != view.Meta.ReplicaId || !proto.Equal(report.GetMeta().GetVersion(), view.Meta.Version) {
					continue
				}
				state := report.GetMeta().GetState()
				if state == expected {
					return
				}
				if expected == viewpb.QueryViewState_QueryViewStateUnrecoverable {
					require.NotEqual(t, viewpb.QueryViewState_QueryViewStateReady, state, "unretained old base was accepted")
				} else if expected != viewpb.QueryViewState_QueryViewStateDropped {
					require.NotEqual(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, state, "retained base was rejected")
				}
			}
		}
	}
	oldVersion := oldView.Meta.Version.DataVersion
	retained := newReplica(2, oldVersion)
	transition(retained, viewpb.QueryViewState_QueryViewStatePreparing, viewpb.QueryViewState_QueryViewStateReady)
	transition(retained, viewpb.QueryViewState_QueryViewStateUp, viewpb.QueryViewState_QueryViewStateUp)
	checkRetainedRows := func() {
		response, err := viewpb.NewViewQueryServiceClient(sn).QueryOnView(ctx, &viewpb.QueryOnViewRequest{
			ShardId: &viewpb.ShardID{ReplicaId: retained.Meta.ReplicaId, Vchannel: retained.Meta.Vchannel},
			Version: retained.Meta.Version, Mvcc: newerPlan.Mvcc, LegacyReq: retrieve,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(response.GetLegacyResults().GetStatus()))
		expected := make([]int64, 0, 19)
		for id := int64(0); id < 20; id++ {
			if id != 3 {
				expected = append(expected, id)
			}
		}
		require.ElementsMatch(t, expected, response.GetLegacyResults().GetIds().GetIntId().GetData())
	}
	checkRetainedRows()
	t.Log("cross-replica old-base reuse is Ready and returns all 19 expected rows")
	return func() {
		checkRetainedRows()
		t.Log("remaining old-base reference still returns 19 rows after original view and fixture old views are dropped")
		transition(retained, viewpb.QueryViewState_QueryViewStateDown, viewpb.QueryViewState_QueryViewStateDown)
		// OnDropped follows reference removal, Advance and BeforeRelease. This
		// acknowledgement is the release barrier; no fixed sleep or retry of
		// the stale acquisition may hide an early Dropped report.
		transition(retained, viewpb.QueryViewState_QueryViewStateDropped, viewpb.QueryViewState_QueryViewStateDropped)
		stale := newReplica(3, oldVersion)
		transition(stale, viewpb.QueryViewState_QueryViewStatePreparing, viewpb.QueryViewState_QueryViewStateUnrecoverable)
		transition(stale, viewpb.QueryViewState_QueryViewStateDropped, viewpb.QueryViewState_QueryViewStateDropped)
		current := newReplica(4, newerPlan.Version.DataVersion)
		transition(current, viewpb.QueryViewState_QueryViewStatePreparing, viewpb.QueryViewState_QueryViewStateReady)
		transition(current, viewpb.QueryViewState_QueryViewStateDropped, viewpb.QueryViewState_QueryViewStateDropped)
		t.Log("last old-base reference dropped: old version rejected, current version Ready while newer view stays Up")
	}
}
