package streamingquery

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestStreamingQueryRPC exercises the extracted server independently of the
// future Coord balancer and Proxy query client. The explicit endpoint and etcd
// namespace must belong to an isolated cluster created by the cluster skill.
// The fixture has one shard and id=0..19. Its vector is [id,0,0,0], or
// SN_QUERY_TEST_BM25=1 selects a BM25 output for text "shared special" (id=3)
// and "shared regular" (others). KEEP_UP=1 retains the view for a restart;
// RECOVER=1 resumes that view and expects id=3 to have been deleted.
func TestStreamingQueryRPC(t *testing.T) {
	endpoint := os.Getenv("SN_QUERY_TEST_PROXY")
	if endpoint == "" {
		t.Skip("requires an explicitly managed local cluster")
	}
	namespace := os.Getenv("SN_QUERY_TEST_ETCD_ROOT")
	require.NotEmpty(t, namespace)
	name := os.Getenv("SN_QUERY_TEST_COLLECTION")
	require.NotEmpty(t, name)
	recovering := os.Getenv("SN_QUERY_TEST_RECOVER") == "1"
	bm25 := os.Getenv("SN_QUERY_TEST_BM25") == "1"
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dial := func(address string) *grpc.ClientConn {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	proxy := milvuspb.NewMilvusServiceClient(dial(endpoint))
	desc, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	require.NoError(t, err)
	require.NoError(t, merr.Error(desc.GetStatus()))
	require.Len(t, desc.GetVirtualChannelNames(), 1)
	vc := desc.GetVirtualChannelNames()[0]
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{os.Getenv("SN_QUERY_TEST_ETCD")}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer etcd.Close()
	var assignment *streamingpb.PChannelMeta
	var sn *grpc.ClientConn
	var rpcctx context.Context
	// Process health can precede WAL reassignment after an SN restart. Wait for
	// the assigned term and its MVCC service before driving the view protocol.
	require.Eventually(t, func() bool {
		kvs, err := etcd.Get(ctx, namespace+"/meta/streamingcoord-meta/pchannel/", clientv3.WithPrefix())
		require.NoError(t, err)
		for _, kv := range kvs.Kvs {
			var candidate streamingpb.PChannelMeta
			require.NoError(t, proto.Unmarshal(kv.Value, &candidate))
			if candidate.GetChannel().GetName() != funcutil.ToPhysicalChannel(vc) || candidate.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED {
				continue
			}
			if assignment == nil || assignment.GetNode().GetAddress() != candidate.GetNode().GetAddress() {
				sn = dial(candidate.GetNode().GetAddress())
			}
			assignment = &candidate
			rpcctx = handler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{Name: candidate.GetChannel().GetName(), Term: candidate.GetChannel().GetTerm(), AccessMode: types.AccessModeRW})
			probe, cancel := context.WithTimeout(rpcctx, time.Second)
			defer cancel()
			_, err = viewpb.NewQueryPlanServiceClient(sn).GetMVCCTimestamp(probe, &viewpb.GetMVCCTimestampRequest{Vchannel: vc})
			return err == nil
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "assigned SN WAL must be query-ready")
	kvs, err := etcd.Get(ctx, namespace+"/meta/coord/dv/", clientv3.WithPrefix())
	require.NoError(t, err)
	var dv *viewpb.DataViewOfCollection
	for _, kv := range kvs.Kvs {
		var v viewpb.DataViewOfCollection
		require.NoError(t, proto.Unmarshal(kv.Value, &v))
		if v.GetCollectionId() == desc.GetCollectionID() && (dv == nil || qviews.FromProtoDataVersion(v.GetDataVersion()).GT(qviews.FromProtoDataVersion(dv.GetDataVersion()))) {
			dv = &v
		}
	}
	require.NotNil(t, dv)
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{CollectionId: desc.GetCollectionID(), ReplicaId: 40451, Vchannel: vc, Version: &viewpb.QueryViewVersion{DataVersion: dv.GetDataVersion(), QueryVersion: 1}, State: viewpb.QueryViewState_QueryViewStatePreparing, LoadInfoVersion: 1}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
	if recovering {
		saved, err := etcd.Get(ctx, namespace+"/meta/streamingnode-meta/wal/"+assignment.GetChannel().GetName()+"/qv/", clientv3.WithPrefix())
		require.NoError(t, err)
		found := false
		for _, kv := range saved.Kvs {
			var recovered viewpb.QueryViewOfShard
			require.NoError(t, proto.Unmarshal(kv.Value, &recovered))
			if recovered.GetMeta().GetVchannel() == vc && recovered.GetMeta().GetReplicaId() == 40451 {
				view = &recovered
				found = true
				break
			}
		}
		require.True(t, found, "SN must preserve the Up recovery record across restart")
	}
	syncer, err := viewpb.NewViewSyncServiceClient(sn).SyncQueryView(rpcctx)
	require.NoError(t, err)
	apply := func(state viewpb.QueryViewState) {
		view.Meta.State = state
		require.NoError(t, syncer.Send(&viewpb.SyncRequest{Request: &viewpb.SyncRequest_Views{Views: &viewpb.SyncQueryViewsRequest{QueryViews: []*viewpb.QueryViewOfShard{view}}}}))
	}
	await := func(state viewpb.QueryViewState) {
		for {
			response, err := syncer.Recv()
			require.NoError(t, err)
			for _, reported := range response.GetViews().GetQueryViews() {
				t.Logf("SN view report: %s", reported.GetMeta().GetState())
				require.NotEqual(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, reported.GetMeta().GetState())
				if reported.GetMeta().GetState() == state {
					return
				}
			}
		}
	}
	apply(viewpb.QueryViewState_QueryViewStatePreparing)
	if recovering {
		await(viewpb.QueryViewState_QueryViewStateUp)
	} else {
		await(viewpb.QueryViewState_QueryViewStateReady)
		apply(viewpb.QueryViewState_QueryViewStateUp)
		await(viewpb.QueryViewState_QueryViewStateUp)
	}
	schema, err := typeutil.CreateSchemaHelper(desc.GetSchema())
	require.NoError(t, err)
	pk, err := schema.GetFieldFromName("id")
	require.NoError(t, err)
	retrievePlan, err := planparserv2.CreateRetrievePlan(schema, "id >= 0", nil)
	require.NoError(t, err)
	retrievePlan.OutputFieldIds = []int64{pk.GetFieldID()}
	retrieveBytes, err := proto.Marshal(retrievePlan)
	require.NoError(t, err)
	plans := viewpb.NewQueryPlanServiceClient(sn)
	queries := viewpb.NewViewQueryServiceClient(sn)
	getPlan := func() *viewpb.QueryPlan {
		response, err := plans.GetQueryPlan(rpcctx, &viewpb.GetQueryPlanRequest{CollectionId: desc.GetCollectionID(), ShardId: &viewpb.ShardID{ReplicaId: 40451, Vchannel: vc}, Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: desc.GetCollectionID(), SerializedExprPlan: retrieveBytes, OutputFieldsId: []int64{pk.GetFieldID()}, Limit: 100}}})
		require.NoError(t, err)
		require.NotEmpty(t, response.GetPlan().GetWorkNodes())
		return response.GetPlan()
	}
	plan := getPlan()
	query := func(plan *viewpb.QueryPlan) []int64 {
		response, err := queries.QueryOnView(rpcctx, &viewpb.QueryOnViewRequest{LegacyReq: plan.GetLegacyRetrieveRequest(), ShardId: plan.GetShardId(), Version: plan.GetVersion(), Mvcc: plan.GetMvcc()})
		require.NoError(t, err)
		require.NoError(t, merr.Error(response.GetLegacyResults().GetStatus()))
		return response.GetLegacyResults().GetIds().GetIntId().GetData()
	}
	ids := query(plan)
	if recovering {
		require.Len(t, ids, 19)
		require.NotContains(t, ids, int64(3))
	} else {
		require.Len(t, ids, 20)
		for i := int64(0); i < 20; i++ {
			require.Contains(t, ids, i)
		}
	}
	t.Logf("Phase 1 + Phase 2 returned %d expected rows (recovery=%v)", len(ids), recovering)
	metric := "L2"
	if bm25 {
		metric = "BM25"
	}
	searchPlan, err := planparserv2.CreateSearchPlan(schema, "id >= 0", "vector", &planpb.QueryInfo{Topk: 3, MetricType: metric, SearchParams: "{}", RoundDecimal: -1}, nil, nil)
	require.NoError(t, err)
	searchBytes, err := proto.Marshal(searchPlan)
	require.NoError(t, err)
	vector := make([]byte, 16)
	binary.LittleEndian.PutUint32(vector, math.Float32bits(3))
	placeholderType := commonpb.PlaceholderType_FloatVector
	if bm25 {
		vector = []byte("special")
		if recovering {
			vector = []byte("regular")
		}
		placeholderType = commonpb.PlaceholderType_VarChar
	}
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: placeholderType, Values: [][]byte{vector}}}})
	require.NoError(t, err)
	vectorField, err := schema.GetFieldFromName("vector")
	require.NoError(t, err)
	searchReq := &internalpb.SearchRequest{CollectionID: desc.GetCollectionID(), FieldId: vectorField.GetFieldID(), SerializedExprPlan: searchBytes, PlaceholderGroup: placeholder, Nq: 1, Topk: 3, MetricType: metric}
	optimized, err := plans.GetQueryPlan(rpcctx, &viewpb.GetQueryPlanRequest{CollectionId: desc.GetCollectionID(), ShardId: plan.GetShardId(), Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: searchReq}})
	require.NoError(t, err)
	searchPlanRPC := optimized.GetPlan()
	require.NotEmpty(t, searchPlanRPC.GetWorkNodes(), "a nonempty BM25 corpus must not be optimized away after recovery")
	search, err := queries.SearchOnView(rpcctx, &viewpb.SearchOnViewRequest{ShardId: searchPlanRPC.GetShardId(), Version: searchPlanRPC.GetVersion(), Mvcc: searchPlanRPC.GetMvcc(), LegacyReq: searchPlanRPC.GetLegacySearchRequest()})
	require.NoError(t, err, "SearchOnView details: %v", status.Convert(err).Details())
	require.NoError(t, merr.Error(search.GetLegacyResults().GetStatus()))
	var searchData schemapb.SearchResultData
	require.NoError(t, proto.Unmarshal(search.GetLegacyResults().GetSlicedBlob(), &searchData))
	if bm25 && !recovering {
		require.Len(t, searchData.GetIds().GetIntId().GetData(), 1)
	} else {
		require.Len(t, searchData.GetIds().GetIntId().GetData(), 3)
	}
	if !recovering {
		require.Equal(t, int64(3), searchData.GetIds().GetIntId().GetData()[0])
	} else {
		require.NotContains(t, searchData.GetIds().GetIntId().GetData(), int64(3))
	}
	t.Logf("SearchOnView returned expected neighbors: %v", searchData.GetIds().GetIntId().GetData())
	// A delete after the runtime snapshot must flow through the same live scanner.
	if !recovering {
		deleted, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: name, Expr: "id in [3]"})
		require.NoError(t, err)
		require.NoError(t, merr.Error(deleted.GetStatus()))
		ids = query(getPlan())
		require.Len(t, ids, 19)
		require.NotContains(t, ids, int64(3))
		t.Log("live delete is visible through the SN query MVCC")
	}
	if os.Getenv("SN_QUERY_TEST_KEEP_UP") == "1" {
		require.NoError(t, syncer.CloseSend())
		t.Log("persisted Up view retained for managed SN restart")
		return
	}
	apply(viewpb.QueryViewState_QueryViewStateDown)
	await(viewpb.QueryViewState_QueryViewStateDown)
	_, err = queries.QueryOnView(rpcctx, &viewpb.QueryOnViewRequest{LegacyReq: plan.GetLegacyRetrieveRequest(), ShardId: plan.GetShardId(), Version: plan.GetVersion(), Mvcc: plan.GetMvcc()})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	t.Logf("Down rejects an old Phase 2 plan: %v", err)
	apply(viewpb.QueryViewState_QueryViewStateDropped)
	await(viewpb.QueryViewState_QueryViewStateDropped)
	require.NoError(t, syncer.CloseSend())
}
