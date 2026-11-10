package cmek

import (
	"context"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

func (s *rawDataSuite) collectionQueryViews(ctx context.Context, collectionID int64) []*viewpb.QueryViewOfShard {
	var views []*viewpb.QueryViewOfShard
	for _, view := range readPropagationRecords(ctx, s, "coord/qv", func() *viewpb.QueryViewOfShard { return &viewpb.QueryViewOfShard{} }) {
		if view.GetMeta().GetCollectionId() == collectionID {
			views = append(views, view)
		}
	}
	return views
}

// Keep physical loading evidence separate from serving placement. QV physical
// segments have no legacy load-task version or delegator LeaderView.
func (s *rawDataSuite) parquetReadSnapshot(ctx context.Context, collectionID int64) v3ReadSnapshot {
	snapshot := v3ReadSnapshot{Loaded: make(map[int64][]v3LoadedIdentity), Serving: make(map[int64][]int64)}
	for _, node := range s.Cluster.GetAllQueryNodes() {
		response, err := node.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.Require().NoError(merr.CheckRPCCall(response, err))
		s.Require().False(response.GetIsDelta())
		for _, segment := range response.GetSegments() {
			if segment.GetCollection() != collectionID {
				continue
			}
			locator, err := inspector.ParseManifestLocatorV3(segment.GetManifestPath())
			s.Require().NoError(err)
			snapshot.Loaded[segment.GetID()] = append(snapshot.Loaded[segment.GetID()], v3LoadedIdentity{response.GetNodeID(), locator})
		}
	}
	latest := make(map[qviews.ShardID]*viewpb.QueryViewOfShard)
	for _, view := range s.collectionQueryViews(ctx, collectionID) {
		meta := view.GetMeta()
		if meta.GetState() != viewpb.QueryViewState_QueryViewStateUp {
			continue
		}
		id := qviews.ShardID{VChannel: meta.GetVchannel(), ReplicaID: meta.GetReplicaId()}
		previous := latest[id]
		if previous == nil || qviews.FromProtoQueryViewVersion(meta.GetVersion()).GT(qviews.FromProtoQueryViewVersion(previous.GetMeta().GetVersion())) {
			latest[id] = view
		}
	}
	assignments := make(map[string]*streamingpb.PChannelMeta)
	for _, assignment := range readPropagationRecords(ctx, s, "streamingcoord-meta/pchannel", func() *streamingpb.PChannelMeta { return &streamingpb.PChannelMeta{} }) {
		assignments[assignment.GetChannel().GetName()] = assignment
	}
	for id, view := range latest {
		assignment := assignments[funcutil.ToPhysicalChannel(id.VChannel)]
		if assignment.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED {
			snapshot.Unserviceable = true
			continue
		}
		conn, err := process.DailGRPClient(ctx, assignment.GetNode().GetAddress(), s.Cluster.RootPath(), assignment.GetNode().GetServerId())
		s.Require().NoError(err)
		rpcctx := handler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{
			Name: assignment.GetChannel().GetName(), Term: assignment.GetChannel().GetTerm(), AccessMode: types.AccessModeRW,
		})
		response, err := viewpb.NewQueryPlanServiceClient(conn).GetQueryPlan(rpcctx, &viewpb.GetQueryPlanRequest{
			CollectionId: collectionID, ShardId: id.IntoProto(),
			Mvcc:    &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
			Request: &viewpb.GetQueryPlanRequest_StatisticsRequest{StatisticsRequest: &viewpb.StatisticsRequest{}},
		})
		_ = conn.Close()
		if err != nil || !proto.Equal(response.GetPlan().GetVersion(), view.GetMeta().GetVersion()) {
			snapshot.Unserviceable = true
			continue
		}
		s.Require().Positive(response.GetPlan().GetVersion().GetQueryVersion())
		growing := response.GetGrowingInlineResponse().GetStatisticsResponse()
		s.Require().NotNil(growing, "statistics must observe the selected SN runtime")
		snapshot.Growing = snapshot.Growing || growing.GetRowCount() != 0
		routed := make(map[int64]bool)
		for _, worker := range response.GetPlan().GetWorkNodes() {
			s.Require().NotNil(worker.GetQueryNode(), "statistics routes sealed reads to QNs")
			routed[worker.GetQueryNode().GetNodeId()] = true
		}
		for _, node := range view.GetQueryNode() {
			for _, partition := range node.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					snapshot.Unserviceable = snapshot.Unserviceable || !routed[node.GetNodeId()]
					snapshot.Serving[segmentID] = append(snapshot.Serving[segmentID], node.GetNodeId())
				}
			}
		}
	}
	for _, nodes := range snapshot.Serving {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	}
	return snapshot
}
