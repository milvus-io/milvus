package cmek

import (
	"context"
	"encoding/json"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
)

func (s *rawDataSuite) collectionQueryViews(ctx context.Context, collectionID int64) []*viewpb.QueryViewOfShard {
	return s.GetQueryViewDistribution(ctx, collectionID).Views
}

// Keep physical QN metrics separate from durable QV requirements and live SN serving.
func (s *rawDataSuite) parquetReadSnapshot(ctx context.Context, collectionID int64) v3ReadSnapshot {
	snapshot := v3ReadSnapshot{Loaded: make(map[int64][]v3LoadedIdentity), Serving: make(map[int64][]v3ServingIdentity)}
	request, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{
		metricsinfo.MetricTypeKey: metricsinfo.SegmentKey, metricsinfo.MetricRequestParamCollectionIDKey: collectionID,
	})
	s.Require().NoError(err)
	for _, node := range s.Cluster.GetAllQueryNodes() {
		response, err := node.MustGetClient(ctx).GetMetrics(ctx, request)
		s.Require().NoError(merr.CheckRPCCall(response, err))
		var segments []*metricsinfo.Segment
		s.Require().NoError(json.Unmarshal([]byte(response.GetResponse()), &segments))
		for _, segment := range segments {
			if segment.CollectionID == collectionID && segment.State == "Sealed" {
				snapshot.Loaded[segment.SegmentID] = append(snapshot.Loaded[segment.SegmentID], v3LoadedIdentity{NodeID: segment.NodeID})
			}
		}
	}
	distribution := s.GetQueryViewDistribution(ctx, collectionID)
	assignments := make(map[string]*streamingpb.PChannelMeta)
	for _, assignment := range readPropagationRecords(ctx, s, "streamingcoord-meta/pchannel", func() *streamingpb.PChannelMeta { return &streamingpb.PChannelMeta{} }) {
		assignments[assignment.GetChannel().GetName()] = assignment
	}
	for _, view := range distribution.UpViews {
		id := qviews.ShardID{VChannel: view.GetMeta().GetVchannel(), ReplicaID: view.GetMeta().GetReplicaId()}
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
				for i, segmentID := range partition.GetSegmentIds() {
					snapshot.Unserviceable = snapshot.Unserviceable || !routed[node.GetNodeId()]
					s.Require().Len(partition.GetSegmentManifestVersions(), len(partition.GetSegmentIds()), "V3 views require explicit manifest versions")
					snapshot.Serving[segmentID] = append(snapshot.Serving[segmentID], v3ServingIdentity{NodeID: node.GetNodeId(), MinimumManifestVersion: partition.GetSegmentManifestVersions()[i]})
				}
			}
		}
	}
	for _, nodes := range snapshot.Serving {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	}
	return snapshot
}
