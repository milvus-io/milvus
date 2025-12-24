package datacoord

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// Fallback memory for pooling DataNode (returns 0 from GetMetrics)
	defaultPoolingDataNodeMemory = 32 * 1024 * 1024 * 1024 // 32GB
)

// CollectionTopology captures memory constraints for a collection
type CollectionTopology struct {
	CollectionID     int64
	NumReplicas      int
	IsStandaloneMode bool
	IsPooling        bool

	QueryNodeMemory map[int64]uint64
	DataNodeMemory  map[int64]uint64
}

// CollectionTopologyQuerier queries collection topology including replicas and memory info
type CollectionTopologyQuerier interface {
	GetCollectionTopology(ctx context.Context, collectionID int64) (*CollectionTopology, error)
}

type forceMergeCompactionPolicy struct {
	meta            *meta
	allocator       allocator.Allocator
	handler         Handler
	topologyQuerier CollectionTopologyQuerier
}

func newForceMergeCompactionPolicy(meta *meta, allocator allocator.Allocator, handler Handler) *forceMergeCompactionPolicy {
	return &forceMergeCompactionPolicy{
		meta:            meta,
		allocator:       allocator,
		handler:         handler,
		topologyQuerier: nil,
	}
}

func (policy *forceMergeCompactionPolicy) SetTopologyQuerier(querier CollectionTopologyQuerier) {
	policy.topologyQuerier = querier
}

func (policy *forceMergeCompactionPolicy) triggerOneCollection(
	ctx context.Context,
	collectionID int64,
	targetSize int64,
) ([]CompactionView, int64, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("targetSize", targetSize))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}
	triggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		return nil, 0, err
	}

	collectionTTL, err := getCollectionTTL(collection.Properties)
	if err != nil {
		log.Warn("failed to get collection ttl, use default", zap.Error(err))
		collectionTTL = 0
	}

	configMaxSize := getExpectedSegmentSize(policy.meta, collectionID, collection.Schema)

	segments := policy.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting &&
			!segment.GetIsImporting() &&
			segment.GetLevel() != datapb.SegmentLevel_L0
	}))

	if len(segments) == 0 {
		log.Info("no eligible segments for force merge")
		return nil, 0, nil
	}

	topology, err := policy.topologyQuerier.GetCollectionTopology(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}

	views := []CompactionView{}
	for label, groups := range groupByPartitionChannel(GetViewsByInfo(segments...)) {
		view := &ForceMergeSegmentView{
			label:         label,
			segments:      groups,
			triggerID:     triggerID,
			collectionTTL: collectionTTL,

			configMaxSize: float64(configMaxSize),
			topology:      topology,
		}
		views = append(views, view)
	}
	return views, triggerID, nil

	log.Info("force merge triggered", zap.Int("viewCount", len(views)))
	return views, triggerID, nil
}

func groupByPartitionChannel(segments []*SegmentView) map[*CompactionGroupLabel][]*SegmentView {
	result := make(map[*CompactionGroupLabel][]*SegmentView)

	for _, seg := range segments {
		label := seg.label
		key := label.Key()

		var foundLabel *CompactionGroupLabel
		for l := range result {
			if l.Key() == key {
				foundLabel = l
				break
			}
		}
		if foundLabel == nil {
			foundLabel = label
		}

		result[foundLabel] = append(result[foundLabel], seg)
	}

	return result
}

type metricsNodeMemoryQuerier struct {
	nodeManager session.NodeManager
	mixCoord    types.MixCoord
	session     sessionutil.SessionInterface
}

func newMetricsNodeMemoryQuerier(nodeManager session.NodeManager, mixCoord types.MixCoord, session sessionutil.SessionInterface) *metricsNodeMemoryQuerier {
	return &metricsNodeMemoryQuerier{
		nodeManager: nodeManager,
		mixCoord:    mixCoord,
		session:     session,
	}
}

var _ CollectionTopologyQuerier = (*metricsNodeMemoryQuerier)(nil)

func (q *metricsNodeMemoryQuerier) GetCollectionTopology(ctx context.Context, collectionID int64) (*CollectionTopology, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	if q.mixCoord == nil {
		return nil, fmt.Errorf("mixCoord not available for topology query")
	}

	// 1. Get replica information
	replicasResp, err := q.mixCoord.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		CollectionID: collectionID,
	})
	if err != nil {
		return nil, err
	}
	numReplicas := len(replicasResp.GetReplicas())

	// 2. Get QueryNode metrics for memory info
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, err
	}

	// Get QueryNode sessions from etcd to filter out embedded nodes
	sessions, _, err := q.session.GetSessions(ctx, typeutil.QueryNodeRole)
	if err != nil {
		log.Warn("failed to get QueryNode sessions", zap.Error(err))
		return nil, err
	}

	// Build set of embedded QueryNode IDs to exclude
	embeddedNodeIDs := make(map[int64]struct{})
	for _, sess := range sessions {
		// Check if this is an embedded QueryNode in streaming node
		if labels := sess.ServerLabels; labels != nil {
			if labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] == "1" {
				embeddedNodeIDs[sess.ServerID] = struct{}{}
			}
		}
	}

	log.Info("excluding embedded QueryNode", zap.Int64s("nodeIDs", lo.Keys(embeddedNodeIDs)))
	rsp, err := q.mixCoord.GetQcMetrics(ctx, req)
	if err = merr.CheckRPCCall(rsp, err); err != nil {
		return nil, err
	}
	topology := &metricsinfo.QueryCoordTopology{}
	if err := metricsinfo.UnmarshalTopology(rsp.GetResponse(), topology); err != nil {
		return nil, err
	}

	// Build QueryNode memory map: nodeID → memory size (exclude embedded nodes)
	queryNodeMemory := make(map[int64]uint64)
	for _, node := range topology.Cluster.ConnectedNodes {
		if _, ok := embeddedNodeIDs[node.ID]; ok {
			continue
		}
		queryNodeMemory[node.ID] = node.HardwareInfos.Memory
	}

	// 3. Get DataNode memory info
	dataNodeMemory := make(map[int64]uint64)
	isPooling := false
	nodes := q.nodeManager.GetClientIDs()
	for _, nodeID := range nodes {
		cli, err := q.nodeManager.GetClient(nodeID)
		if err != nil {
			continue
		}

		resp, err := cli.GetMetrics(ctx, req)
		if err != nil {
			continue
		}

		var infos metricsinfo.DataNodeInfos
		if err := metricsinfo.UnmarshalComponentInfos(resp.GetResponse(), &infos); err != nil {
			continue
		}

		if infos.HardwareInfos.Memory > 0 {
			dataNodeMemory[nodeID] = infos.HardwareInfos.Memory
		} else {
			// Pooling DataNode returns 0 from GetMetrics
			// Use default fallback: 32GB
			isPooling = true
			log.Warn("DataNode returned 0 memory (pooling mode?), using default",
				zap.Int64("nodeID", nodeID),
				zap.Uint64("defaultMemory", defaultPoolingDataNodeMemory))
			dataNodeMemory[nodeID] = defaultPoolingDataNodeMemory
		}
	}

	isStandaloneMode := paramtable.GetRole() == typeutil.StandaloneRole
	log.Info("Collection topology",
		zap.Int64("collectionID", collectionID),
		zap.Int("numReplicas", numReplicas),
		zap.Any("querynodes", queryNodeMemory),
		zap.Any("datanodes", dataNodeMemory),
		zap.Bool("isStandaloneMode", isStandaloneMode),
		zap.Bool("isPooling", isPooling))

	return &CollectionTopology{
		CollectionID:     collectionID,
		NumReplicas:      numReplicas,
		QueryNodeMemory:  queryNodeMemory,
		DataNodeMemory:   dataNodeMemory,
		IsStandaloneMode: isStandaloneMode,
		IsPooling:        isPooling,
	}, nil
}
