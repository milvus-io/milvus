package datacoord

import (
	"context"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CollectionTopology captures memory constraints for a collection
type CollectionTopology struct {
	CollectionID     int64
	NumReplicas      int
	NumShards        int
	IsStandaloneMode bool
	IsPooling        bool

	QueryNodeMemory map[int64]uint64
	DataNodeMemory  map[int64]uint64
}

// CollectionTopologyQuerier queries collection topology including replicas and memory info
type CollectionTopologyQuerier interface {
	GetCollectionTopology(ctx context.Context, collectionID int64) (*CollectionTopology, error)
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

func (q *metricsNodeMemoryQuerier) GetCollectionTopology(ctx context.Context, collectionID int64) (*CollectionTopology, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	if q.mixCoord == nil {
		return nil, merr.WrapErrServiceInternalMsg("mixCoord not available for topology query")
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
		log.Warn(ctx, "failed to get QueryNode sessions", mlog.Err(err))
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

	log.Info(ctx, "excluding embedded QueryNode", mlog.Int64s("nodeIDs", lo.Keys(embeddedNodeIDs)))
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
			log.Warn(ctx, "DataNode returned 0 memory (pooling mode?), using default",
				mlog.FieldNodeID(nodeID),
				mlog.Uint64("defaultMemory", defaultPoolingDataNodeMemory))
			dataNodeMemory[nodeID] = defaultPoolingDataNodeMemory
		}
	}

	isStandaloneMode := paramtable.GetRole() == typeutil.StandaloneRole
	log.Info(ctx, "Collection topology",
		mlog.FieldCollectionID(collectionID),
		mlog.Int("numReplicas", numReplicas),
		mlog.Any("querynodes", queryNodeMemory),
		mlog.Any("datanodes", dataNodeMemory),
		mlog.Bool("isStandaloneMode", isStandaloneMode),
		mlog.Bool("isPooling", isPooling))

	return &CollectionTopology{
		CollectionID:     collectionID,
		NumReplicas:      numReplicas,
		QueryNodeMemory:  queryNodeMemory,
		DataNodeMemory:   dataNodeMemory,
		IsStandaloneMode: isStandaloneMode,
		IsPooling:        isPooling,
	}, nil
}

const (
	// Fallback memory for pooling DataNode (returns 0 from GetMetrics)
	defaultPoolingDataNodeMemory = 32 * 1024 * 1024 * 1024 // 32GB
)
