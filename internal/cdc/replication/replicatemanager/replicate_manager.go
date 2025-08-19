package replicatemanager

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/cdc/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context

	// clusterReplicators is a map of target clusterID to ClusterReplicator.
	clusterReplicators *typeutil.ConcurrentMap[string, ClusterReplicator]
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:                context.Background(),
		clusterReplicators: typeutil.NewConcurrentMap[string, ClusterReplicator](),
	}
}

func (r *replicateManager) BroadcastReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error {
	updateConfigFn := func(topology *milvuspb.CrossClusterTopology) error {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		logger := log.Ctx(r.ctx).With(
			zap.String("target_cluster", targetCluster.GetClusterID()),
			zap.String("URI", targetCluster.GetConnectionParam().GetUri()),
		)
		targetClient, err := resource.Resource().ClusterClient().CreateMilvusClient(r.ctx, targetCluster)
		if err != nil {
			logger.Warn("failed to create milvus client", zap.Error(err))
			return err
		}
		defer targetClient.Close(r.ctx)
		err = targetClient.UpdateReplicateConfiguration(r.ctx, &milvuspb.UpdateReplicateConfigurationRequest{
			ReplicateConfiguration: config,
		})
		if err != nil {
			logger.Warn("failed to update replicate configuration", zap.Error(err))
			return err
		}
		return nil
	}
	for _, topology := range config.GetCrossClusterTopology() {
		err := updateConfigFn(topology)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replicateManager) StartReplications(config *milvuspb.ReplicateConfiguration) {
	for _, topology := range config.GetCrossClusterTopology() {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		clusterReplicator := NewClusterReplicator(targetCluster)
		clusterReplicator.StartReplicateCluster()
		r.clusterReplicators.Insert(targetCluster.GetClusterID(), clusterReplicator)
	}
}

func (r *replicateManager) StopReplications(config *milvuspb.ReplicateConfiguration) {
	for _, topology := range config.GetCrossClusterTopology() {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		replicator, ok := r.clusterReplicators.Get(targetCluster.GetClusterID())
		if !ok {
			continue
		}
		replicator.StopReplicateCluster()
	}
}

func (r *replicateManager) Close() {
	r.clusterReplicators.Range(func(key string, value ClusterReplicator) bool {
		value.StopReplicateCluster()
		return true
	})
}
