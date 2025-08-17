package replicatemanager

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/replicator"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/cdc/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

var _ ReplicateManagerClient = (*replicateManager)(nil)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context

	// replicators is a map of target clusterID to replicator.
	replicators *typeutil.ConcurrentMap[string, replicator.Replicator]
}

func NewReplicateManager() ReplicateManagerClient {
	return &replicateManager{
		ctx:         context.Background(),
		replicators: typeutil.NewConcurrentMap[string, replicator.Replicator](),
	}
}

func (r *replicateManager) BroadcastReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error {
	for _, topology := range config.GetCrossClusterTopology() {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		targetClient, err := resource.Resource().ClusterClient().CreateMilvusClient(targetCluster)
		logger := log.Ctx(r.ctx).With(
			zap.String("target_cluster", targetCluster.GetClusterID()),
			zap.String("URI", targetCluster.GetConnectionParam().GetUri()),
		)
		if err != nil {
			logger.Error("failed to create milvus client", zap.Error(err))
			return err
		}
		err = targetClient.UpdateReplicateConfiguration(r.ctx, &milvuspb.UpdateReplicateConfigurationRequest{
			ReplicateConfiguration: config,
		})
		if err != nil {
			logger.Error("failed to update replicate configuration", zap.Error(err))
			return err
		}
	}
	return nil
}

func (r *replicateManager) StartReplications(config *milvuspb.ReplicateConfiguration) error {
	for _, topology := range config.GetCrossClusterTopology() {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		replicator, ok := r.replicators.GetOrInsert(targetCluster.GetClusterID(), replicator.NewReplicator(targetCluster))
		if !ok {
			// New replicator created, start replication.
			err := replicator.StartReplication()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *replicateManager) StopReplications(config *milvuspb.ReplicateConfiguration) error {
	for _, topology := range config.GetCrossClusterTopology() {
		targetCluster := util.GetMilvusCluster(topology.GetTargetClusterID(), config)
		replicator, ok := r.replicators.Get(targetCluster.GetClusterID())
		if !ok {
			continue
		}
		err := replicator.StopReplication()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replicateManager) Close() {
	r.replicators.Range(func(key string, value replicator.Replicator) bool {
		value.StopReplication()
		return true
	})
}
