package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (s *Server) broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) error {
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// double check if the collection is already dropped
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	userSpecifiedReplicaMode := req.GetReplicaNumber() > 0
	replicaNumber, resourceGroups, err := s.getDefaultResourceGroupsAndReplicaNumber(ctx, req.GetReplicaNumber(), req.GetResourceGroups(), req.GetCollectionID())
	if err != nil {
		return err
	}

	expectedReplicasNumber, err := utils.AssignReplica(ctx, s.meta, resourceGroups, replicaNumber, false)
	if err != nil {
		return err
	}

	currentLoadConfig := s.getCurrentLoadConfig(ctx, req.GetCollectionID())
	partitionIDsSet := typeutil.NewSet(currentLoadConfig.GetPartitionIDs()...)
	// add new incoming partitionIDs.
	for _, partition := range req.PartitionIDs {
		partitionIDsSet.Insert(partition)
	}
	alterLoadConfigReq := &job.AlterLoadConfigRequest{
		Meta:           s.meta,
		CollectionInfo: coll,
		Current:        s.getCurrentLoadConfig(ctx, req.GetCollectionID()),
		Expected: job.ExpectedLoadConfig{
			ExpectedPartitionIDs:             partitionIDsSet.Collect(),
			ExpectedReplicaNumber:            expectedReplicasNumber,
			ExpectedFieldIndexID:             req.GetFieldIndexID(),
			ExpectedLoadFields:               req.GetLoadFields(),
			ExpectedPriority:                 req.GetPriority(),
			ExpectedUserSpecifiedReplicaMode: userSpecifiedReplicaMode,
		},
	}
	if err := alterLoadConfigReq.CheckIfLoadPartitionsExecutable(); err != nil {
		return err
	}
	msg, err := job.GenerateAlterLoadConfigMessage(ctx, alterLoadConfigReq)
	if err != nil {
		return err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}
