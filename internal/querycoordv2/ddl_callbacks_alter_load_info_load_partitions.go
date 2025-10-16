package querycoordv2

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (s *Server) broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) error {
	// just get the collection name and db name.
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// double check if the collection is already dropped
	if coll, err = s.broker.DescribeCollection(ctx, req.GetCollectionID()); err != nil {
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
	partitionIDsSet := typeutil.NewSet[int64](currentLoadConfig.GetPartitionIDs()...)
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
	msg, err := job.GenerateAlterLoadConfigMessage(ctx, alterLoadConfigReq)
	if err != nil {
		if errors.Is(err, job.ErrNoChanged) {
			return nil
		}
		return err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}
