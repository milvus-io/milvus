package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (s *Server) broadcastAlterLoadConfigCollectionV2ForReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) error {
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

	currentLoadConfig := s.getCurrentLoadConfig(ctx, req.GetCollectionID())
	if currentLoadConfig.Collection == nil {
		// collection is not loaded, return success directly.
		return nil
	}

	// remove the partitions that should be released.
	partitionIDsSet := typeutil.NewSet(currentLoadConfig.GetPartitionIDs()...)
	previousLength := len(partitionIDsSet)
	for _, partitionID := range req.PartitionIDs {
		partitionIDsSet.Remove(partitionID)
	}

	// no partition to be released, return success directly.
	if len(partitionIDsSet) == previousLength {
		return job.ErrIgnoredAlterLoadConfig
	}

	var msg message.BroadcastMutableMessage
	if len(partitionIDsSet) == 0 {
		// all partitions are released, release the collection directly.
		msg = message.NewDropLoadConfigMessageBuilderV2().
			WithHeader(&message.DropLoadConfigMessageHeader{
				DbId:         coll.DbId,
				CollectionId: coll.CollectionID,
			}).
			WithBody(&message.DropLoadConfigMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}). // TODO: after we support query view in 3.0, we should broadcast the drop load config message to all vchannels.
			MustBuildBroadcast()
	} else {
		// only some partitions are released, alter the load config.
		alterLoadConfigReq := &job.AlterLoadConfigRequest{
			Meta:           s.meta,
			CollectionInfo: coll,
			Current:        s.getCurrentLoadConfig(ctx, req.GetCollectionID()),
			Expected: job.ExpectedLoadConfig{
				ExpectedPartitionIDs:             partitionIDsSet.Collect(),
				ExpectedReplicaNumber:            currentLoadConfig.GetReplicaNumber(),
				ExpectedFieldIndexID:             currentLoadConfig.GetFieldIndexID(),
				ExpectedLoadFields:               currentLoadConfig.GetLoadFields(),
				ExpectedPriority:                 commonpb.LoadPriority_HIGH,
				ExpectedUserSpecifiedReplicaMode: currentLoadConfig.GetUserSpecifiedReplicaMode(),
			},
		}
		if msg, err = job.GenerateAlterLoadConfigMessage(ctx, alterLoadConfigReq); err != nil {
			return err
		}
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}
