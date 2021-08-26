package querycoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestTriggerTask(t *testing.T) {
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	t.Run("Test LoadCollection", func(t *testing.T) {
		req := &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genCollectionSchema(defaultCollectionID, false),
		}
		loadCollectionTask := &LoadCollectionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadCollectionRequest: req,
			rootCoord:             queryCoord.rootCoordClient,
			dataCoord:             queryCoord.dataCoordClient,
			cluster:               queryCoord.cluster,
			meta:                  queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		}
		loadCollectionTask := &ReleaseCollectionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleaseCollectionRequest: req,
			rootCoord:                queryCoord.rootCoordClient,
			cluster:                  queryCoord.cluster,
			meta:                     queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		req := &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		loadCollectionTask := &LoadPartitionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadPartitionsRequest: req,
			dataCoord:             queryCoord.dataCoordClient,
			cluster:               queryCoord.cluster,
			meta:                  queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		req := &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		loadCollectionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleasePartitionsRequest: req,
			cluster:                  queryCoord.cluster,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	//nodes, err := queryCoord.cluster.getOnServiceNodes()
	//assert.Nil(t, err)

	err = node.stop()
	//assert.Nil(t, err)

	//allNodeOffline := waitAllQueryNodeOffline(queryCoord.cluster, nodes)
	//assert.Equal(t, allNodeOffline, true)
	queryCoord.Stop()
}
