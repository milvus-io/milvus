package querycoord

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

// releasePartitionTask will release all the data of this partition on query nodes
type releasePartitionTask struct {
	*baseTask
	*querypb.ReleasePartitionsRequest
	cluster Cluster
	meta    Meta
}

func (rpt *releasePartitionTask) msgBase() *commonpb.MsgBase {
	return rpt.Base
}

func (rpt *releasePartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(rpt.ReleasePartitionsRequest)
}

func (rpt *releasePartitionTask) msgType() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionTask) timestamp() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionTask) updateTaskProcess() {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	parentTask := rpt.getParentTask()
	if parentTask == nil {
		// all queryNodes have successfully released the data, clean up collectionMeta
		err := rpt.meta.releasePartitions(collectionID, partitionIDs)
		if err != nil {
			log.Error("releasePartitionTask: release collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rpt.Base.MsgID), zap.Error(err))
			panic(err)
		}

	}
}

func (rpt *releasePartitionTask) preExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	rpt.setResultInfo(nil)
	log.Info("start do releasePartitionTask",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (rpt *releasePartitionTask) execute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rpt.NodeID <= 0 {
		onlineNodeIDs := rpt.cluster.OnlineNodeIDs()
		for _, nodeID := range onlineNodeIDs {
			req := proto.Clone(rpt.ReleasePartitionsRequest).(*querypb.ReleasePartitionsRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
			baseTask.setParentTask(rpt)
			releasePartitionTask := &releasePartitionTask{
				baseTask:                 baseTask,
				ReleasePartitionsRequest: req,
				cluster:                  rpt.cluster,
				meta:                     rpt.meta,
			}
			rpt.addChildTask(releasePartitionTask)
			log.Info("releasePartitionTask: add a releasePartitionTask to releasePartitionTask's childTask", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rpt.Base.MsgID))
		}
	} else {
		// If the node crashed or be offline, the loaded segments are lost
		defer rpt.reduceRetryCount()
		err := rpt.cluster.ReleasePartitions(ctx, rpt.NodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			log.Warn("ReleasePartitionsTask: release partition end, node occur error", zap.Int64("collectionID", collectionID), zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
			// after release failed, the task will always redo
			// if the query node happens to be down, the node release was judged to have succeeded
			return err
		}
	}

	log.Info("releasePartitionTask Execute done",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *releasePartitionTask) postExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	if rpt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		rpt.clearChildTasks()
	}

	log.Info("releasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *releasePartitionTask) rollBack(ctx context.Context) []task {
	//TODO::
	//if taskID == 0, recovery meta
	//if taskID != 0, recovery partition on queryNode
	return nil
}
