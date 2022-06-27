package querycoord

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

// releaseCollectionTask will release all the data of this collection on query nodes
type releaseCollectionTask struct {
	*baseTask
	*querypb.ReleaseCollectionRequest
	cluster Cluster
	meta    Meta
	broker  *globalMetaBroker
}

func (rct *releaseCollectionTask) msgBase() *commonpb.MsgBase {
	return rct.Base
}

func (rct *releaseCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(rct.ReleaseCollectionRequest)
}

func (rct *releaseCollectionTask) msgType() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) timestamp() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) updateTaskProcess() {
	collectionID := rct.CollectionID
	parentTask := rct.getParentTask()
	if parentTask == nil {
		// all queryNodes have successfully released the data, clean up collectionMeta
		err := rct.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("releaseCollectionTask: release collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rct.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}
}

func (rct *releaseCollectionTask) preExecute(context.Context) error {
	collectionID := rct.CollectionID
	rct.setResultInfo(nil)
	log.Info("start do releaseCollectionTask",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *releaseCollectionTask) execute(ctx context.Context) error {
	collectionID := rct.CollectionID

	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rct.NodeID <= 0 {
		// invalidate all the collection meta cache with the specified collectionID
		err := rct.broker.invalidateCollectionMetaCache(ctx, collectionID)
		if err != nil {
			log.Error("releaseCollectionTask: release collection end, invalidateCollectionMetaCache occur error", zap.Int64("collectionID", rct.CollectionID), zap.Int64("msgID", rct.Base.MsgID), zap.Error(err))
			rct.setResultInfo(err)
			return err
		}

		// TODO(yah01): broadcast to all nodes? Or only nodes serve the collection
		onlineNodeIDs := rct.cluster.OnlineNodeIDs()
		for _, nodeID := range onlineNodeIDs {
			req := proto.Clone(rct.ReleaseCollectionRequest).(*querypb.ReleaseCollectionRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
			baseTask.setParentTask(rct)
			releaseCollectionTask := &releaseCollectionTask{
				baseTask:                 baseTask,
				ReleaseCollectionRequest: req,
				cluster:                  rct.cluster,
			}

			rct.addChildTask(releaseCollectionTask)
			log.Info("releaseCollectionTask: add a releaseCollectionTask to releaseCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	} else {
		// If the node crashed or be offline, the loaded segments are lost
		defer rct.reduceRetryCount()
		err := rct.cluster.ReleaseCollection(ctx, rct.NodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Warn("releaseCollectionTask: release collection end, node occur error", zap.Int64("collectionID", collectionID), zap.Int64("nodeID", rct.NodeID))
			// after release failed, the task will always redo
			// if the query node happens to be down, the node release was judged to have succeeded
			return err
		}
	}

	log.Info("releaseCollectionTask Execute done",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *releaseCollectionTask) postExecute(context.Context) error {
	collectionID := rct.CollectionID
	if rct.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		rct.clearChildTasks()
	}

	log.Info("releaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *releaseCollectionTask) rollBack(ctx context.Context) []task {
	//TODO::
	//if taskID == 0, recovery meta
	//if taskID != 0, recovery collection on queryNode
	return nil
}
