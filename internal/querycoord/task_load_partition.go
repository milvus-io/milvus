package querycoord

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

// loadPartitionTask will load all the data of this partition to query nodes
type loadPartitionTask struct {
	*baseTask
	*querypb.LoadPartitionsRequest
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
	addCol  bool
	once    sync.Once
}

func (lpt *loadPartitionTask) msgBase() *commonpb.MsgBase {
	return lpt.Base
}

func (lpt *loadPartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(lpt.LoadPartitionsRequest)
}

func (lpt *loadPartitionTask) msgType() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionTask) timestamp() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionTask) updateTaskProcess() {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	childTasks := lpt.getChildTask()
	allDone := true
	for _, t := range childTasks {
		if t.getState() != taskDone {
			allDone = false
		}

		// wait watchDeltaChannel task done after loading segment
		nodeID := getDstNodeIDByTask(t)
		if t.msgType() == commonpb.MsgType_LoadSegments {
			if !lpt.cluster.HasWatchedDeltaChannel(lpt.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}
	}
	if allDone {
		for _, id := range partitionIDs {
			err := lpt.meta.setLoadPercentage(collectionID, id, 100, querypb.LoadType_LoadPartition)
			if err != nil {
				log.Error("loadPartitionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", id))
				lpt.setResultInfo(err)
				return
			}
		}
		lpt.once.Do(func() {
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
			metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(lpt.elapseSpan().Milliseconds()))
			metrics.QueryCoordNumChildTasks.WithLabelValues().Sub(float64(len(lpt.getChildTask())))
		})
	}
}

func (lpt *loadPartitionTask) preExecute(context.Context) error {
	if lpt.ReplicaNumber < 1 {
		log.Warn("replicaNumber is less than 1 for load partitions request, will set it to 1",
			zap.Int32("replicaNumber", lpt.ReplicaNumber))
		lpt.ReplicaNumber = 1
	}

	collectionID := lpt.CollectionID
	lpt.setResultInfo(nil)
	log.Info("start do loadPartitionTask",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lpt *loadPartitionTask) execute(ctx context.Context) error {
	defer lpt.reduceRetryCount()
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs

	var (
		replicas          = make([]*milvuspb.ReplicaInfo, lpt.ReplicaNumber)
		replicaIds        = make([]int64, lpt.ReplicaNumber)
		segmentLoadInfos  = make([]*querypb.SegmentLoadInfo, 0)
		deltaChannelInfos = make([]*datapb.VchannelInfo, 0)
		dmChannelInfos    = make([]*datapb.VchannelInfo, 0)
		collectionSize    uint64
	)

	for _, partitionID := range partitionIDs {
		vChannelInfos, binlogs, err := lpt.broker.getRecoveryInfo(lpt.ctx, collectionID, partitionID)
		if err != nil {
			log.Error("loadPartitionTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			lpt.setResultInfo(err)
			return err
		}

		for _, segmentBingLog := range binlogs {
			segmentLoadInfo := lpt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, lpt.Schema)
			segmentLoadInfos = append(segmentLoadInfos, segmentLoadInfo)
			collectionSize += uint64(segmentLoadInfo.SegmentSize)
		}

		for _, info := range vChannelInfos {
			deltaChannelInfo, err := generateWatchDeltaChannelInfo(info)
			if err != nil {
				log.Error("loadPartitionTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
				lpt.setResultInfo(err)
				return err
			}
			deltaChannelInfos = append(deltaChannelInfos, deltaChannelInfo)
			dmChannelInfos = append(dmChannelInfos, info)
		}
	}
	mergedDeltaChannels := mergeWatchDeltaChannelInfo(deltaChannelInfos)
	// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
	err := lpt.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
	if err != nil {
		log.Error("loadPartitionTask: set delta channel info failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)

	for i := range replicas {
		replica, err := lpt.meta.generateReplica(lpt.CollectionID, partitionIDs)
		if err != nil {
			lpt.setResultInfo(err)
			return err
		}

		replicas[i] = replica
		replicaIds[i] = replica.ReplicaID
	}

	err = lpt.cluster.AssignNodesToReplicas(ctx, replicas, collectionSize)
	if err != nil {
		log.Error("failed to assign nodes to replicas",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", lpt.Base.MsgID),
			zap.Int32("replicaNumber", lpt.ReplicaNumber),
			zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		var (
			loadSegmentReqs    = []*querypb.LoadSegmentsRequest{}
			watchDmChannelReqs = []*querypb.WatchDmChannelsRequest{}
		)

		for _, segmentLoadInfo := range segmentLoadInfos {
			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:         msgBase,
				Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:       lpt.Schema,
				CollectionID: collectionID,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadPartition,
					CollectionID: collectionID,
					PartitionIDs: partitionIDs,
				},
				ReplicaID: replica.ReplicaID,
			}
			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		for _, info := range mergedDmChannel {
			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchDmChannels
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         msgBase,
				CollectionID: collectionID,
				PartitionIDs: partitionIDs,
				Infos:        []*datapb.VchannelInfo{info},
				Schema:       lpt.Schema,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadPartition,
					CollectionID: collectionID,
					PartitionIDs: partitionIDs,
				},
				ReplicaID: replica.GetReplicaID(),
			}

			fullWatchRequest, err := generateFullWatchDmChannelsRequest(lpt.meta, watchRequest)
			if err != nil {
				lpt.setResultInfo(err)
				return err
			}
			watchDmChannelReqs = append(watchDmChannelReqs, fullWatchRequest)
		}

		internalTasks, err := assignInternalTask(ctx, lpt, lpt.meta, lpt.cluster, loadSegmentReqs, watchDmChannelReqs, false, nil, replica.GetNodeIds(), -1, lpt.broker)
		if err != nil {
			log.Error("loadPartitionTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			lpt.setResultInfo(err)
			return err
		}
		for _, internalTask := range internalTasks {
			lpt.addChildTask(internalTask)
			if task, ok := internalTask.(*watchDmChannelTask); ok {
				nodeInfo, err := lpt.cluster.GetNodeInfoByID(task.NodeID)
				if err != nil {
					log.Error("loadCollectionTask: get shard leader node info failed",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", lpt.Base.MsgID),
						zap.Int64("nodeID", task.NodeID),
						zap.Error(err))
					lpt.setResultInfo(err)
					return err
				}

				replica.ShardReplicas = append(replica.ShardReplicas, &milvuspb.ShardReplica{
					LeaderID:      task.NodeID,
					LeaderAddr:    nodeInfo.(*queryNode).address,
					DmChannelName: task.WatchDmChannelsRequest.Infos[0].ChannelName,
				})
			}
			log.Info("loadPartitionTask: add a childTask", zap.Int64("collectionID", collectionID), zap.String("task type", internalTask.msgType().String()))
		}
		metrics.QueryCoordNumChildTasks.WithLabelValues().Add(float64(len(internalTasks)))
		log.Info("loadPartitionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID))
	}

	err = lpt.meta.addCollection(collectionID, querypb.LoadType_LoadPartition, lpt.Schema)
	if err != nil {
		log.Error("loadPartitionTask: add collection to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	err = lpt.meta.addPartitions(collectionID, partitionIDs)
	if err != nil {
		log.Error("loadPartitionTask: add partition to meta failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		err = lpt.meta.addReplica(replica)
		if err != nil {
			log.Error("failed to add replica", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Int32("replicaNumber", lpt.ReplicaNumber))
			lpt.setResultInfo(err)
			return err
		}
	}

	log.Info("loadPartitionTask Execute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", lpt.Base.MsgID))
	return nil
}

func (lpt *loadPartitionTask) postExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lpt.clearChildTasks()
		err := lpt.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("loadPartitionTask: occur error when release collection info from meta", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}

	log.Info("loadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *loadPartitionTask) globalPostExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID

	collection, err := lpt.meta.getCollectionInfoByID(collectionID)
	if err != nil {
		log.Error("loadPartitionTask: failed to get collection info from meta",
			zap.Int64("taskID", lpt.getTaskID()),
			zap.Int64("collectionID", collectionID),
			zap.Error(err))

		return err
	}

	for _, replica := range collection.ReplicaIds {
		err := syncReplicaSegments(lpt.ctx, lpt.meta, lpt.cluster, replica)
		if err != nil {
			log.Error("loadPartitionTask: failed to sync replica segments to shard leader",
				zap.Int64("taskID", lpt.getTaskID()),
				zap.Int64("collectionID", collectionID),
				zap.Error(err))

			return err
		}
	}

	return nil
}

func (lpt *loadPartitionTask) rollBack(ctx context.Context) []task {
	collectionID := lpt.CollectionID
	resultTasks := make([]task, 0)
	//brute force rollBack, should optimize
	onlineNodeIDs := lpt.cluster.OnlineNodeIDs()
	for _, nodeID := range onlineNodeIDs {
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ReleaseCollection,
				MsgID:     lpt.Base.MsgID,
				Timestamp: lpt.Base.Timestamp,
				SourceID:  lpt.Base.SourceID,
			},
			DbID:         lpt.DbID,
			CollectionID: collectionID,
			NodeID:       nodeID,
		}
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
		baseTask.setParentTask(lpt)
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: req,
			cluster:                  lpt.cluster,
		}
		resultTasks = append(resultTasks, releaseCollectionTask)
	}

	err := lpt.meta.releaseCollection(collectionID)
	if err != nil {
		log.Error("loadPartitionTask: release collection info from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		panic(err)
	}
	log.Info("loadPartitionTask: generate rollBack task for loadPartitionTask", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID))
	return resultTasks
}
