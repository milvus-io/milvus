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

type loadCollectionTask struct {
	*baseTask
	*querypb.LoadCollectionRequest
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
	once    sync.Once
}

func (lct *loadCollectionTask) msgBase() *commonpb.MsgBase {
	return lct.Base
}

func (lct *loadCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(lct.LoadCollectionRequest)
}

func (lct *loadCollectionTask) msgType() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) timestamp() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) updateTaskProcess() {
	collectionID := lct.CollectionID
	childTasks := lct.getChildTask()
	allDone := true
	for _, t := range childTasks {
		if t.getState() != taskDone {
			allDone = false
			break
		}

		// wait watchDeltaChannel task done after loading segment
		nodeID := getDstNodeIDByTask(t)
		if t.msgType() == commonpb.MsgType_LoadSegments {
			if !lct.cluster.HasWatchedDeltaChannel(lct.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}

	}
	if allDone {
		err := lct.meta.setLoadPercentage(collectionID, 0, 100, querypb.LoadType_LoadCollection)
		if err != nil {
			log.Error("loadCollectionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID))
			lct.setResultInfo(err)
			return
		}

		lct.once.Do(func() {
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
			metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(lct.elapseSpan().Milliseconds()))
			metrics.QueryCoordNumChildTasks.WithLabelValues().Sub(float64(len(lct.getChildTask())))
		})
	}
}

func (lct *loadCollectionTask) preExecute(ctx context.Context) error {
	if lct.ReplicaNumber < 1 {
		log.Warn("replicaNumber is less than 1 for load collection request, will set it to 1",
			zap.Int32("replicaNumber", lct.ReplicaNumber))
		lct.ReplicaNumber = 1
	}

	collectionID := lct.CollectionID
	schema := lct.Schema
	lct.setResultInfo(nil)
	log.Info("start do loadCollectionTask",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema),
		zap.Int32("replicaNumber", lct.ReplicaNumber))
	return nil
}

func (lct *loadCollectionTask) execute(ctx context.Context) error {
	defer lct.reduceRetryCount()
	collectionID := lct.CollectionID

	partitionIds, err := lct.broker.showPartitionIDs(ctx, collectionID)
	if err != nil {
		log.Error("loadCollectionTask: showPartition failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}
	log.Info("loadCollectionTask: get collection's all partitionIDs", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID))

	var (
		replicas          = make([]*milvuspb.ReplicaInfo, lct.ReplicaNumber)
		replicaIds        = make([]int64, lct.ReplicaNumber)
		segmentLoadInfos  = make([]*querypb.SegmentLoadInfo, 0)
		deltaChannelInfos = make([]*datapb.VchannelInfo, 0)
		dmChannelInfos    = make([]*datapb.VchannelInfo, 0)
		collectionSize    uint64
	)

	for _, partitionID := range partitionIds {
		vChannelInfos, binlogs, err := lct.broker.getRecoveryInfo(lct.ctx, collectionID, partitionID)
		if err != nil {
			log.Error("loadCollectionTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			lct.setResultInfo(err)
			return err
		}

		for _, segmentBinlog := range binlogs {
			segmentLoadInfo := lct.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBinlog, true, lct.Schema)
			collectionSize += uint64(segmentLoadInfo.SegmentSize)
			segmentLoadInfos = append(segmentLoadInfos, segmentLoadInfo)
		}

		for _, info := range vChannelInfos {
			deltaChannelInfo, err := generateWatchDeltaChannelInfo(info)
			if err != nil {
				log.Error("loadCollectionTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
				lct.setResultInfo(err)
				return err
			}
			deltaChannelInfos = append(deltaChannelInfos, deltaChannelInfo)
			dmChannelInfos = append(dmChannelInfos, info)
		}
	}

	mergedDeltaChannels := mergeWatchDeltaChannelInfo(deltaChannelInfos)
	// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
	err = lct.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
	if err != nil {
		log.Error("loadCollectionTask: set delta channel info failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)

	for i := range replicas {
		replica, err := lct.meta.generateReplica(lct.CollectionID, partitionIds)
		if err != nil {
			lct.setResultInfo(err)
			return err
		}

		replicas[i] = replica
		replicaIds[i] = replica.ReplicaID
	}

	err = lct.cluster.AssignNodesToReplicas(ctx, replicas, collectionSize)
	if err != nil {
		log.Error("failed to assign nodes to replicas",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIds),
			zap.Int64("msgID", lct.Base.MsgID),
			zap.Int32("replicaNumber", lct.ReplicaNumber),
			zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		var (
			loadSegmentReqs    = []*querypb.LoadSegmentsRequest{}
			watchDmChannelReqs = []*querypb.WatchDmChannelsRequest{}
		)

		for _, segmentLoadInfo := range segmentLoadInfos {
			msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:         msgBase,
				Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:       lct.Schema,
				CollectionID: collectionID,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadCollection,
					CollectionID: collectionID,
					PartitionIDs: partitionIds,
				},
				ReplicaID: replica.ReplicaID,
			}

			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		//TODO:: queryNode receive dm message according partitionID cache
		//TODO:: queryNode add partitionID to cache if receive create partition message from dmChannel
		for _, info := range mergedDmChannel {
			msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchDmChannels
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         msgBase,
				CollectionID: collectionID,
				//PartitionIDs: toLoadPartitionIDs,
				Infos:  []*datapb.VchannelInfo{info},
				Schema: lct.Schema,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadCollection,
					CollectionID: collectionID,
					PartitionIDs: partitionIds,
				},
				ReplicaID: replica.GetReplicaID(),
			}

			fullWatchRequest, err := generateFullWatchDmChannelsRequest(lct.meta, watchRequest)
			if err != nil {
				lct.setResultInfo(err)
				return err
			}
			watchDmChannelReqs = append(watchDmChannelReqs, fullWatchRequest)
		}

		internalTasks, err := assignInternalTask(ctx, lct, lct.meta, lct.cluster, loadSegmentReqs, watchDmChannelReqs, false, nil, replica.GetNodeIds(), -1, lct.broker)
		if err != nil {
			log.Error("loadCollectionTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			lct.setResultInfo(err)
			return err
		}
		for _, internalTask := range internalTasks {
			lct.addChildTask(internalTask)
			if task, ok := internalTask.(*watchDmChannelTask); ok {
				nodeInfo, err := lct.cluster.GetNodeInfoByID(task.NodeID)
				if err != nil {
					log.Error("loadCollectionTask: get shard leader node info failed",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", lct.Base.MsgID),
						zap.Int64("nodeID", task.NodeID),
						zap.Error(err))
					lct.setResultInfo(err)
					return err
				}
				replica.ShardReplicas = append(replica.ShardReplicas, &milvuspb.ShardReplica{
					LeaderID:      task.NodeID,
					LeaderAddr:    nodeInfo.(*queryNode).address,
					DmChannelName: task.WatchDmChannelsRequest.Infos[0].ChannelName,
				})
			}
			log.Info("loadCollectionTask: add a childTask", zap.Int64("collectionID", collectionID), zap.String("task type", internalTask.msgType().String()), zap.Int64("msgID", lct.Base.MsgID))
		}
		metrics.QueryCoordNumChildTasks.WithLabelValues().Add(float64(len(internalTasks)))
		log.Info("loadCollectionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID))
	}

	err = lct.meta.addCollection(collectionID, querypb.LoadType_LoadCollection, lct.Schema)
	if err != nil {
		log.Error("loadCollectionTask: add collection to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	err = lct.meta.addPartitions(collectionID, partitionIds)
	if err != nil {
		log.Error("loadCollectionTask: add partitions to meta failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		err = lct.meta.addReplica(replica)
		if err != nil {
			log.Error("failed to add replica", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID), zap.Int32("replicaNumber", lct.ReplicaNumber))
			lct.setResultInfo(err)
			return err
		}
	}

	log.Info("LoadCollection execute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) postExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	if lct.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lct.clearChildTasks()
		err := lct.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("loadCollectionTask: occur error when release collection info from meta", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}

	log.Info("loadCollectionTask postExecute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) globalPostExecute(ctx context.Context) error {
	collection, err := lct.meta.getCollectionInfoByID(lct.CollectionID)
	if err != nil {
		log.Error("loadCollectionTask: failed to get collection info from meta",
			zap.Int64("taskID", lct.getTaskID()),
			zap.Int64("collectionID", lct.CollectionID),
			zap.Error(err))

		return err
	}

	for _, replica := range collection.ReplicaIds {
		err := syncReplicaSegments(lct.ctx, lct.meta, lct.cluster, replica)
		if err != nil {
			log.Error("loadCollectionTask: failed to sync replica segments to shard leader",
				zap.Int64("taskID", lct.getTaskID()),
				zap.Int64("collectionID", lct.CollectionID),
				zap.Error(err))

			return err
		}
	}

	return nil
}

func (lct *loadCollectionTask) rollBack(ctx context.Context) []task {
	onlineNodeIDs := lct.cluster.OnlineNodeIDs()
	resultTasks := make([]task, 0)
	for _, nodeID := range onlineNodeIDs {
		//brute force rollBack, should optimize
		msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
		msgBase.MsgType = commonpb.MsgType_ReleaseCollection
		req := &querypb.ReleaseCollectionRequest{
			Base:         msgBase,
			DbID:         lct.DbID,
			CollectionID: lct.CollectionID,
			NodeID:       nodeID,
		}
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
		baseTask.setParentTask(lct)
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: req,
			cluster:                  lct.cluster,
		}
		resultTasks = append(resultTasks, releaseCollectionTask)
	}

	err := lct.meta.releaseCollection(lct.CollectionID)
	if err != nil {
		log.Error("releaseCollectionTask: release collectionInfo from meta failed", zap.Int64("collectionID", lct.CollectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		panic(err)
	}

	log.Info("loadCollectionTask: generate rollBack task for loadCollectionTask", zap.Int64("collectionID", lct.CollectionID), zap.Int64("msgID", lct.Base.MsgID))
	return resultTasks
}
