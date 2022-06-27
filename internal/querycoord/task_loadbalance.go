package querycoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ task = (*loadBalanceTask)(nil)

type loadBalanceTask struct {
	*baseTask
	*querypb.LoadBalanceRequest
	broker    *globalMetaBroker
	cluster   Cluster
	meta      Meta
	replicaID int64
}

func (lbt *loadBalanceTask) msgBase() *commonpb.MsgBase {
	return lbt.Base
}

func (lbt *loadBalanceTask) marshal() ([]byte, error) {
	return proto.Marshal(lbt.LoadBalanceRequest)
}

func (lbt *loadBalanceTask) msgType() commonpb.MsgType {
	return lbt.Base.MsgType
}

func (lbt *loadBalanceTask) timestamp() Timestamp {
	return lbt.Base.Timestamp
}

func (lbt *loadBalanceTask) preExecute(context.Context) error {
	lbt.setResultInfo(nil)
	log.Info("start do loadBalanceTask",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))

	if lbt.triggerCondition == querypb.TriggerCondition_LoadBalance {
		if err := lbt.checkForManualLoadBalance(); err != nil {
			lbt.setResultInfo(err)
			return err
		}
		if len(lbt.SourceNodeIDs) == 0 {
			err := errors.New("loadBalanceTask: empty source Node list to balance")
			log.Error(err.Error())
			lbt.setResultInfo(err)
			return err
		}
	}

	return nil
}

func (lbt *loadBalanceTask) checkForManualLoadBalance() error {
	// check segments belong to the same collection
	collectionID := lbt.GetCollectionID()
	for _, sid := range lbt.SealedSegmentIDs {
		segment, err := lbt.meta.getSegmentInfoByID(sid)
		if err != nil {
			return err
		}
		if collectionID == 0 {
			collectionID = segment.GetCollectionID()
		} else if collectionID != segment.GetCollectionID() {
			err := errors.New("segments of a load balance task do not belong to the same collection")
			return err
		}
	}

	if collectionID == 0 {
		err := errors.New("a load balance task has to specify a collectionID or pass segments of a collection")
		return err
	}

	// check source and dst nodes belong to the same replica
	var replicaID int64 = -1
	for _, nodeID := range lbt.SourceNodeIDs {
		replica, err := lbt.getReplica(nodeID, collectionID)
		if err != nil {
			return err
		}
		if replicaID == -1 {
			replicaID = replica.GetReplicaID()
		} else if replicaID != replica.GetReplicaID() {
			err := errors.New("source nodes and destination nodes must be in the same replica group")
			return err
		}
	}

	if replicaID == -1 {
		return errors.New("source nodes is empty")
	}

	for _, nodeID := range lbt.DstNodeIDs {
		replica, err := lbt.getReplica(nodeID, collectionID)
		if err != nil {
			return err
		}
		if replicaID != replica.GetReplicaID() {
			err := errors.New("source nodes and destination nodes must be in the same replica group")
			return err
		}
	}

	lbt.replicaID = replicaID

	log.Info("start do loadBalanceTask",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) execute(ctx context.Context) error {
	defer lbt.reduceRetryCount()

	if lbt.triggerCondition == querypb.TriggerCondition_NodeDown {
		err := lbt.processNodeDownLoadBalance(ctx)
		if err != nil {
			return err
		}
	} else if lbt.triggerCondition == querypb.TriggerCondition_LoadBalance {
		err := lbt.processManualLoadBalance(ctx)
		if err != nil {
			return err
		}
	}

	log.Info("loadBalanceTask Execute done",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) processNodeDownLoadBalance(ctx context.Context) error {
	var internalTasks []task
	for _, nodeID := range lbt.SourceNodeIDs {
		segments := make(map[UniqueID]*querypb.SegmentInfo)
		dmChannels := make(map[string]*querypb.DmChannelWatchInfo)
		recoveredCollectionIDs := make(typeutil.UniqueSet)
		segmentInfos := lbt.meta.getSegmentInfosByNode(nodeID)
		for _, segmentInfo := range segmentInfos {
			segments[segmentInfo.SegmentID] = segmentInfo
			recoveredCollectionIDs.Insert(segmentInfo.CollectionID)
		}
		dmChannelWatchInfos := lbt.meta.getDmChannelInfosByNodeID(nodeID)
		for _, watchInfo := range dmChannelWatchInfos {
			dmChannels[watchInfo.DmChannel] = watchInfo
			recoveredCollectionIDs.Insert(watchInfo.CollectionID)
		}

		if len(segments) == 0 && len(dmChannels) == 0 {
			continue
		}

		for collectionID := range recoveredCollectionIDs {
			loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
			watchDmChannelReqs := make([]*querypb.WatchDmChannelsRequest, 0)
			collectionInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
			if err != nil {
				log.Error("loadBalanceTask: get collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Error(err))
				lbt.setResultInfo(err)
				return err
			}
			schema := collectionInfo.Schema
			var deltaChannelInfos []*datapb.VchannelInfo
			var dmChannelInfos []*datapb.VchannelInfo

			var toRecoverPartitionIDs []UniqueID
			if collectionInfo.LoadType == querypb.LoadType_LoadCollection {
				toRecoverPartitionIDs, err = lbt.broker.showPartitionIDs(ctx, collectionID)
				if err != nil {
					log.Error("loadBalanceTask: show collection's partitionIDs failed", zap.Int64("collectionID", collectionID), zap.Error(err))
					lbt.setResultInfo(err)
					panic(err)
				}
			} else {
				toRecoverPartitionIDs = collectionInfo.PartitionIDs
			}
			log.Info("loadBalanceTask: get collection's all partitionIDs", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", toRecoverPartitionIDs))
			replica, err := lbt.getReplica(nodeID, collectionID)
			if err != nil {
				// getReplica maybe failed, it will cause the balanceTask execute infinitely
				log.Warn("loadBalanceTask: get replica failed", zap.Int64("collectionID", collectionID), zap.Int64("nodeId", nodeID))
				continue
			}

			for _, partitionID := range toRecoverPartitionIDs {
				vChannelInfos, binlogs, err := lbt.broker.getRecoveryInfo(lbt.ctx, collectionID, partitionID)
				if err != nil {
					log.Error("loadBalanceTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
					lbt.setResultInfo(err)
					panic(err)
				}

				for _, segmentBingLog := range binlogs {
					segmentID := segmentBingLog.SegmentID
					if _, ok := segments[segmentID]; ok {
						segmentLoadInfo := lbt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, schema)

						msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
						msgBase.MsgType = commonpb.MsgType_LoadSegments

						loadSegmentReq := &querypb.LoadSegmentsRequest{
							Base:         msgBase,
							Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
							Schema:       schema,
							CollectionID: collectionID,

							LoadMeta: &querypb.LoadMetaInfo{
								LoadType:     collectionInfo.LoadType,
								CollectionID: collectionID,
								PartitionIDs: toRecoverPartitionIDs,
							},
							ReplicaID: replica.ReplicaID,
						}

						loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
					}
				}

				for _, info := range vChannelInfos {
					deltaChannel, err := generateWatchDeltaChannelInfo(info)
					if err != nil {
						log.Error("loadBalanceTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Error(err))
						lbt.setResultInfo(err)
						panic(err)
					}
					deltaChannelInfos = append(deltaChannelInfos, deltaChannel)
					dmChannelInfos = append(dmChannelInfos, info)
				}
			}

			mergedDeltaChannel := mergeWatchDeltaChannelInfo(deltaChannelInfos)
			// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
			err = lbt.meta.setDeltaChannel(collectionID, mergedDeltaChannel)
			if err != nil {
				log.Error("loadBalanceTask: set delta channel info meta failed", zap.Int64("collectionID", collectionID), zap.Error(err))
				lbt.setResultInfo(err)
				panic(err)
			}

			mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)
			for channelName, vChannelInfo := range mergedDmChannel {
				if _, ok := dmChannels[channelName]; ok {
					msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
					msgBase.MsgType = commonpb.MsgType_WatchDmChannels
					watchRequest := &querypb.WatchDmChannelsRequest{
						Base:         msgBase,
						CollectionID: collectionID,
						Infos:        []*datapb.VchannelInfo{vChannelInfo},
						Schema:       schema,
						LoadMeta: &querypb.LoadMetaInfo{
							LoadType:     collectionInfo.LoadType,
							CollectionID: collectionID,
							PartitionIDs: toRecoverPartitionIDs,
						},
						ReplicaID: replica.ReplicaID,
					}

					if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
						watchRequest.PartitionIDs = toRecoverPartitionIDs
					}

					fullWatchRequest, err := generateFullWatchDmChannelsRequest(lbt.meta, watchRequest)
					if err != nil {
						lbt.setResultInfo(err)
						return err
					}

					watchDmChannelReqs = append(watchDmChannelReqs, fullWatchRequest)
				}
			}

			tasks, err := assignInternalTask(ctx, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, watchDmChannelReqs, false, lbt.SourceNodeIDs, lbt.DstNodeIDs, replica.GetReplicaID(), lbt.broker)
			if err != nil {
				log.Error("loadBalanceTask: assign child task failed", zap.Int64("sourceNodeID", nodeID))
				lbt.setResultInfo(err)
				return err
			}
			internalTasks = append(internalTasks, tasks...)
		}
	}
	for _, internalTask := range internalTasks {
		lbt.addChildTask(internalTask)
		log.Info("loadBalanceTask: add a childTask", zap.String("task type", internalTask.msgType().String()), zap.Any("task", internalTask))
	}
	log.Info("loadBalanceTask: assign child task done", zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs))

	return nil
}

func (lbt *loadBalanceTask) processManualLoadBalance(ctx context.Context) error {
	balancedSegmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	balancedSegmentIDs := make([]UniqueID, 0)
	for _, nodeID := range lbt.SourceNodeIDs {
		nodeExist := lbt.cluster.HasNode(nodeID)
		if !nodeExist {
			err := fmt.Errorf("loadBalanceTask: query node %d is not exist to balance", nodeID)
			log.Error(err.Error())
			lbt.setResultInfo(err)
			return err
		}
		segmentInfos := lbt.meta.getSegmentInfosByNode(nodeID)
		for _, info := range segmentInfos {
			balancedSegmentInfos[info.SegmentID] = info
			balancedSegmentIDs = append(balancedSegmentIDs, info.SegmentID)
		}
	}

	// check balanced sealedSegmentIDs in request whether exist in query node
	for _, segmentID := range lbt.SealedSegmentIDs {
		if _, ok := balancedSegmentInfos[segmentID]; !ok {
			err := fmt.Errorf("loadBalanceTask: unloaded segment %d", segmentID)
			log.Warn(err.Error())
			lbt.setResultInfo(err)
			return err
		}
	}

	if len(lbt.SealedSegmentIDs) != 0 {
		balancedSegmentIDs = lbt.SealedSegmentIDs
	}

	// TODO(yah01): release balanced segments in source nodes
	// balancedSegmentSet := make(typeutil.UniqueSet)
	// balancedSegmentSet.Insert(balancedSegmentIDs...)

	// for _, nodeID := range lbt.SourceNodeIDs {
	// 	segments := lbt.meta.getSegmentInfosByNode(nodeID)

	// 	shardSegments := make(map[string][]UniqueID)
	// 	for _, segment := range segments {
	// 		if !balancedSegmentSet.Contain(segment.SegmentID) {
	// 			continue
	// 		}

	// 		shardSegments[segment.DmChannel] = append(shardSegments[segment.DmChannel], segment.SegmentID)
	// 	}

	// 	for dmc, segmentIDs := range shardSegments {
	// 		shardLeader, err := getShardLeaderByNodeID(lbt.meta, lbt.replicaID, dmc)
	// 		if err != nil {
	// 			log.Error("failed to get shardLeader",
	// 				zap.Int64("replicaID", lbt.replicaID),
	// 				zap.Int64("nodeID", nodeID),
	// 				zap.String("dmChannel", dmc),
	// 				zap.Error(err))
	// 			lbt.setResultInfo(err)

	// 			return err
	// 		}

	// 		releaseSegmentReq := &querypb.ReleaseSegmentsRequest{
	// 			Base: &commonpb.MsgBase{
	// 				MsgType: commonpb.MsgType_ReleaseSegments,
	// 			},

	// 			NodeID:     nodeID,
	// 			SegmentIDs: segmentIDs,
	// 		}
	// 		baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
	// 		lbt.addChildTask(&releaseSegmentTask{
	// 			baseTask:               baseTask,
	// 			ReleaseSegmentsRequest: releaseSegmentReq,
	// 			cluster:                lbt.cluster,
	// 			leaderID:               shardLeader,
	// 		})
	// 	}
	// }

	col2PartitionIDs := make(map[UniqueID][]UniqueID)
	par2Segments := make(map[UniqueID][]*querypb.SegmentInfo)
	for _, segmentID := range balancedSegmentIDs {
		info := balancedSegmentInfos[segmentID]
		collectionID := info.CollectionID
		partitionID := info.PartitionID
		if _, ok := col2PartitionIDs[collectionID]; !ok {
			col2PartitionIDs[collectionID] = make([]UniqueID, 0)
		}
		if _, ok := par2Segments[partitionID]; !ok {
			col2PartitionIDs[collectionID] = append(col2PartitionIDs[collectionID], partitionID)
			par2Segments[partitionID] = make([]*querypb.SegmentInfo, 0)
		}
		par2Segments[partitionID] = append(par2Segments[partitionID], info)
	}

	loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
	for collectionID, partitionIDs := range col2PartitionIDs {
		var watchDeltaChannels []*datapb.VchannelInfo
		collectionInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
		if err != nil {
			log.Error("loadBalanceTask: can't find collectionID in meta", zap.Int64("collectionID", collectionID), zap.Error(err))
			lbt.setResultInfo(err)
			return err
		}
		for _, partitionID := range partitionIDs {
			dmChannelInfos, binlogs, err := lbt.broker.getRecoveryInfo(lbt.ctx, collectionID, partitionID)
			if err != nil {
				log.Error("loadBalanceTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
				lbt.setResultInfo(err)
				return err
			}

			segmentID2Binlog := make(map[UniqueID]*datapb.SegmentBinlogs)
			for _, binlog := range binlogs {
				segmentID2Binlog[binlog.SegmentID] = binlog
			}

			for _, segmentInfo := range par2Segments[partitionID] {
				segmentID := segmentInfo.SegmentID
				if _, ok := segmentID2Binlog[segmentID]; !ok {
					log.Warn("loadBalanceTask: can't find binlog of segment to balance, may be has been compacted", zap.Int64("segmentID", segmentID))
					continue
				}

				segmentBingLog := segmentID2Binlog[segmentID]
				segmentLoadInfo := lbt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, collectionInfo.Schema)
				msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
				msgBase.MsgType = commonpb.MsgType_LoadSegments
				loadSegmentReq := &querypb.LoadSegmentsRequest{
					Base:         msgBase,
					Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
					Schema:       collectionInfo.Schema,
					CollectionID: collectionID,
					ReplicaID:    lbt.replicaID,
					LoadMeta: &querypb.LoadMetaInfo{
						CollectionID: collectionID,
						PartitionIDs: collectionInfo.PartitionIDs,
					},
				}
				loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
			}

			for _, info := range dmChannelInfos {
				deltaChannel, err := generateWatchDeltaChannelInfo(info)
				if err != nil {
					return err
				}
				watchDeltaChannels = append(watchDeltaChannels, deltaChannel)
			}
		}
		mergedDeltaChannels := mergeWatchDeltaChannelInfo(watchDeltaChannels)
		// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
		err = lbt.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
		if err != nil {
			log.Error("loadBalanceTask: set delta channel info to meta failed", zap.Error(err))
			lbt.setResultInfo(err)
			return err
		}
	}
	internalTasks, err := assignInternalTask(ctx, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, nil, false, lbt.SourceNodeIDs, lbt.DstNodeIDs, lbt.replicaID, lbt.broker)
	if err != nil {
		log.Error("loadBalanceTask: assign child task failed", zap.Any("balance request", lbt.LoadBalanceRequest))
		lbt.setResultInfo(err)
		return err
	}
	for _, internalTask := range internalTasks {
		lbt.addChildTask(internalTask)
		log.Info("loadBalanceTask: add a childTask", zap.String("task type", internalTask.msgType().String()), zap.Any("balance request", lbt.LoadBalanceRequest))
	}
	log.Info("loadBalanceTask: assign child task done", zap.Any("balance request", lbt.LoadBalanceRequest))

	return nil
}

func (lbt *loadBalanceTask) getReplica(nodeID, collectionID int64) (*milvuspb.ReplicaInfo, error) {
	replicas, err := lbt.meta.getReplicasByNodeID(nodeID)
	if err != nil {
		return nil, err
	}
	for _, replica := range replicas {
		if replica.GetCollectionID() == collectionID {
			return replica, nil
		}
	}
	return nil, fmt.Errorf("unable to find replicas of collection %d and node %d", collectionID, nodeID)
}

func (lbt *loadBalanceTask) postExecute(context.Context) error {
	if lbt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lbt.clearChildTasks()
	}

	log.Info("loadBalanceTask postExecute done",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) globalPostExecute(ctx context.Context) error {
	if lbt.BalanceReason != querypb.TriggerCondition_NodeDown {
		return nil
	}

	replicas := make(map[UniqueID]*milvuspb.ReplicaInfo)
	segments := make(map[UniqueID]*querypb.SegmentInfo)
	dmChannels := make(map[string]*querypb.DmChannelWatchInfo)

	for _, id := range lbt.SourceNodeIDs {
		for _, segment := range lbt.meta.getSegmentInfosByNode(id) {
			segments[segment.SegmentID] = segment
		}
		for _, dmChannel := range lbt.meta.getDmChannelInfosByNodeID(id) {
			dmChannels[dmChannel.DmChannel] = dmChannel
		}

		nodeReplicas, err := lbt.meta.getReplicasByNodeID(id)
		if err != nil {
			log.Warn("failed to get replicas for removing offline querynode from it",
				zap.Int64("querynodeID", id),
				zap.Error(err))

			continue
		}
		for _, replica := range nodeReplicas {
			replicas[replica.ReplicaID] = replica
		}
	}

	log.Debug("removing offline nodes from replicas and segments...",
		zap.Int("replicaNum", len(replicas)),
		zap.Int("segmentNum", len(segments)),
		zap.Int64("triggerTaskID", lbt.getTaskID()),
	)

	wg := errgroup.Group{}
	// Remove offline nodes from replica
	for replicaID := range replicas {
		replicaID := replicaID
		wg.Go(func() error {
			return lbt.meta.applyReplicaBalancePlan(
				NewRemoveBalancePlan(replicaID, lbt.SourceNodeIDs...))
		})
	}

	// Remove offline nodes from dmChannels
	for _, dmChannel := range dmChannels {
		dmChannel := dmChannel
		wg.Go(func() error {
			dmChannel.NodeIds = removeFromSlice(dmChannel.NodeIds, lbt.SourceNodeIDs...)

			err := lbt.meta.setDmChannelInfos(dmChannel)
			if err != nil {
				log.Error("failed to remove offline nodes from dmChannel info",
					zap.String("dmChannel", dmChannel.DmChannel),
					zap.Error(err))

				return err
			}

			log.Info("remove offline nodes from dmChannel",
				zap.Int64("taskID", lbt.getTaskID()),
				zap.String("dmChannel", dmChannel.DmChannel),
				zap.Int64s("nodeIds", dmChannel.NodeIds))

			return nil
		})
	}

	// Update shard leaders for replicas
	for _, childTask := range lbt.getChildTask() {
		if task, ok := childTask.(*watchDmChannelTask); ok {
			wg.Go(func() error {
				leaderID := task.NodeID
				dmChannel := task.Infos[0].ChannelName

				nodeInfo, err := lbt.cluster.GetNodeInfoByID(leaderID)
				if err != nil {
					log.Error("failed to get node info to update shard leader info",
						zap.Int64("triggerTaskID", lbt.getTaskID()),
						zap.Int64("taskID", task.getTaskID()),
						zap.Int64("nodeID", leaderID),
						zap.String("dmChannel", dmChannel),
						zap.Error(err))
					return err
				}

				err = lbt.meta.updateShardLeader(task.ReplicaID, dmChannel, leaderID, nodeInfo.(*queryNode).address)
				if err != nil {
					log.Error("failed to update shard leader info of replica",
						zap.Int64("triggerTaskID", lbt.getTaskID()),
						zap.Int64("taskID", task.getTaskID()),
						zap.Int64("replicaID", task.ReplicaID),
						zap.String("dmChannel", dmChannel),
						zap.Error(err))
					return err
				}

				log.Debug("LoadBalance: update shard leader",
					zap.Int64("triggerTaskID", lbt.getTaskID()),
					zap.Int64("taskID", task.getTaskID()),
					zap.String("dmChannel", dmChannel),
					zap.Int64("leader", leaderID))

				return nil
			})
		}
	}

	err := wg.Wait()
	if err != nil {
		return err
	}

	for replicaID := range replicas {
		shards := make([]string, 0, len(dmChannels))
		for _, dmc := range dmChannels {
			shards = append(shards, dmc.DmChannel)
		}

		err := syncReplicaSegments(lbt.ctx, lbt.meta, lbt.cluster, replicaID, shards...)
		if err != nil {
			log.Error("loadBalanceTask: failed to sync segments distribution",
				zap.Int64("collectionID", lbt.CollectionID),
				zap.Int64("replicaID", lbt.replicaID),
				zap.Error(err))
			return err
		}
	}

	for _, offlineNodeID := range lbt.SourceNodeIDs {
		err := lbt.cluster.RemoveNodeInfo(offlineNodeID)
		if err != nil {
			log.Error("loadBalanceTask: occur error when removing node info from cluster",
				zap.Int64("nodeID", offlineNodeID),
				zap.Error(err))
			lbt.setResultInfo(err)
			return err
		}
	}

	return nil
}
