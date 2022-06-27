package querycoord

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

var _ task = (*handoffTask)(nil)

type handoffTask struct {
	*baseTask
	*querypb.HandoffSegmentsRequest
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
}

func (ht *handoffTask) msgBase() *commonpb.MsgBase {
	return ht.Base
}

func (ht *handoffTask) marshal() ([]byte, error) {
	return proto.Marshal(ht.HandoffSegmentsRequest)
}

func (ht *handoffTask) msgType() commonpb.MsgType {
	return ht.Base.MsgType
}

func (ht *handoffTask) timestamp() Timestamp {
	return ht.Base.Timestamp
}

func (ht *handoffTask) preExecute(context.Context) error {
	ht.setResultInfo(nil)
	segmentIDs := make([]UniqueID, 0)
	segmentInfos := ht.HandoffSegmentsRequest.SegmentInfos
	for _, info := range segmentInfos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	log.Info("start do handoff segments task",
		zap.Int64s("segmentIDs", segmentIDs))
	return nil
}

func (ht *handoffTask) execute(ctx context.Context) error {
	segmentInfos := ht.HandoffSegmentsRequest.SegmentInfos
	for _, segmentInfo := range segmentInfos {
		collectionID := segmentInfo.CollectionID
		partitionID := segmentInfo.PartitionID
		segmentID := segmentInfo.SegmentID

		collectionInfo, err := ht.meta.getCollectionInfoByID(collectionID)
		if err != nil {
			log.Warn("handoffTask: collection has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID))
			continue
		}

		if collectionInfo.LoadType == querypb.LoadType_LoadCollection && ht.meta.hasReleasePartition(collectionID, partitionID) {
			log.Warn("handoffTask: partition has been released", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
			continue
		}

		partitionLoaded := false
		for _, id := range collectionInfo.PartitionIDs {
			if id == partitionID {
				partitionLoaded = true
			}
		}

		if collectionInfo.LoadType != querypb.LoadType_LoadCollection && !partitionLoaded {
			log.Warn("handoffTask: partition has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("segmentID", segmentID))
			continue
		}

		//  segment which is compacted from should exist in query node
		for _, compactedSegID := range segmentInfo.CompactionFrom {
			_, err = ht.meta.getSegmentInfoByID(compactedSegID)
			if err != nil {
				log.Error("handoffTask: compacted segment has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("segmentID", segmentID))
				ht.setResultInfo(err)
				return err
			}
		}

		// segment which is compacted to should not exist in query node
		_, err = ht.meta.getSegmentInfoByID(segmentID)
		if err != nil {
			dmChannelInfos, binlogs, err := ht.broker.getRecoveryInfo(ht.ctx, collectionID, partitionID)
			if err != nil {
				log.Error("handoffTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
				ht.setResultInfo(err)
				return err
			}

			findBinlog := false
			var loadSegmentReq *querypb.LoadSegmentsRequest
			var watchDeltaChannels []*datapb.VchannelInfo
			for _, segmentBinlog := range binlogs {
				if segmentBinlog.SegmentID == segmentID {
					findBinlog = true
					segmentLoadInfo := ht.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBinlog, false, nil)
					segmentLoadInfo.CompactionFrom = segmentInfo.CompactionFrom
					segmentLoadInfo.IndexInfos = segmentInfo.IndexInfos
					msgBase := proto.Clone(ht.Base).(*commonpb.MsgBase)
					msgBase.MsgType = commonpb.MsgType_LoadSegments
					loadSegmentReq = &querypb.LoadSegmentsRequest{
						Base:         msgBase,
						Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
						Schema:       collectionInfo.Schema,
						CollectionID: collectionID,
						LoadMeta: &querypb.LoadMetaInfo{
							CollectionID: collectionID,
							PartitionIDs: collectionInfo.PartitionIDs,
						},
					}
				}
			}
			for _, info := range dmChannelInfos {
				deltaChannel, err := generateWatchDeltaChannelInfo(info)
				if err != nil {
					return err
				}
				watchDeltaChannels = append(watchDeltaChannels, deltaChannel)
			}

			if !findBinlog {
				err = fmt.Errorf("segmnet has not been flushed, segmentID is %d", segmentID)
				ht.setResultInfo(err)
				return err
			}
			mergedDeltaChannels := mergeWatchDeltaChannelInfo(watchDeltaChannels)
			// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
			err = ht.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
			if err != nil {
				log.Error("handoffTask: set delta channel info to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Error(err))
				ht.setResultInfo(err)
				return err
			}
			replicas, err := ht.meta.getReplicasByCollectionID(collectionID)
			if err != nil {
				ht.setResultInfo(err)
				return err
			}
			var internalTasks []task
			for _, replica := range replicas {
				if len(replica.NodeIds) == 0 {
					log.Warn("handoffTask: find empty replica", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Int64("replicaID", replica.GetReplicaID()))
					err := fmt.Errorf("replica %d of collection %d is empty", replica.GetReplicaID(), collectionID)
					ht.setResultInfo(err)
					return err
				}
				// we should copy a request because assignInternalTask will change DstNodeID of LoadSegmentRequest
				clonedReq := proto.Clone(loadSegmentReq).(*querypb.LoadSegmentsRequest)
				clonedReq.ReplicaID = replica.ReplicaID
				tasks, err := assignInternalTask(ctx, ht, ht.meta, ht.cluster, []*querypb.LoadSegmentsRequest{clonedReq}, nil, true, nil, nil, replica.GetReplicaID(), ht.broker)
				if err != nil {
					log.Error("handoffTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Error(err))
					ht.setResultInfo(err)
					return err
				}
				internalTasks = append(internalTasks, tasks...)
			}
			for _, internalTask := range internalTasks {
				ht.addChildTask(internalTask)
				log.Info("handoffTask: add a childTask", zap.String("task type", internalTask.msgType().String()), zap.Int64("segmentID", segmentID))
			}
		} else {
			err = fmt.Errorf("sealed segment has been exist on query node, segmentID is %d", segmentID)
			log.Error("handoffTask: handoff segment failed", zap.Int64("segmentID", segmentID), zap.Error(err))
			ht.setResultInfo(err)
			return err
		}
	}

	log.Info("handoffTask: assign child task done", zap.Any("segmentInfos", segmentInfos), zap.Int64("taskID", ht.getTaskID()))
	return nil
}

func (ht *handoffTask) postExecute(context.Context) error {
	if ht.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		ht.clearChildTasks()
	}

	log.Info("handoffTask postExecute done", zap.Int64("taskID", ht.getTaskID()))
	return nil
}

func (ht *handoffTask) rollBack(ctx context.Context) []task {
	resultTasks := make([]task, 0)
	childTasks := ht.getChildTask()
	for _, childTask := range childTasks {
		if childTask.msgType() == commonpb.MsgType_LoadSegments {
			// TODO:: add release segment to rollBack, no release does not affect correctness of query
		}
	}

	return resultTasks
}
