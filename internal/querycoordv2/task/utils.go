package task

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
)

func Wait(ctx context.Context, timeout time.Duration, tasks ...Task) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	go func() {
		for _, task := range tasks {
			err = task.Wait()
			if err != nil {
				cancel()
				break
			}
		}
		cancel()
	}()
	<-ctx.Done()

	return err
}

// GetTaskType returns the task's type,
// for now, only 3 types;
// - only 1 grow action -> Grow
// - only 1 reduce action -> Reduce
// - 1 grow action, and ends with 1 reduce action -> Move
func GetTaskType(task Task) Type {
	if len(task.Actions()) > 1 {
		return TaskTypeMove
	} else if task.Actions()[0].Type() == ActionTypeGrow {
		return TaskTypeGrow
	} else {
		return TaskTypeReduce
	}
}

func packLoadSegmentRequest(
	task *SegmentTask,
	action Action,
	schema *schemapb.CollectionSchema,
	loadMeta *querypb.LoadMetaInfo,
	loadInfo *querypb.SegmentLoadInfo,
	deltaPositions []*internalpb.MsgPosition,
) *querypb.LoadSegmentsRequest {
	return &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
			MsgID:   task.SourceID(),
		},
		Infos:          []*querypb.SegmentLoadInfo{loadInfo},
		Schema:         schema,
		LoadMeta:       loadMeta,
		CollectionID:   task.CollectionID(),
		ReplicaID:      task.ReplicaID(),
		DeltaPositions: deltaPositions,
		DstNodeID:      action.Node(),
		Version:        time.Now().UnixNano(),
		NeedTransfer:   true,
	}
}

func packReleaseSegmentRequest(task *SegmentTask, action *SegmentAction, shard string) *querypb.ReleaseSegmentsRequest {
	return &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseSegments,
			MsgID:   task.SourceID(),
		},

		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		SegmentIDs:   []int64{task.SegmentID()},
		Shard:        shard,
		Scope:        action.Scope(),
		NeedTransfer: false,
	}
}

func packLoadMeta(loadType querypb.LoadType, collectionID int64, partitions ...int64) *querypb.LoadMetaInfo {
	return &querypb.LoadMetaInfo{
		LoadType:     loadType,
		CollectionID: collectionID,
		PartitionIDs: partitions,
	}
}

func packSubDmChannelRequest(
	task *ChannelTask,
	action Action,
	schema *schemapb.CollectionSchema,
	loadMeta *querypb.LoadMetaInfo,
	channel *meta.DmChannel,
) *querypb.WatchDmChannelsRequest {
	return &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
			MsgID:   task.SourceID(),
		},
		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		Infos:        []*datapb.VchannelInfo{channel.VchannelInfo},
		Schema:       schema,
		LoadMeta:     loadMeta,
		ReplicaID:    task.ReplicaID(),
	}
}

func fillSubDmChannelRequest(
	ctx context.Context,
	req *querypb.WatchDmChannelsRequest,
	broker meta.Broker,
) error {
	segmentIDs := typeutil.NewUniqueSet()
	for _, vchannel := range req.GetInfos() {
		segmentIDs.Insert(vchannel.GetFlushedSegmentIds()...)
		segmentIDs.Insert(vchannel.GetUnflushedSegmentIds()...)
		segmentIDs.Insert(vchannel.GetDroppedSegmentIds()...)
	}

	if segmentIDs.Len() == 0 {
		return nil
	}

	resp, err := broker.GetSegmentInfo(ctx, segmentIDs.Collect()...)
	if err != nil {
		return err
	}
	segmentInfos := make(map[int64]*datapb.SegmentInfo)
	for _, info := range resp {
		segmentInfos[info.GetID()] = info
	}
	req.SegmentInfos = segmentInfos
	return nil
}

func packUnsubDmChannelRequest(task *ChannelTask, action Action) *querypb.UnsubDmChannelRequest {
	return &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_UnsubDmChannel,
			MsgID:   task.SourceID(),
		},
		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		ChannelName:  task.Channel(),
	}
}

func getShardLeader(replicaMgr *meta.ReplicaManager, distMgr *meta.DistributionManager, collectionID, nodeID int64, channel string) (int64, bool) {
	replica := replicaMgr.GetByCollectionAndNode(collectionID, nodeID)
	if replica == nil {
		return 0, false
	}
	return distMgr.GetShardLeader(replica, channel)
}

func getSegmentDeltaPositions(ctx context.Context, targetMgr *meta.TargetManager, broker meta.Broker, collectionID, partitionID int64, channel string) ([]*internalpb.MsgPosition, error) {
	deltaChannelName, err := funcutil.ConvertChannelName(channel, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		return nil, err
	}

	// vchannels, _, err := broker.GetRecoveryInfo(ctx, collectionID, partitionID)
	// if err != nil {
	// 	return nil, err
	// }

	deltaChannels := make([]*datapb.VchannelInfo, 0)
	for _, info := range targetMgr.GetDmChannelsByCollection(collectionID) {
		deltaChannelInfo, err := generatDeltaChannelInfo(info.VchannelInfo)
		if err != nil {
			return nil, err
		}
		if deltaChannelInfo.ChannelName == deltaChannelName {
			deltaChannels = append(deltaChannels, deltaChannelInfo)
		}
	}
	deltaChannels = mergeWatchDeltaChannelInfo(deltaChannels)

	return lo.Map(deltaChannels, func(channel *datapb.VchannelInfo, _ int) *internalpb.MsgPosition {
		return channel.GetSeekPosition()
	}), nil
}

func generatDeltaChannelInfo(info *datapb.VchannelInfo) (*datapb.VchannelInfo, error) {
	deltaChannelName, err := funcutil.ConvertChannelName(info.ChannelName, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		return nil, err
	}
	deltaChannel := proto.Clone(info).(*datapb.VchannelInfo)
	deltaChannel.ChannelName = deltaChannelName
	deltaChannel.UnflushedSegmentIds = nil
	deltaChannel.FlushedSegmentIds = nil
	deltaChannel.DroppedSegmentIds = nil
	return deltaChannel, nil
}

func mergeWatchDeltaChannelInfo(infos []*datapb.VchannelInfo) []*datapb.VchannelInfo {
	minPositions := make(map[string]int)
	for index, info := range infos {
		_, ok := minPositions[info.ChannelName]
		if !ok {
			minPositions[info.ChannelName] = index
		}
		minTimeStampIndex := minPositions[info.ChannelName]
		if info.SeekPosition.GetTimestamp() < infos[minTimeStampIndex].SeekPosition.GetTimestamp() {
			minPositions[info.ChannelName] = index
		}
	}
	var result []*datapb.VchannelInfo
	for _, index := range minPositions {
		result = append(result, infos[index])
	}

	return result
}
