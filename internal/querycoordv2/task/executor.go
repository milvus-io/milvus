package task

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

const (
	actionTimeout = 10 * time.Second
)

type actionIndex struct {
	Task int64
	Step int
}

type Executor struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	broker    meta.Broker
	targetMgr *meta.TargetManager
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	executingActions sync.Map
}

func NewExecutor(meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	targetMgr *meta.TargetManager,
	cluster session.Cluster,
	nodeMgr *session.NodeManager) *Executor {
	return &Executor{
		meta:      meta,
		dist:      dist,
		broker:    broker,
		targetMgr: targetMgr,
		cluster:   cluster,
		nodeMgr:   nodeMgr,

		executingActions: sync.Map{},
	}
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int, action Action) bool {
	index := actionIndex{
		Task: task.ID(),
		Step: step,
	}
	_, exist := ex.executingActions.LoadOrStore(index, struct{}{})
	if exist {
		return false
	}

	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int("step", step),
		zap.Int64("source", task.SourceID()),
	)

	go func() {
		log.Info("execute the action of task")
		switch action := action.(type) {
		case *SegmentAction:
			ex.executeSegmentAction(task.(*SegmentTask), action)

		case *ChannelAction:
			ex.executeDmChannelAction(task.(*ChannelTask), action)
		}

		ex.executingActions.Delete(index)
	}()

	return true
}

func (ex *Executor) executeSegmentAction(task *SegmentTask, action *SegmentAction) {
	switch action.Type() {
	case ActionTypeGrow:
		ex.loadSegment(task, action)

	case ActionTypeReduce:
		ex.releaseSegment(task, action)
	}
}

func (ex *Executor) loadSegment(task *SegmentTask, action *SegmentAction) {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("collection", task.CollectionID()),
		zap.Int64("segment", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	schema, err := ex.broker.GetCollectionSchema(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get schema of collection", zap.Error(err))
		return
	}
	partitions, err := utils.GetPartitions(ex.meta.CollectionManager, ex.broker, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return
	}
	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		task.CollectionID(),
		partitions...,
	)
	segments, err := ex.broker.GetSegmentInfo(ctx, task.SegmentID())
	if err != nil || len(segments) == 0 {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return
	}
	segment := segments[0]
	indexes, err := ex.broker.GetIndexInfo(ctx, task.CollectionID(), segment.GetID())
	if err != nil {
		log.Warn("failed to get index of segment, will load without index")
	}
	loadInfo := utils.PackSegmentLoadInfo(segment, indexes)

	// Get shard leader for the given replica and segment
	leader, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), segment.GetInsertChannel())
	if !ok {
		msg := "no shard leader for the segment to execute loading"
		task.SetErr(utils.WrapError(msg, ErrTaskStale))
		log.Warn(msg, zap.String("shard", segment.GetInsertChannel()))
		return
	}
	log = log.With(zap.Int64("shardLeader", leader))

	deltaPositions, err := getSegmentDeltaPositions(ctx, ex.targetMgr, ex.broker, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetInsertChannel())
	if err != nil {
		log.Warn("failed to get delta positions of segment")
		return
	}

	req := packLoadSegmentRequest(task, action, schema, loadMeta, loadInfo, deltaPositions)
	status, err := ex.cluster.LoadSegments(ctx, leader, req)
	if err != nil {
		log.Warn("failed to load segment, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to load segment", zap.String("reason", status.GetReason()))
		return
	}
}

func (ex *Executor) releaseSegment(task *SegmentTask, action *SegmentAction) {
	defer action.isReleaseCommitted.Store(true)

	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("collection", task.CollectionID()),
		zap.Int64("segment", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	var targetSegment *meta.Segment
	segments := ex.dist.SegmentDistManager.GetByNode(action.Node())
	for _, segment := range segments {
		if segment.ID == task.SegmentID() {
			targetSegment = segment
			break
		}
	}
	if targetSegment == nil {
		log.Info("segment to release not found in distribution")
		return
	}

	req := packReleaseSegmentRequest(task, action, targetSegment.GetInsertChannel())

	// Get shard leader for the given replica and segment
	dstNode := action.Node()
	if ex.meta.CollectionManager.Exist(task.CollectionID()) {
		leader, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), targetSegment.GetInsertChannel())
		if !ok {
			log.Warn("no shard leader for the segment to execute loading", zap.String("shard", targetSegment.GetInsertChannel()))
			return
		}
		dstNode = leader
		log = log.With(zap.Int64("shardLeader", leader))
		req.NeedTransfer = true
	}
	status, err := ex.cluster.ReleaseSegments(ctx, dstNode, req)
	if err != nil {
		log.Warn("failed to release segment, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to release segment", zap.String("reason", status.GetReason()))
		return
	}
}

func (ex *Executor) executeDmChannelAction(task *ChannelTask, action *ChannelAction) {
	switch action.Type() {
	case ActionTypeGrow:
		ex.subDmChannel(task, action)

	case ActionTypeReduce:
		ex.unsubDmChannel(task, action)
	}
}

func (ex *Executor) subDmChannel(task *ChannelTask, action *ChannelAction) {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("collection", task.CollectionID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	schema, err := ex.broker.GetCollectionSchema(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get schema of collection")
		return
	}
	partitions, err := utils.GetPartitions(ex.meta.CollectionManager, ex.broker, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection")
		return
	}
	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		task.CollectionID(),
		partitions...,
	)
	// DO NOT fetch channel info from DataCoord here,
	// that may lead to leaking some data
	// channels := make([]*datapb.VchannelInfo, 0, len(partitions))
	// for _, partition := range partitions {
	// 	vchannels, _, err := ex.broker.GetRecoveryInfo(ctx, task.CollectionID(), partition)
	// 	if err != nil {
	// 		log.Warn("failed to get vchannel from DataCoord", zap.Error(err))
	// 		return
	// 	}

	// 	for _, channel := range vchannels {
	// 		if channel.ChannelName == action.ChannelName() {
	// 			channels = append(channels, channel)
	// 		}
	// 	}
	// }
	// if len(channels) == 0 {
	// 	log.Warn("no such channel in DataCoord")
	// 	return
	// }

	// dmChannel := utils.MergeDmChannelInfo(channels)
	dmChannel := ex.targetMgr.GetDmChannel(action.ChannelName())
	req := packSubDmChannelRequest(task, action, schema, loadMeta, dmChannel)
	err = fillSubDmChannelRequest(ctx, req, ex.broker)
	if err != nil {
		log.Warn("failed to subscribe DmChannel, failed to fill the request with segments",
			zap.Error(err))
		return
	}
	status, err := ex.cluster.WatchDmChannels(ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to subscribe DmChannel, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to subscribe DmChannel", zap.String("reason", status.GetReason()))
		return
	}
	log.Info("subscribe DmChannel done")
}

func (ex *Executor) unsubDmChannel(task *ChannelTask, action *ChannelAction) {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("collection", task.CollectionID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	ctx, cancel := context.WithTimeout(task.Context(), actionTimeout)
	defer cancel()

	req := packUnsubDmChannelRequest(task, action)
	status, err := ex.cluster.UnsubDmChannel(ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to unsubscribe DmChannel, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to unsubscribe DmChannel", zap.String("reason", status.GetReason()))
		return
	}
}
