// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// segmentsVersion is used for the flushed segments should not be included in the watch dm channel request
var segmentsVersion = semver.Version{
	Major: 2,
	Minor: 3,
	Patch: 4,
}

type Executor struct {
	doneCh    chan struct{}
	wg        sync.WaitGroup
	meta      *meta.Meta
	dist      *meta.DistributionManager
	broker    meta.Broker
	targetMgr meta.TargetManagerInterface
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	executingTasks   *typeutil.ConcurrentSet[string] // task index
	executingTaskNum atomic.Int32
	executedFlag     chan struct{}
}

func NewExecutor(meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	cluster session.Cluster,
	nodeMgr *session.NodeManager,
) *Executor {
	return &Executor{
		doneCh:    make(chan struct{}),
		meta:      meta,
		dist:      dist,
		broker:    broker,
		targetMgr: targetMgr,
		cluster:   cluster,
		nodeMgr:   nodeMgr,

		executingTasks: typeutil.NewConcurrentSet[string](),
		executedFlag:   make(chan struct{}, 1),
	}
}

func (ex *Executor) Start(ctx context.Context) {
}

func (ex *Executor) Stop() {
	ex.wg.Wait()
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int) bool {
	exist := !ex.executingTasks.Insert(task.Index())
	if exist {
		return false
	}
	if ex.executingTaskNum.Inc() > Params.QueryCoordCfg.TaskExecutionCap.GetAsInt32() {
		ex.executingTasks.Remove(task.Index())
		ex.executingTaskNum.Dec()
		return false
	}

	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int("step", step),
		zap.String("source", task.Source().String()),
	)

	go func() {
		log.Info("execute the action of task")
		switch task.Actions()[step].(type) {
		case *SegmentAction:
			ex.executeSegmentAction(task.(*SegmentTask), step)

		case *ChannelAction:
			ex.executeDmChannelAction(task.(*ChannelTask), step)

		case *LeaderAction:
			ex.executeLeaderAction(task.(*LeaderTask), step)
		}
	}()

	return true
}

func (ex *Executor) GetExecutedFlag() <-chan struct{} {
	return ex.executedFlag
}

func (ex *Executor) removeTask(task Task, step int) {
	if task.Err() != nil {
		log.Info("execute action done, remove it",
			zap.Int64("taskID", task.ID()),
			zap.Int("step", step),
			zap.Error(task.Err()))
	} else {
		select {
		case ex.executedFlag <- struct{}{}:
		default:
		}
	}

	ex.executingTasks.Remove(task.Index())
	ex.executingTaskNum.Dec()
}

func (ex *Executor) executeSegmentAction(task *SegmentTask, step int) {
	switch task.Actions()[step].Type() {
	case ActionTypeGrow, ActionTypeUpdate:
		ex.loadSegment(task, step)

	case ActionTypeReduce:
		ex.releaseSegment(task, step)
	}
}

// loadSegment commits the request to merger,
// not really executes the request
func (ex *Executor) loadSegment(task *SegmentTask, step int) error {
	action := task.Actions()[step].(*SegmentAction)
	defer action.rpcReturned.Store(true)
	ctx := task.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
		ex.removeTask(task, step)
	}()

	collectionInfo, loadMeta, channel, err := ex.getMetaInfo(ctx, task)
	if err != nil {
		return err
	}

	loadInfo, indexInfos, err := ex.getLoadInfo(ctx, task.CollectionID(), action.SegmentID, channel)
	if err != nil {
		return err
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfo.GetSchema(),
		collectionInfo.GetProperties(),
		loadMeta,
		loadInfo,
		indexInfos,
	)

	// get segment's replica first, then get shard leader by replica
	replica := ex.meta.ReplicaManager.GetByCollectionAndNode(ctx, task.CollectionID(), action.Node())
	if replica == nil {
		msg := "node doesn't belong to any replica"
		err := merr.WrapErrNodeNotAvailable(action.Node())
		log.Warn(msg, zap.Error(err))
		return err
	}
	view := ex.dist.LeaderViewManager.GetLatestShardLeaderByFilter(meta.WithReplica2LeaderView(replica), meta.WithChannelName2LeaderView(action.Shard))
	if view == nil {
		msg := "no shard leader for the segment to execute loading"
		err = merr.WrapErrChannelNotFound(task.Shard(), "shard delegator not found")
		log.Warn(msg, zap.Error(err))
		return err
	}
	log = log.With(zap.Int64("shardLeader", view.ID))

	startTs := time.Now()
	log.Info("load segments...")
	status, err := ex.cluster.LoadSegments(task.Context(), view.ID, req)
	err = merr.CheckRPCCall(status, err)
	if err != nil {
		log.Warn("failed to load segment", zap.Error(err))
		return err
	}

	elapsed := time.Since(startTs)
	log.Info("load segments done", zap.Duration("elapsed", elapsed))

	return nil
}

func (ex *Executor) releaseSegment(task *SegmentTask, step int) {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*SegmentAction)
	defer action.rpcReturned.Store(true)

	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	ctx := task.Context()

	dstNode := action.Node()

	req := packReleaseSegmentRequest(task, action)
	channel := ex.targetMgr.GetDmChannel(ctx, task.CollectionID(), task.Shard(), meta.CurrentTarget)
	if channel != nil {
		// if channel exists in current target, set cp to ReleaseSegmentRequest, need to use it as growing segment's exclude ts
		req.Checkpoint = channel.GetSeekPosition()
	}

	if action.Scope == querypb.DataScope_Streaming {
		// Any modification to the segment distribution have to set NeedTransfer true,
		// to protect the version, which serves search/query
		req.NeedTransfer = true
	} else {
		req.Shard = task.shard

		if ex.meta.CollectionManager.Exist(ctx, task.CollectionID()) {
			// get segment's replica first, then get shard leader by replica
			replica := ex.meta.ReplicaManager.GetByCollectionAndNode(ctx, task.CollectionID(), action.Node())
			if replica == nil {
				msg := "node doesn't belong to any replica, try to send release to worker"
				err := merr.WrapErrNodeNotAvailable(action.Node())
				log.Warn(msg, zap.Error(err))
				dstNode = action.Node()
				req.NeedTransfer = false
			} else {
				view := ex.dist.LeaderViewManager.GetLatestShardLeaderByFilter(meta.WithReplica2LeaderView(replica), meta.WithChannelName2LeaderView(action.Shard))
				if view == nil {
					msg := "no shard leader for the segment to execute releasing"
					err := merr.WrapErrChannelNotFound(task.Shard(), "shard delegator not found")
					log.Warn(msg, zap.Error(err))
					return
				}
				dstNode = view.ID
				log = log.With(zap.Int64("shardLeader", view.ID))
				req.NeedTransfer = true
			}
		}
	}

	log.Info("release segment...")
	status, err := ex.cluster.ReleaseSegments(ctx, dstNode, req)
	err = merr.CheckRPCCall(status, err)
	if err != nil {
		log.Warn("failed to release segment", zap.Error(err))
		return
	}
	elapsed := time.Since(startTs)
	log.Info("release segment done", zap.Int64("taskID", task.ID()), zap.Duration("time taken", elapsed))
}

func (ex *Executor) executeDmChannelAction(task *ChannelTask, step int) {
	switch task.Actions()[step].Type() {
	case ActionTypeGrow:
		ex.subscribeChannel(task, step)

	case ActionTypeReduce:
		ex.unsubscribeChannel(task, step)
	}
}

func (ex *Executor) subscribeChannel(task *ChannelTask, step int) error {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*ChannelAction)
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
	}()

	ctx := task.Context()

	collectionInfo, err := ex.broker.DescribeCollection(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get collection info", zap.Error(err))
		return err
	}
	loadFields := ex.meta.GetLoadFields(ctx, task.CollectionID())
	partitions, err := utils.GetPartitions(ctx, ex.targetMgr, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return err
	}
	indexInfo, err := ex.broker.ListIndexes(ctx, task.CollectionID())
	if err != nil {
		log.Warn("fail to get index meta of collection", zap.Error(err))
		return err
	}
	dbResp, err := ex.broker.DescribeDatabase(ctx, collectionInfo.GetDbName())
	if err != nil {
		log.Warn("failed to get database info", zap.Error(err))
		return err
	}
	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(ctx, task.CollectionID()),
		task.CollectionID(),
		collectionInfo.GetDbName(),
		task.ResourceGroup(),
		loadFields,
		partitions...,
	)
	loadMeta.DbProperties = dbResp.GetProperties()

	dmChannel := ex.targetMgr.GetDmChannel(ctx, task.CollectionID(), action.ChannelName(), meta.NextTarget)
	if dmChannel == nil {
		msg := "channel does not exist in next target, skip it"
		log.Warn(msg, zap.String("channelName", action.ChannelName()))
		return merr.WrapErrChannelReduplicate(action.ChannelName())
	}
	req := packSubChannelRequest(
		task,
		action,
		collectionInfo.GetSchema(),
		loadMeta,
		dmChannel,
		indexInfo,
	)
	err = fillSubChannelRequest(ctx, req, ex.broker, ex.shouldIncludeFlushedSegmentInfo(action.Node()))
	if err != nil {
		log.Warn("failed to subscribe channel, failed to fill the request with segments",
			zap.Error(err))
		return err
	}

	ts := dmChannel.GetSeekPosition().GetTimestamp()
	log.Info("subscribe channel...",
		zap.Uint64("checkpoint", ts),
		zap.Duration("sinceCheckpoint", time.Since(tsoutil.PhysicalTime(ts))),
	)
	status, err := ex.cluster.WatchDmChannels(ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to subscribe channel, it may be a false failure", zap.Error(err))
		return err
	}
	if !merr.Ok(status) {
		err = merr.Error(status)
		log.Warn("failed to subscribe channel", zap.Error(err))
		return err
	}
	elapsed := time.Since(startTs)
	log.Info("subscribe channel done", zap.Int64("taskID", task.ID()), zap.Duration("time taken", elapsed))
	return nil
}

func (ex *Executor) shouldIncludeFlushedSegmentInfo(nodeID int64) bool {
	node := ex.nodeMgr.Get(nodeID)
	if node == nil {
		return false
	}
	return node.Version().LT(segmentsVersion)
}

func (ex *Executor) unsubscribeChannel(task *ChannelTask, step int) error {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*ChannelAction)
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
	}()

	ctx := task.Context()

	req := packUnsubDmChannelRequest(task, action)
	log.Info("unsubscribe channel...")
	status, err := ex.cluster.UnsubDmChannel(ctx, action.Node(), req)
	if err != nil {
		log.Warn("failed to unsubscribe channel, it may be a false failure", zap.Error(err))
		return err
	}
	if !merr.Ok(status) {
		err = merr.Error(status)
		log.Warn("failed to unsubscribe channel", zap.Error(err))
		return err
	}

	elapsed := time.Since(startTs)
	log.Info("unsubscribe channel done", zap.Int64("taskID", task.ID()), zap.Duration("time taken", elapsed))
	return nil
}

func (ex *Executor) executeLeaderAction(task *LeaderTask, step int) {
	switch task.Actions()[step].Type() {
	case ActionTypeGrow:
		ex.setDistribution(task, step)

	case ActionTypeReduce:
		ex.removeDistribution(task, step)

	case ActionTypeUpdate:
		ex.updatePartStatsVersions(task, step)
	}
}

func (ex *Executor) updatePartStatsVersions(task *LeaderTask, step int) error {
	action := task.Actions()[step].(*LeaderAction)
	defer action.rpcReturned.Store(true)
	ctx := task.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("leader", action.leaderID),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)
	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
		ex.removeTask(task, step)
	}()

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
			commonpbutil.WithMsgID(task.ID()),
		),
		CollectionID: task.collectionID,
		Channel:      task.Shard(),
		ReplicaID:    task.ReplicaID(),
		Actions: []*querypb.SyncAction{
			{
				Type:                   querypb.SyncType_UpdatePartitionStats,
				SegmentID:              action.SegmentID(),
				NodeID:                 action.Node(),
				Version:                action.Version(),
				PartitionStatsVersions: action.partStatsVersions,
			},
		},
	}
	startTs := time.Now()
	log.Debug("Update partition stats versions...")
	status, err := ex.cluster.SyncDistribution(task.Context(), task.leaderID, req)
	err = merr.CheckRPCCall(status, err)
	if err != nil {
		log.Warn("failed to update partition stats versions", zap.Error(err))
		return err
	}

	elapsed := time.Since(startTs)
	log.Debug("update partition stats done", zap.Duration("elapsed", elapsed))

	return nil
}

func (ex *Executor) setDistribution(task *LeaderTask, step int) error {
	action := task.Actions()[step].(*LeaderAction)
	defer action.rpcReturned.Store(true)
	ctx := task.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("leader", action.leaderID),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
		ex.removeTask(task, step)
	}()

	collectionInfo, loadMeta, channel, err := ex.getMetaInfo(ctx, task)
	if err != nil {
		return err
	}

	loadInfo, indexInfo, err := ex.getLoadInfo(ctx, task.CollectionID(), action.SegmentID(), channel)
	if err != nil {
		return err
	}

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
			commonpbutil.WithMsgID(task.ID()),
		),
		CollectionID: task.collectionID,
		Channel:      task.Shard(),
		Schema:       collectionInfo.GetSchema(),
		LoadMeta:     loadMeta,
		ReplicaID:    task.ReplicaID(),
		Actions: []*querypb.SyncAction{
			{
				Type:        querypb.SyncType_Set,
				PartitionID: loadInfo.GetPartitionID(),
				SegmentID:   action.SegmentID(),
				NodeID:      action.Node(),
				Info:        loadInfo,
				Version:     action.Version(),
			},
		},
		IndexInfoList: indexInfo,
	}

	startTs := time.Now()
	log.Info("Sync Distribution...")
	status, err := ex.cluster.SyncDistribution(task.Context(), task.leaderID, req)
	err = merr.CheckRPCCall(status, err)
	if err != nil {
		log.Warn("failed to sync distribution", zap.Error(err))
		return err
	}

	elapsed := time.Since(startTs)
	log.Info("sync distribution done", zap.Duration("elapsed", elapsed))

	return nil
}

func (ex *Executor) removeDistribution(task *LeaderTask, step int) error {
	action := task.Actions()[step].(*LeaderAction)
	defer action.rpcReturned.Store(true)
	ctx := task.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("leader", action.leaderID),
		zap.Int64("node", action.Node()),
		zap.String("source", task.Source().String()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Fail(err)
		}
		ex.removeTask(task, step)
	}()

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
			commonpbutil.WithMsgID(task.ID()),
		),
		CollectionID: task.collectionID,
		Channel:      task.Shard(),
		ReplicaID:    task.ReplicaID(),
		Actions: []*querypb.SyncAction{
			{
				Type:      querypb.SyncType_Remove,
				SegmentID: action.SegmentID(),
				NodeID:    action.Node(),
			},
		},
	}

	startTs := time.Now()
	log.Info("Remove Distribution...")
	status, err := ex.cluster.SyncDistribution(task.Context(), task.leaderID, req)
	err = merr.CheckRPCCall(status, err)
	if err != nil {
		log.Warn("failed to remove distribution", zap.Error(err))
		return err
	}

	elapsed := time.Since(startTs)
	log.Info("remove distribution done", zap.Duration("elapsed", elapsed))

	return nil
}

func (ex *Executor) getMetaInfo(ctx context.Context, task Task) (*milvuspb.DescribeCollectionResponse, *querypb.LoadMetaInfo, *meta.DmChannel, error) {
	collectionID := task.CollectionID()
	shard := task.Shard()
	log := log.Ctx(ctx)
	collectionInfo, err := ex.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		log.Warn("failed to get collection info", zap.Error(err))
		return nil, nil, nil, err
	}
	loadFields := ex.meta.GetLoadFields(ctx, task.CollectionID())
	partitions, err := utils.GetPartitions(ctx, ex.targetMgr, collectionID)
	if err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return nil, nil, nil, err
	}

	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(ctx, task.CollectionID()),
		task.CollectionID(),
		collectionInfo.GetDbName(),
		task.ResourceGroup(),
		loadFields,
		partitions...,
	)

	// get channel first, in case of target updated after segment info fetched
	channel := ex.targetMgr.GetDmChannel(ctx, collectionID, shard, meta.NextTargetFirst)
	if channel == nil {
		return nil, nil, nil, merr.WrapErrChannelNotAvailable(shard)
	}

	return collectionInfo, loadMeta, channel, nil
}

func (ex *Executor) getLoadInfo(ctx context.Context, collectionID, segmentID int64, channel *meta.DmChannel) (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	log := log.Ctx(ctx)
	segmentInfos, err := ex.broker.GetSegmentInfo(ctx, segmentID)
	if err != nil || len(segmentInfos) == 0 {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return nil, nil, err
	}
	segment := segmentInfos[0]
	log = log.With(zap.String("level", segment.GetLevel().String()))

	indexes, err := ex.broker.GetIndexInfo(ctx, collectionID, segment.GetID())
	if err != nil {
		if !errors.Is(err, merr.ErrIndexNotFound) {
			log.Warn("failed to get index of segment", zap.Error(err))
			return nil, nil, err
		}
		indexes = nil
	}

	// Get collection index info
	indexInfos, err := ex.broker.ListIndexes(ctx, collectionID)
	if err != nil {
		log.Warn("fail to get index meta of collection", zap.Error(err))
		return nil, nil, err
	}
	// update the field index params
	for _, segmentIndex := range indexes[segment.GetID()] {
		index, found := lo.Find(indexInfos, func(indexInfo *indexpb.IndexInfo) bool {
			return indexInfo.IndexID == segmentIndex.IndexID
		})
		if !found {
			log.Warn("no collection index info for the given segment index", zap.String("indexName", segmentIndex.GetIndexName()))
		}

		params := funcutil.KeyValuePair2Map(segmentIndex.GetIndexParams())
		for _, kv := range index.GetUserIndexParams() {
			if indexparams.IsConfigableIndexParam(kv.GetKey()) {
				params[kv.GetKey()] = kv.GetValue()
			}
		}
		segmentIndex.IndexParams = funcutil.Map2KeyValuePair(params)
	}

	loadInfo := utils.PackSegmentLoadInfo(segment, channel.GetSeekPosition(), indexes[segment.GetID()])
	return loadInfo, indexInfos, nil
}
