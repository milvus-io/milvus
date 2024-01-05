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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Executor struct {
	doneCh    chan struct{}
	wg        sync.WaitGroup
	meta      *meta.Meta
	dist      *meta.DistributionManager
	broker    meta.Broker
	targetMgr *meta.TargetManager
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	executingTasks   *typeutil.ConcurrentSet[string] // task index
	executingTaskNum atomic.Int32
}

func NewExecutor(meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	targetMgr *meta.TargetManager,
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
		}
	}()

	return true
}

func (ex *Executor) removeTask(task Task, step int) {
	if task.Err() != nil {
		log.Info("execute action done, remove it",
			zap.Int64("taskID", task.ID()),
			zap.Int("step", step),
			zap.Error(task.Err()))
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

	collectionInfo, err := ex.broker.DescribeCollection(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get collection info", zap.Error(err))
		return err
	}
	partitions, err := utils.GetPartitions(ex.meta.CollectionManager, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return err
	}

	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		task.CollectionID(),
		partitions...,
	)
	// get channel first, in case of target updated after segment info fetched
	channel := ex.targetMgr.GetDmChannel(task.CollectionID(), task.shard, meta.NextTargetFirst)
	if channel == nil {
		return merr.WrapErrChannelNotAvailable(task.shard)
	}

	resp, err := ex.broker.GetSegmentInfo(ctx, task.SegmentID())
	if err != nil || len(resp.GetInfos()) == 0 {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return err
	}
	segment := resp.GetInfos()[0]

	indexes, err := ex.broker.GetIndexInfo(ctx, task.CollectionID(), segment.GetID())
	if err != nil {
		if !errors.Is(err, merr.ErrIndexNotFound) {
			log.Warn("failed to get index of segment", zap.Error(err))
			return err
		}
		indexes = nil
	}

	// Get collection index info
	indexInfos, err := ex.broker.DescribeIndex(ctx, task.CollectionID())
	if err != nil {
		log.Warn("fail to get index meta of collection")
		return err
	}
	// update the field index params
	for _, segmentIndex := range indexes {
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

	loadInfo := utils.PackSegmentLoadInfo(resp.GetInfos()[0], channel.GetSeekPosition(), indexes)

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfo.GetSchema(),
		collectionInfo.GetProperties(),
		loadMeta,
		loadInfo,
		indexInfos,
	)

	// Get shard leader for the given replica and segment
	leaderID, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), segment.GetInsertChannel())
	if !ok {
		msg := "no shard leader for the segment to execute loading"
		err = merr.WrapErrChannelNotFound(segment.GetInsertChannel(), "shard delegator not found")
		log.Warn(msg, zap.Error(err))
		return err
	}
	log = log.With(zap.Int64("shardLeader", leaderID))

	startTs := time.Now()
	log.Info("load segments...")
	status, err := ex.cluster.LoadSegments(task.Context(), leaderID, req)
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
	if action.Scope() == querypb.DataScope_Streaming {
		// Any modification to the segment distribution have to set NeedTransfer true,
		// to protect the version, which serves search/query
		req.NeedTransfer = true
	} else {
		var targetSegment *meta.Segment
		segments := ex.dist.SegmentDistManager.GetByNode(action.Node())
		for _, segment := range segments {
			if segment.GetID() == task.SegmentID() {
				targetSegment = segment
				break
			}
		}
		if targetSegment == nil {
			log.Info("segment to release not found in distribution")
			return
		}
		req.Shard = targetSegment.GetInsertChannel()

		if ex.meta.CollectionManager.Exist(task.CollectionID()) {
			leader, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), req.GetShard())
			if !ok {
				log.Warn("no shard leader for the segment to execute releasing", zap.String("shard", req.GetShard()))
				return
			}
			dstNode = leader
			log = log.With(zap.Int64("shardLeader", leader))
			req.NeedTransfer = true
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
		log.Warn("failed to get collection info")
		return err
	}
	partitions, err := utils.GetPartitions(ex.meta.CollectionManager, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection")
		return err
	}
	indexInfo, err := ex.broker.DescribeIndex(ctx, task.CollectionID())
	if err != nil {
		log.Warn("fail to get index meta of collection")
		return err
	}
	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		task.CollectionID(),
		partitions...,
	)

	dmChannel := ex.targetMgr.GetDmChannel(task.CollectionID(), action.ChannelName(), meta.NextTarget)
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
	err = fillSubChannelRequest(ctx, req, ex.broker)
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
