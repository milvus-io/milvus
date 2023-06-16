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

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

	// Merge load segment requests
	merger *Merger[segmentIndex, *querypb.LoadSegmentsRequest]

	executingTasks   sync.Map
	executingTaskNum atomic.Int32
}

func NewExecutor(meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
	targetMgr *meta.TargetManager,
	cluster session.Cluster,
	nodeMgr *session.NodeManager) *Executor {
	return &Executor{
		doneCh:    make(chan struct{}),
		meta:      meta,
		dist:      dist,
		broker:    broker,
		targetMgr: targetMgr,
		cluster:   cluster,
		nodeMgr:   nodeMgr,
		merger:    NewMerger[segmentIndex, *querypb.LoadSegmentsRequest](),

		executingTasks: sync.Map{},
	}
}

func (ex *Executor) Start(ctx context.Context) {
	ex.merger.Start(ctx)
	ex.scheduleRequests()
}

func (ex *Executor) Stop() {
	ex.merger.Stop()
	ex.wg.Wait()
}

// Execute executes the given action,
// does nothing and returns false if the action is already committed,
// returns true otherwise.
func (ex *Executor) Execute(task Task, step int) bool {
	_, exist := ex.executingTasks.LoadOrStore(task.ID(), struct{}{})
	if exist {
		return false
	}
	if ex.executingTaskNum.Inc() > Params.QueryCoordCfg.TaskExecutionCap.GetAsInt32() {
		ex.executingTasks.Delete(task.ID())
		ex.executingTaskNum.Dec()
		return false
	}

	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int("step", step),
		zap.Int64("source", task.SourceID()),
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

func (ex *Executor) Exist(taskID int64) bool {
	_, ok := ex.executingTasks.Load(taskID)
	return ok
}

func (ex *Executor) scheduleRequests() {
	ex.wg.Add(1)
	go func() {
		defer ex.wg.Done()
		for mergeTask := range ex.merger.Chan() {
			task := mergeTask.(*LoadSegmentsTask)
			log.Info("get merge task, process it",
				zap.Int64("collectionID", task.req.GetCollectionID()),
				zap.Int64("replicaID", task.req.GetReplicaID()),
				zap.String("shard", task.req.GetInfos()[0].GetInsertChannel()),
				zap.Int64("nodeID", task.req.GetDstNodeID()),
				zap.Int("taskNum", len(task.tasks)),
			)
			go ex.processMergeTask(mergeTask.(*LoadSegmentsTask))
		}
	}()
}

func (ex *Executor) processMergeTask(mergeTask *LoadSegmentsTask) {
	startTs := time.Now()
	task := mergeTask.tasks[0]
	action := task.Actions()[mergeTask.steps[0]]

	var err error
	defer func() {
		if err != nil {
			for i := range mergeTask.tasks {
				mergeTask.tasks[i].Cancel(err)
			}
		}
		for i := range mergeTask.tasks {
			ex.removeTask(mergeTask.tasks[i], mergeTask.steps[i])
		}
	}()

	taskIDs := make([]int64, 0, len(mergeTask.tasks))
	segments := make([]int64, 0, len(mergeTask.tasks))
	for _, task := range mergeTask.tasks {
		taskIDs = append(taskIDs, task.ID())
		segments = append(segments, task.SegmentID())
	}
	log := log.With(
		zap.Int64s("taskIDs", taskIDs),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("shard", task.Shard()),
		zap.Int64s("segmentIDs", segments),
		zap.Int64("nodeID", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	// Get shard leader for the given replica and segment
	channel := mergeTask.req.GetInfos()[0].GetInsertChannel()
	leader, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), channel)
	if !ok {
		err = merr.WrapErrChannelNotFound(channel, "shard delegator not found")
		log.Warn("no shard leader for the segment to execute loading", zap.Error(task.Err()))
		return
	}

	log.Info("load segments...")
	status, err := ex.cluster.LoadSegments(task.Context(), leader, mergeTask.req)
	if err != nil {
		log.Warn("failed to load segment", zap.Error(err))
		return
	}
	if !merr.Ok(status) {
		err = merr.Error(status)
		log.Warn("failed to load segment", zap.Error(err))
		return
	}

	elapsed := time.Since(startTs)
	log.Info("load segments done", zap.Duration("elapsed", elapsed))
}

func (ex *Executor) removeTask(task Task, step int) {
	if task.Err() != nil {
		log.Info("execute action done, remove it",
			zap.Int64("taskID", task.ID()),
			zap.Int("step", step),
			zap.Error(task.Err()))
	}

	ex.executingTasks.Delete(task.ID())
	ex.executingTaskNum.Dec()
}

func (ex *Executor) executeSegmentAction(task *SegmentTask, step int) {
	switch task.Actions()[step].Type() {
	case ActionTypeGrow:
		ex.loadSegment(task, step)

	case ActionTypeReduce:
		ex.releaseSegment(task, step)
	}
}

// loadSegment commits the request to merger,
// not really executes the request
func (ex *Executor) loadSegment(task *SegmentTask, step int) error {
	action := task.Actions()[step].(*SegmentAction)
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Cancel(err)
			ex.removeTask(task, step)
		}
	}()

	ctx := task.Context()
	schema, err := ex.broker.GetCollectionSchema(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get schema of collection", zap.Error(err))
		return err
	}
	partitions, err := utils.GetPartitions(ex.meta.CollectionManager, task.CollectionID())
	if err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return err
	}

	// TODO: improve this, queryCoord should keep index and schema in memory to save RPC.
	indexInfo, err := ex.broker.DescribeIndex(ctx, task.CollectionID())
	if err != nil {
		log.Warn("fail to get index meta of collection")
		return err
	}
	metricType, err := getMetricType(indexInfo, schema)
	if err != nil {
		log.Warn("failed to get metric type", zap.Error(err))
		return err
	}

	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		metricType,
		task.CollectionID(),
		partitions...,
	)
	resp, err := ex.broker.GetSegmentInfo(ctx, task.SegmentID())
	if err != nil || len(resp.GetInfos()) == 0 {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return err
	}
	segment := resp.GetInfos()[0]
	indexes, err := ex.broker.GetIndexInfo(ctx, task.CollectionID(), segment.GetID())
	if err != nil {
		log.Warn("failed to get index of segment", zap.Error(err))
		return err
	}
	loadInfo := utils.PackSegmentLoadInfo(resp, indexes)

	// Get shard leader for the given replica and segment
	leader, ok := getShardLeader(ex.meta.ReplicaManager, ex.dist, task.CollectionID(), action.Node(), segment.GetInsertChannel())
	if !ok {
		msg := "no shard leader for the segment to execute loading"
		err = merr.WrapErrChannelNotFound(segment.GetInsertChannel(), "shard delegator not found")
		log.Warn(msg, zap.Error(err))
		return err
	}
	log = log.With(zap.Int64("shardLeader", leader))

	req := packLoadSegmentRequest(task, action, schema, loadMeta, loadInfo)
	loadTask := NewLoadSegmentsTask(task, step, req)
	ex.merger.Add(loadTask)
	log.Info("load segment task committed")
	return nil
}

func (ex *Executor) releaseSegment(task *SegmentTask, step int) {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*SegmentAction)
	defer action.isReleaseCommitted.Store(true)

	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.segmentID),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
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
	if err != nil {
		log.Warn("failed to release segment, it may be a false failure", zap.Error(err))
		return
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to release segment", zap.String("reason", status.GetReason()))
		return
	}
	elapsed := time.Since(startTs)
	log.Info("release segment done", zap.Int64("taskID", task.ID()), zap.Duration("time taken", elapsed))
}

func (ex *Executor) executeDmChannelAction(task *ChannelTask, step int) {
	switch task.Actions()[step].Type() {
	case ActionTypeGrow:
		ex.subDmChannel(task, step)

	case ActionTypeReduce:
		ex.unsubDmChannel(task, step)
	}
}

func (ex *Executor) subDmChannel(task *ChannelTask, step int) error {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*ChannelAction)
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Cancel(err)
		}
	}()

	ctx := task.Context()

	schema, err := ex.broker.GetCollectionSchema(ctx, task.CollectionID())
	if err != nil {
		log.Warn("failed to get schema of collection")
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
	metricType, err := getMetricType(indexInfo, schema)
	if err != nil {
		log.Warn("failed to get metric type", zap.Error(err))
		return err
	}
	loadMeta := packLoadMeta(
		ex.meta.GetLoadType(task.CollectionID()),
		metricType,
		task.CollectionID(),
		partitions...,
	)

	dmChannel := ex.targetMgr.GetDmChannel(task.CollectionID(), action.ChannelName(), meta.NextTarget)
	if dmChannel == nil {
		msg := "channel does not exist in next target, skip it"
		log.Warn(msg, zap.String("channelName", action.ChannelName()))
		return merr.WrapErrChannelReduplicate(action.ChannelName())
	}
	req := packSubChannelRequest(task, action, schema, loadMeta, dmChannel, indexInfo)
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

func (ex *Executor) unsubDmChannel(task *ChannelTask, step int) error {
	defer ex.removeTask(task, step)
	startTs := time.Now()
	action := task.Actions()[step].(*ChannelAction)
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("channel", task.Channel()),
		zap.Int64("node", action.Node()),
		zap.Int64("source", task.SourceID()),
	)

	var err error
	defer func() {
		if err != nil {
			task.Cancel(err)
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
