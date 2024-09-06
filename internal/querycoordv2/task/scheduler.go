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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	TaskTypeGrow Type = iota + 1
	TaskTypeReduce
	TaskTypeMove
	TaskTypeUpdate
)

var TaskTypeName = map[Type]string{
	TaskTypeGrow:   "Grow",
	TaskTypeReduce: "Reduce",
	TaskTypeMove:   "Move",
	TaskTypeUpdate: "Update",
}

type Type int32

func (t Type) String() string {
	return TaskTypeName[t]
}

type replicaSegmentIndex struct {
	ReplicaID int64
	SegmentID int64
	IsGrowing bool
}

func NewReplicaSegmentIndex(task *SegmentTask) replicaSegmentIndex {
	isGrowing := task.Actions()[0].(*SegmentAction).Scope() == querypb.DataScope_Streaming
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: isGrowing,
	}
}

func NewReplicaLeaderIndex(task *LeaderTask) replicaSegmentIndex {
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: false,
	}
}

type replicaChannelIndex struct {
	ReplicaID int64
	Channel   string
}

type taskQueue struct {
	// TaskPriority -> TaskID -> Task
	buckets []map[int64]Task
}

func newTaskQueue() *taskQueue {
	buckets := make([]map[int64]Task, len(TaskPriorities))
	for i := range buckets {
		buckets[i] = make(map[int64]Task)
	}
	return &taskQueue{
		buckets: buckets,
	}
}

func (queue *taskQueue) Len() int {
	taskNum := 0
	for _, tasks := range queue.buckets {
		taskNum += len(tasks)
	}

	return taskNum
}

func (queue *taskQueue) Add(task Task) {
	bucket := queue.buckets[task.Priority()]
	bucket[task.ID()] = task
}

func (queue *taskQueue) Remove(task Task) {
	bucket := queue.buckets[task.Priority()]
	delete(bucket, task.ID())
}

// Range iterates all tasks in the queue ordered by priority from high to low
func (queue *taskQueue) Range(fn func(task Task) bool) {
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for _, task := range queue.buckets[priority] {
			if !fn(task) {
				return
			}
		}
	}
}

type Scheduler interface {
	Start()
	Stop()
	AddExecutor(nodeID int64)
	RemoveExecutor(nodeID int64)
	Add(task Task) error
	Dispatch(node int64)
	RemoveByNode(node int64)
	GetExecutedFlag(nodeID int64) <-chan struct{}
	GetChannelTaskNum() int
	GetSegmentTaskNum() int

	GetSegmentTaskDelta(nodeID int64, collectionID int64) int
	GetChannelTaskDelta(nodeID int64, collectionID int64) int
}

type taskScheduler struct {
	rwmutex     sync.RWMutex
	ctx         context.Context
	executors   map[int64]*Executor // NodeID -> Executor
	idAllocator func() UniqueID

	distMgr   *meta.DistributionManager
	meta      *meta.Meta
	targetMgr meta.TargetManagerInterface
	broker    meta.Broker
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	tasks        UniqueSet
	segmentTasks map[replicaSegmentIndex]Task
	channelTasks map[replicaChannelIndex]Task
	processQueue *taskQueue
	waitQueue    *taskQueue

	// executing task delta changes on node: nodeID -> collectionID -> delta changes
	// delta changes measure by segment row count and channel num
	segmentExecutingTaskDelta map[int64]map[int64]int
	channelExecutingTaskDelta map[int64]map[int64]int
}

func NewScheduler(ctx context.Context,
	meta *meta.Meta,
	distMgr *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	broker meta.Broker,
	cluster session.Cluster,
	nodeMgr *session.NodeManager,
) *taskScheduler {
	id := time.Now().UnixMilli()
	return &taskScheduler{
		ctx:       ctx,
		executors: make(map[int64]*Executor),
		idAllocator: func() UniqueID {
			id++
			return id
		},

		distMgr:   distMgr,
		meta:      meta,
		targetMgr: targetMgr,
		broker:    broker,
		cluster:   cluster,
		nodeMgr:   nodeMgr,

		tasks:                     make(UniqueSet),
		segmentTasks:              make(map[replicaSegmentIndex]Task),
		channelTasks:              make(map[replicaChannelIndex]Task),
		processQueue:              newTaskQueue(),
		waitQueue:                 newTaskQueue(),
		segmentExecutingTaskDelta: make(map[int64]map[int64]int),
		channelExecutingTaskDelta: make(map[int64]map[int64]int),
	}
}

func (scheduler *taskScheduler) Start() {}

func (scheduler *taskScheduler) Stop() {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	for nodeID, executor := range scheduler.executors {
		executor.Stop()
		delete(scheduler.executors, nodeID)
		delete(scheduler.segmentExecutingTaskDelta, nodeID)
		delete(scheduler.channelExecutingTaskDelta, nodeID)
	}

	for _, task := range scheduler.segmentTasks {
		scheduler.remove(task)
	}
	for _, task := range scheduler.channelTasks {
		scheduler.remove(task)
	}
}

func (scheduler *taskScheduler) AddExecutor(nodeID int64) {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	if _, exist := scheduler.executors[nodeID]; exist {
		return
	}

	executor := NewExecutor(scheduler.meta,
		scheduler.distMgr,
		scheduler.broker,
		scheduler.targetMgr,
		scheduler.cluster,
		scheduler.nodeMgr)

	scheduler.segmentExecutingTaskDelta[nodeID] = make(map[int64]int)
	scheduler.channelExecutingTaskDelta[nodeID] = make(map[int64]int)
	scheduler.executors[nodeID] = executor
	executor.Start(scheduler.ctx)
	log.Info("add executor for new QueryNode", zap.Int64("nodeID", nodeID))
}

func (scheduler *taskScheduler) RemoveExecutor(nodeID int64) {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	executor, ok := scheduler.executors[nodeID]
	if ok {
		executor.Stop()
		delete(scheduler.executors, nodeID)
		delete(scheduler.segmentExecutingTaskDelta, nodeID)
		delete(scheduler.channelExecutingTaskDelta, nodeID)
		log.Info("remove executor of offline QueryNode", zap.Int64("nodeID", nodeID))
	}
}

func (scheduler *taskScheduler) Add(task Task) error {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	err := scheduler.preAdd(task)
	if err != nil {
		task.Cancel(err)
		return err
	}

	task.SetID(scheduler.idAllocator())
	scheduler.waitQueue.Add(task)
	scheduler.tasks.Insert(task.ID())
	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		scheduler.segmentTasks[index] = task

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		scheduler.channelTasks[index] = task

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		scheduler.segmentTasks[index] = task
	}

	scheduler.updateTaskMetrics()
	scheduler.updateTaskDelta(task)

	log.Ctx(task.Context()).Info("task added", zap.String("task", task.String()))
	task.RecordStartTs()
	return nil
}

func (scheduler *taskScheduler) updateTaskDelta(task Task) {
	var delta int
	var deltaMap map[int64]map[int64]int
	switch task := task.(type) {
	case *SegmentTask:
		// skip growing segment's count, cause doesn't know realtime row number of growing segment
		if task.Actions()[0].(*SegmentAction).Scope() == querypb.DataScope_Historical {
			segment := scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.NextTargetFirst)
			if segment != nil {
				delta = int(segment.GetNumOfRows())
			}
		}

		deltaMap = scheduler.segmentExecutingTaskDelta

	case *ChannelTask:
		delta = 1
		deltaMap = scheduler.channelExecutingTaskDelta
	}

	// turn delta to negative when try to remove task
	if task.Status() == TaskStatusSucceeded || task.Status() == TaskStatusFailed || task.Status() == TaskStatusCanceled {
		delta = -delta
	}

	if delta != 0 {
		for _, action := range task.Actions() {
			if deltaMap[action.Node()] == nil {
				deltaMap[action.Node()] = make(map[int64]int)
			}
			if action.Type() == ActionTypeGrow {
				deltaMap[action.Node()][task.CollectionID()] += delta
			} else if action.Type() == ActionTypeReduce {
				deltaMap[action.Node()][task.CollectionID()] -= delta
			}
		}
	}
}

func (scheduler *taskScheduler) updateTaskMetrics() {
	segmentGrowNum, segmentReduceNum, segmentMoveNum := 0, 0, 0
	channelGrowNum, channelReduceNum, channelMoveNum := 0, 0, 0
	for _, task := range scheduler.segmentTasks {
		taskType := GetTaskType(task)
		switch taskType {
		case TaskTypeGrow:
			segmentGrowNum++
		case TaskTypeReduce:
			segmentReduceNum++
		case TaskTypeMove:
			segmentMoveNum++
		}
	}

	for _, task := range scheduler.channelTasks {
		taskType := GetTaskType(task)
		switch taskType {
		case TaskTypeGrow:
			channelGrowNum++
		case TaskTypeReduce:
			channelReduceNum++
		case TaskTypeMove:
			channelMoveNum++
		}
	}

	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentGrowTaskLabel).Set(float64(segmentGrowNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentReduceTaskLabel).Set(float64(segmentReduceNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentMoveTaskLabel).Set(float64(segmentMoveNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelGrowTaskLabel).Set(float64(channelGrowNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelReduceTaskLabel).Set(float64(channelReduceNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelMoveTaskLabel).Set(float64(channelMoveNum))
}

// check whether the task is valid to add,
// must hold lock
func (scheduler *taskScheduler) preAdd(task Task) error {
	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		if old, ok := scheduler.segmentTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("oldID", old.ID()),
					zap.String("oldPriority", old.Priority().String()),
					zap.Int64("newID", task.ID()),
					zap.String("newPriority", task.Priority().String()),
				)
				old.Cancel(merr.WrapErrServiceInternal("replaced with the other one with higher priority"))
				scheduler.remove(old)
				return nil
			}

			return merr.WrapErrServiceInternal("task with the same segment exists")
		}

		taskType := GetTaskType(task)

		if taskType == TaskTypeMove {
			views := scheduler.distMgr.LeaderViewManager.GetByFilter(meta.WithSegment2LeaderView(task.SegmentID(), false))
			if len(views) == 0 {
				return merr.WrapErrServiceInternal("segment's delegator not found, stop balancing")
			}
			segmentInTargetNode := scheduler.distMgr.SegmentDistManager.GetByFilter(meta.WithNodeID(task.Actions()[1].Node()), meta.WithSegmentID(task.SegmentID()))
			if len(segmentInTargetNode) == 0 {
				return merr.WrapErrServiceInternal("source segment released, stop balancing")
			}
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		if old, ok := scheduler.channelTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("oldID", old.ID()),
					zap.String("oldPriority", old.Priority().String()),
					zap.Int64("newID", task.ID()),
					zap.String("newPriority", task.Priority().String()),
				)
				old.Cancel(merr.WrapErrServiceInternal("replaced with the other one with higher priority"))
				scheduler.remove(old)
				return nil
			}

			return merr.WrapErrServiceInternal("task with the same channel exists")
		}

		taskType := GetTaskType(task)
		if taskType == TaskTypeGrow {
			views := scheduler.distMgr.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(task.Channel()))
			nodesWithChannel := lo.Map(views, func(v *meta.LeaderView, _ int) UniqueID { return v.ID })
			replicaNodeMap := utils.GroupNodesByReplica(scheduler.meta.ReplicaManager, task.CollectionID(), nodesWithChannel)
			if _, ok := replicaNodeMap[task.ReplicaID()]; ok {
				return merr.WrapErrServiceInternal("channel subscribed, it can be only balanced")
			}
		} else if taskType == TaskTypeMove {
			views := scheduler.distMgr.LeaderViewManager.GetByFilter(meta.WithChannelName2LeaderView(task.Channel()))
			_, ok := lo.Find(views, func(v *meta.LeaderView) bool { return v.ID == task.Actions()[1].Node() })
			if !ok {
				return merr.WrapErrServiceInternal("source channel unsubscribed, stop balancing")
			}
		}
	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		if old, ok := scheduler.segmentTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("oldID", old.ID()),
					zap.String("oldPriority", old.Priority().String()),
					zap.Int64("newID", task.ID()),
					zap.String("newPriority", task.Priority().String()),
				)
				old.Cancel(merr.WrapErrServiceInternal("replaced with the other one with higher priority"))
				scheduler.remove(old)
				return nil
			}

			return merr.WrapErrServiceInternal("task with the same segment exists")
		}
	default:
		panic(fmt.Sprintf("preAdd: forget to process task type: %+v", task))
	}
	return nil
}

func (scheduler *taskScheduler) tryPromoteAll() {
	// Promote waiting tasks
	toPromote := make([]Task, 0, scheduler.waitQueue.Len())
	toRemove := make([]Task, 0)
	scheduler.waitQueue.Range(func(task Task) bool {
		err := scheduler.promote(task)
		if err != nil {
			task.Cancel(err)
			toRemove = append(toRemove, task)
			log.Warn("failed to promote task",
				zap.Int64("taskID", task.ID()),
				zap.Error(err),
			)
		} else {
			toPromote = append(toPromote, task)
		}

		return true
	})

	for _, task := range toPromote {
		scheduler.waitQueue.Remove(task)
	}
	for _, task := range toRemove {
		scheduler.remove(task)
	}

	if len(toPromote) > 0 || len(toRemove) > 0 {
		log.Debug("promoted tasks",
			zap.Int("promotedNum", len(toPromote)),
			zap.Int("toRemoveNum", len(toRemove)))
	}
}

func (scheduler *taskScheduler) promote(task Task) error {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("source", task.Source().String()),
	)

	if err := scheduler.check(task); err != nil {
		log.Info("failed to promote task", zap.Error(err))
		return err
	}

	scheduler.processQueue.Add(task)
	task.SetStatus(TaskStatusStarted)
	return nil
}

func (scheduler *taskScheduler) Dispatch(node int64) {
	select {
	case <-scheduler.ctx.Done():
		log.Info("scheduler stopped")

	default:
		scheduler.rwmutex.Lock()
		defer scheduler.rwmutex.Unlock()
		scheduler.schedule(node)
	}
}

func (scheduler *taskScheduler) GetSegmentTaskDelta(nodeID, collectionID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return scheduler.calculateTaskDelta(nodeID, collectionID, scheduler.segmentExecutingTaskDelta)
}

func (scheduler *taskScheduler) GetChannelTaskDelta(nodeID, collectionID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return scheduler.calculateTaskDelta(nodeID, collectionID, scheduler.channelExecutingTaskDelta)
}

func (scheduler *taskScheduler) calculateTaskDelta(nodeID, collectionID int64, deltaMap map[int64]map[int64]int) int {
	if nodeID == -1 && collectionID == -1 {
		return 0
	}

	sum := 0
	for nid, nInfo := range deltaMap {
		if nid != nodeID && -1 != nodeID {
			continue
		}

		for cid, cInfo := range nInfo {
			if cid == collectionID || -1 == collectionID {
				sum += cInfo
			}
		}
	}

	return sum
}

func (scheduler *taskScheduler) GetExecutedFlag(nodeID int64) <-chan struct{} {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	executor, ok := scheduler.executors[nodeID]
	if !ok {
		return nil
	}

	return executor.GetExecutedFlag()
}

func (scheduler *taskScheduler) GetChannelTaskNum() int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return len(scheduler.channelTasks)
}

func (scheduler *taskScheduler) GetSegmentTaskNum() int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return len(scheduler.segmentTasks)
}

// schedule selects some tasks to execute, follow these steps for each started selected tasks:
// 1. check whether this task is stale, set status to canceled if stale
// 2. step up the task's actions, set status to succeeded if all actions finished
// 3. execute the current action of task
func (scheduler *taskScheduler) schedule(node int64) {
	if scheduler.tasks.Len() == 0 {
		return
	}

	log := log.With(
		zap.Int64("nodeID", node),
	)

	scheduler.tryPromoteAll()

	log.Debug("process tasks related to node",
		zap.Int("processingTaskNum", scheduler.processQueue.Len()),
		zap.Int("waitingTaskNum", scheduler.waitQueue.Len()),
		zap.Int("segmentTaskNum", len(scheduler.segmentTasks)),
		zap.Int("channelTaskNum", len(scheduler.channelTasks)),
	)

	// Process tasks
	toProcess := make([]Task, 0)
	toRemove := make([]Task, 0)
	scheduler.processQueue.Range(func(task Task) bool {
		if scheduler.preProcess(task) && scheduler.isRelated(task, node) {
			toProcess = append(toProcess, task)
		}
		if task.Status() != TaskStatusStarted {
			toRemove = append(toRemove, task)
		}

		return true
	})

	// The scheduler doesn't limit the number of tasks,
	// to commit tasks to executors as soon as possible, to reach higher merge possibility
	commmittedNum := atomic.NewInt32(0)
	funcutil.ProcessFuncParallel(len(toProcess), hardware.GetCPUNum(), func(idx int) error {
		if scheduler.process(toProcess[idx]) {
			commmittedNum.Inc()
		}
		return nil
	}, "process")

	for _, task := range toRemove {
		scheduler.remove(task)
	}

	log.Info("processed tasks",
		zap.Int("toProcessNum", len(toProcess)),
		zap.Int32("committedNum", commmittedNum.Load()),
		zap.Int("toRemoveNum", len(toRemove)),
	)

	log.Info("process tasks related to node done",
		zap.Int("processingTaskNum", scheduler.processQueue.Len()),
		zap.Int("waitingTaskNum", scheduler.waitQueue.Len()),
		zap.Int("segmentTaskNum", len(scheduler.segmentTasks)),
		zap.Int("channelTaskNum", len(scheduler.channelTasks)),
	)
}

func (scheduler *taskScheduler) isRelated(task Task, node int64) bool {
	for _, action := range task.Actions() {
		if action.Node() == node {
			return true
		}
		if task, ok := task.(*SegmentTask); ok {
			taskType := GetTaskType(task)
			var segment *datapb.SegmentInfo
			if taskType == TaskTypeMove || taskType == TaskTypeUpdate {
				segment = scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.CurrentTarget)
			} else {
				segment = scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.NextTarget)
			}
			if segment == nil {
				continue
			}
			replica := scheduler.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
			if replica == nil {
				continue
			}
			leader, ok := scheduler.distMgr.GetShardLeader(replica, segment.GetInsertChannel())
			if !ok {
				continue
			}
			if leader == node {
				return true
			}
		}
	}
	return false
}

// preProcess checks the finished actions of task,
// and converts the task's status,
// return true if the task should be executed,
// false otherwise
func (scheduler *taskScheduler) preProcess(task Task) bool {
	log := log.Ctx(scheduler.ctx).WithRateGroup("qcv2.taskScheduler", 1, 60).With(
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("taskID", task.ID()),
	)
	if task.Status() != TaskStatusStarted {
		return false
	}

	actions, step := task.Actions(), task.Step()
	for step < len(actions) && actions[step].IsFinished(scheduler.distMgr) {
		if GetTaskType(task) == TaskTypeMove && actions[step].Type() == ActionTypeGrow {
			var ready bool
			switch actions[step].(type) {
			case *ChannelAction:
				// if balance channel task has finished grow action, block reduce action until
				// segment distribution has been sync to new delegator, cause new delegator may
				// causes a few time to load delta log, if reduce the old delegator in advance,
				// new delegator can't service search and query, will got no available channel error
				channelAction := actions[step].(*ChannelAction)
				leader := scheduler.distMgr.LeaderViewManager.GetLeaderShardView(channelAction.Node(), channelAction.Shard())
				ready = leader.UnServiceableError == nil
			default:
				ready = true
			}

			if !ready {
				log.RatedInfo(30, "Blocking reduce action in balance channel task")
				break
			}
		}
		task.StepUp()
		step++
	}

	if task.IsFinished(scheduler.distMgr) {
		task.SetStatus(TaskStatusSucceeded)
	} else {
		if err := scheduler.check(task); err != nil {
			task.Cancel(err)
		}
	}

	return task.Status() == TaskStatusStarted
}

// process processes the given task,
// return true if the task is started and succeeds to commit the current action
func (scheduler *taskScheduler) process(task Task) bool {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("type", GetTaskType(task).String()),
		zap.String("source", task.Source().String()),
	)

	actions, step := task.Actions(), task.Step()
	executor, ok := scheduler.executors[actions[step].Node()]
	if !ok {
		log.Warn("no executor for QueryNode",
			zap.Int("step", step),
			zap.Int64("nodeID", actions[step].Node()))
		return false
	}

	return executor.Execute(task, step)
}

func (scheduler *taskScheduler) check(task Task) error {
	err := task.Context().Err()
	if err == nil {
		err = scheduler.checkStale(task)
	}

	return err
}

func (scheduler *taskScheduler) RemoveByNode(node int64) {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	for _, task := range scheduler.segmentTasks {
		if scheduler.isRelated(task, node) {
			scheduler.remove(task)
		}
	}
	for _, task := range scheduler.channelTasks {
		if scheduler.isRelated(task, node) {
			scheduler.remove(task)
		}
	}
}

func (scheduler *taskScheduler) recordSegmentTaskError(task *SegmentTask) {
	log.Warn("task scheduler recordSegmentTaskError",
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.Int64("segmentID", task.SegmentID()),
		zap.String("status", task.Status()),
		zap.Error(task.err),
	)
	meta.GlobalFailedLoadCache.Put(task.collectionID, task.Err())
}

func (scheduler *taskScheduler) remove(task Task) {
	log := log.Ctx(task.Context()).With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("status", task.Status()),
	)

	if errors.Is(task.Err(), merr.ErrSegmentNotFound) {
		log.Info("segment in target has been cleaned, trigger force update next target", zap.Int64("collectionID", task.CollectionID()))
		scheduler.targetMgr.UpdateCollectionNextTarget(task.CollectionID())
	}

	task.Cancel(nil)
	scheduler.tasks.Remove(task.ID())
	scheduler.waitQueue.Remove(task)
	scheduler.processQueue.Remove(task)

	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		delete(scheduler.segmentTasks, index)
		log = log.With(zap.Int64("segmentID", task.SegmentID()))
		if task.Status() == TaskStatusFailed &&
			task.Err() != nil &&
			!errors.IsAny(task.Err(), merr.ErrChannelNotFound, merr.ErrServiceRequestLimitExceeded) {
			scheduler.recordSegmentTaskError(task)
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		delete(scheduler.channelTasks, index)
		log = log.With(zap.String("channel", task.Channel()))

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		delete(scheduler.segmentTasks, index)
		log = log.With(zap.Int64("segmentID", task.SegmentID()))
	}

	scheduler.updateTaskDelta(task)
	scheduler.updateTaskMetrics()
	log.Info("task removed")

	if scheduler.meta.Exist(task.CollectionID()) {
		metrics.QueryCoordTaskLatency.WithLabelValues(fmt.Sprint(task.CollectionID()),
			scheduler.getTaskMetricsLabel(task), task.Shard()).Observe(float64(task.GetTaskLatency()))
	}
}

func (scheduler *taskScheduler) getTaskMetricsLabel(task Task) string {
	taskType := GetTaskType(task)
	switch task.(type) {
	case *SegmentTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.SegmentGrowTaskLabel
		case TaskTypeReduce:
			return metrics.SegmentReduceTaskLabel
		case TaskTypeMove:
			return metrics.SegmentMoveTaskLabel
		case TaskTypeUpdate:
			return metrics.SegmentUpdateTaskLabel
		}

	case *ChannelTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.ChannelGrowTaskLabel
		case TaskTypeReduce:
			return metrics.ChannelReduceTaskLabel
		case TaskTypeMove:
			return metrics.ChannelMoveTaskLabel
		}

	case *LeaderTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.LeaderGrowTaskLabel
		case TaskTypeReduce:
			return metrics.LeaderReduceTaskLabel
		}
	}

	return metrics.UnknownTaskLabel
}

func (scheduler *taskScheduler) checkStale(task Task) error {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("source", task.Source().String()),
	)

	switch task := task.(type) {
	case *SegmentTask:
		if err := scheduler.checkSegmentTaskStale(task); err != nil {
			return err
		}

	case *ChannelTask:
		if err := scheduler.checkChannelTaskStale(task); err != nil {
			return err
		}

	case *LeaderTask:
		if err := scheduler.checkLeaderTaskStale(task); err != nil {
			return err
		}

	default:
		panic(fmt.Sprintf("checkStale: forget to check task type: %+v", task))
	}

	for step, action := range task.Actions() {
		log := log.With(
			zap.Int64("nodeID", action.Node()),
			zap.Int("step", step))

		if scheduler.nodeMgr.Get(action.Node()) == nil {
			log.Warn("the task is stale, the target node is offline")
			return merr.WrapErrNodeNotFound(action.Node())
		}
	}

	return nil
}

func (scheduler *taskScheduler) checkSegmentTaskStale(task *SegmentTask) error {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("source", task.Source().String()),
	)

	for _, action := range task.Actions() {
		switch action.Type() {
		case ActionTypeGrow:
			if ok, _ := scheduler.nodeMgr.IsStoppingNode(action.Node()); ok {
				log.Warn("task stale due to node offline", zap.Int64("segment", task.segmentID))
				return merr.WrapErrNodeOffline(action.Node())
			}
			taskType := GetTaskType(task)
			var segment *datapb.SegmentInfo
			if taskType == TaskTypeMove || taskType == TaskTypeUpdate {
				segment = scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.CurrentTarget)
			} else {
				segment = scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.NextTargetFirst)
			}
			if segment == nil {
				log.Warn("task stale due to the segment to load not exists in targets",
					zap.Int64("segment", task.segmentID),
					zap.String("taskType", taskType.String()),
				)
				return merr.WrapErrSegmentReduplicate(task.SegmentID(), "target doesn't contain this segment")
			}

			replica := scheduler.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
			if replica == nil {
				log.Warn("task stale due to replica not found")
				return merr.WrapErrReplicaNotFound(task.CollectionID(), "by collectionID")
			}
			_, ok := scheduler.distMgr.GetShardLeader(replica, segment.GetInsertChannel())
			if !ok {
				log.Warn("task stale due to leader not found")
				return merr.WrapErrChannelNotFound(segment.GetInsertChannel(), "failed to get shard delegator")
			}

		case ActionTypeReduce:
			// do nothing here
		}
	}
	return nil
}

func (scheduler *taskScheduler) checkChannelTaskStale(task *ChannelTask) error {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("source", task.Source().String()),
	)

	for _, action := range task.Actions() {
		switch action.Type() {
		case ActionTypeGrow:
			if ok, _ := scheduler.nodeMgr.IsStoppingNode(action.Node()); ok {
				log.Warn("task stale due to node offline", zap.String("channel", task.Channel()))
				return merr.WrapErrNodeOffline(action.Node())
			}
			if scheduler.targetMgr.GetDmChannel(task.collectionID, task.Channel(), meta.NextTargetFirst) == nil {
				log.Warn("the task is stale, the channel to subscribe not exists in targets",
					zap.String("channel", task.Channel()))
				return merr.WrapErrChannelReduplicate(task.Channel(), "target doesn't contain this channel")
			}

		case ActionTypeReduce:
			// do nothing here
		}
	}
	return nil
}

func (scheduler *taskScheduler) checkLeaderTaskStale(task *LeaderTask) error {
	log := log.With(
		zap.Int64("taskID", task.ID()),
		zap.Int64("collectionID", task.CollectionID()),
		zap.Int64("replicaID", task.ReplicaID()),
		zap.String("source", task.Source().String()),
		zap.Int64("leaderID", task.leaderID),
	)

	for _, action := range task.Actions() {
		switch action.Type() {
		case ActionTypeGrow:
			if ok, _ := scheduler.nodeMgr.IsStoppingNode(action.(*LeaderAction).GetLeaderID()); ok {
				log.Warn("task stale due to node offline", zap.Int64("segment", task.segmentID))
				return merr.WrapErrNodeOffline(action.Node())
			}

			taskType := GetTaskType(task)
			segment := scheduler.targetMgr.GetSealedSegment(task.CollectionID(), task.SegmentID(), meta.CurrentTargetFirst)
			if segment == nil {
				log.Warn("task stale due to the segment to load not exists in targets",
					zap.Int64("segment", task.segmentID),
					zap.String("taskType", taskType.String()),
				)
				return merr.WrapErrSegmentReduplicate(task.SegmentID(), "target doesn't contain this segment")
			}

			replica := scheduler.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
			if replica == nil {
				log.Warn("task stale due to replica not found")
				return merr.WrapErrReplicaNotFound(task.CollectionID(), "by collectionID")
			}

			view := scheduler.distMgr.GetLeaderShardView(task.leaderID, task.Shard())
			if view == nil {
				log.Warn("task stale due to leader not found")
				return merr.WrapErrChannelNotFound(task.Shard(), "failed to get shard delegator")
			}

		case ActionTypeReduce:
			view := scheduler.distMgr.GetLeaderShardView(task.leaderID, task.Shard())
			if view == nil {
				log.Warn("task stale due to leader not found")
				return merr.WrapErrChannelNotFound(task.Shard(), "failed to get shard delegator")
			}
		}
	}
	return nil
}
