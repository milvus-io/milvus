package task

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	TaskTypeGrow Type = iota + 1
	TaskTypeReduce
	TaskTypeMove

	taskPoolSize = 128
)

var (
	ErrConflictTaskExisted = errors.New("ConflictTaskExisted")

	// The task is canceled or timeout
	ErrTaskCanceled = errors.New("TaskCanceled")

	// The target node is offline,
	// or the target segment is not in TargetManager,
	// or the target channel is not in TargetManager
	ErrTaskStale = errors.New("TaskStale")

	// No enough memory to load segment
	ErrResourceNotEnough = errors.New("ResourceNotEnough")

	ErrTaskQueueFull = errors.New("TaskQueueFull")
)

type Type = int32

type replicaSegmentIndex struct {
	ReplicaID int64
	SegmentID int64
}

type replicaChannelIndex struct {
	ReplicaID int64
	Channel   string
}

type taskQueue struct {
	// TaskPriority -> Tasks
	buckets [][]Task

	cap int
}

func newTaskQueue(cap int) *taskQueue {
	return &taskQueue{
		buckets: make([][]Task, len(TaskPriorities)),

		cap: cap,
	}
}

func (queue *taskQueue) Len() int {
	taskNum := 0
	for _, tasks := range queue.buckets {
		taskNum += len(tasks)
	}

	return taskNum
}

func (queue *taskQueue) Cap() int {
	return queue.cap
}

func (queue *taskQueue) Add(task Task) bool {
	if queue.Len() >= queue.Cap() {
		return false
	}

	queue.buckets[task.Priority()] = append(queue.buckets[task.Priority()], task)
	return true
}

func (queue *taskQueue) Remove(task Task) {
	bucket := &queue.buckets[task.Priority()]

	for i := range *bucket {
		if (*bucket)[i].ID() == task.ID() {
			*bucket = append((*bucket)[:i], (*bucket)[i+1:]...)
			break
		}
	}
}

// Range iterates all tasks in the queue ordered by priority from high to low
func (queue *taskQueue) Range(fn func(task Task) bool) {
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for i := range queue.buckets[priority] {
			if !fn(queue.buckets[priority][i]) {
				return
			}
		}
	}
}

type Scheduler interface {
	Add(task Task) error
	Dispatch(node int64)
	RemoveByNode(node int64)
	GetNodeSegmentDelta(nodeID int64) int
	GetNodeChannelDelta(nodeID int64) int
}

type taskScheduler struct {
	rwmutex     sync.RWMutex
	ctx         context.Context
	executor    *Executor
	idAllocator func() UniqueID

	distMgr   *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	broker    meta.Broker
	nodeMgr   *session.NodeManager

	tasks        UniqueSet
	segmentTasks map[replicaSegmentIndex]Task
	channelTasks map[replicaChannelIndex]Task
	processQueue *taskQueue
	waitQueue    *taskQueue
}

func NewScheduler(ctx context.Context,
	meta *meta.Meta,
	distMgr *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	broker meta.Broker,
	cluster session.Cluster,
	nodeMgr *session.NodeManager) *taskScheduler {
	id := int64(0)
	return &taskScheduler{
		ctx:      ctx,
		executor: NewExecutor(meta, distMgr, broker, targetMgr, cluster, nodeMgr),
		idAllocator: func() UniqueID {
			id++
			return id
		},

		distMgr:   distMgr,
		meta:      meta,
		targetMgr: targetMgr,
		broker:    broker,
		nodeMgr:   nodeMgr,

		tasks:        make(UniqueSet),
		segmentTasks: make(map[replicaSegmentIndex]Task),
		channelTasks: make(map[replicaChannelIndex]Task),
		processQueue: newTaskQueue(taskPoolSize),
		waitQueue:    newTaskQueue(taskPoolSize * 10),
	}
}

func (scheduler *taskScheduler) Add(task Task) error {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	err := scheduler.preAdd(task)
	if err != nil {
		return err
	}

	task.SetID(scheduler.idAllocator())
	scheduler.tasks.Insert(task.ID())
	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		scheduler.segmentTasks[index] = task

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		scheduler.channelTasks[index] = task
	}
	if !scheduler.waitQueue.Add(task) {
		log.Warn("failed to add task", zap.String("task", task.String()))
		return nil
	}
	log.Info("task added", zap.String("task", task.String()))
	return nil
}

// check checks whether the task is valid to add,
// must hold lock
func (scheduler *taskScheduler) preAdd(task Task) error {
	if scheduler.waitQueue.Len() >= scheduler.waitQueue.Cap() {
		return ErrTaskQueueFull
	}

	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.segmentID}
		if old, ok := scheduler.segmentTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("oldID", old.ID()),
					zap.Int32("oldPrioprity", old.Priority()),
					zap.Int64("newID", task.ID()),
					zap.Int32("newPriority", task.Priority()),
				)
				old.SetStatus(TaskStatusCanceled)
				old.SetErr(utils.WrapError("replaced with the other one with higher priority", ErrTaskCanceled))
				scheduler.remove(old)
				return nil
			}

			return ErrConflictTaskExisted
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.channel}
		if old, ok := scheduler.channelTasks[index]; ok {
			if task.Priority() > old.Priority() {
				log.Info("replace old task, the new one with higher priority",
					zap.Int64("oldID", old.ID()),
					zap.Int32("oldPriority", old.Priority()),
					zap.Int64("newID", task.ID()),
					zap.Int32("newPriority", task.Priority()),
				)
				old.SetStatus(TaskStatusCanceled)
				old.SetErr(utils.WrapError("replaced with the other one with higher priority", ErrTaskCanceled))
				scheduler.remove(old)
				return nil
			}

			return ErrConflictTaskExisted
		}

	default:
		panic(fmt.Sprintf("preAdd: forget to process task type: %+v", task))
	}

	return nil
}

func (scheduler *taskScheduler) promote(task Task) error {
	log := log.With(
		zap.Int64("collection", task.CollectionID()),
		zap.Int64("task", task.ID()),
		zap.Int64("source", task.SourceID()),
	)
	err := scheduler.prePromote(task)
	if err != nil {
		log.Info("failed to promote task", zap.Error(err))
		return err
	}

	if scheduler.processQueue.Add(task) {
		task.SetStatus(TaskStatusStarted)
		return nil
	}

	return ErrTaskQueueFull
}

func (scheduler *taskScheduler) tryPromoteAll() {
	// Promote waiting tasks
	toPromote := make([]Task, 0, scheduler.processQueue.Cap()-scheduler.processQueue.Len())
	toRemove := make([]Task, 0)
	scheduler.waitQueue.Range(func(task Task) bool {
		err := scheduler.promote(task)
		if errors.Is(err, ErrTaskStale) { // Task canceled or stale
			task.SetStatus(TaskStatusStale)
			task.SetErr(err)
			toRemove = append(toRemove, task)
		} else if errors.Is(err, ErrTaskCanceled) {
			task.SetStatus(TaskStatusCanceled)
			task.SetErr(err)
			toRemove = append(toRemove, task)
		} else if err == nil {
			toPromote = append(toPromote, task)
		}

		return !errors.Is(err, ErrTaskQueueFull)
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

func (scheduler *taskScheduler) prePromote(task Task) error {
	if scheduler.checkCanceled(task) {
		return ErrTaskCanceled
	} else if scheduler.checkStale(task) {
		return ErrTaskStale
	}

	return nil
}

func (scheduler *taskScheduler) Dispatch(node int64) {
	select {
	case <-scheduler.ctx.Done():
		log.Info("scheduler stopped")

	default:
		scheduler.schedule(node)
	}
}

func (scheduler *taskScheduler) GetNodeSegmentDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return calculateNodeDelta(nodeID, scheduler.segmentTasks)
}

func (scheduler *taskScheduler) GetNodeChannelDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	return calculateNodeDelta(nodeID, scheduler.channelTasks)
}

func calculateNodeDelta[K comparable, T ~map[K]Task](nodeID int64, tasks T) int {
	delta := 0
	for _, task := range tasks {
		for _, action := range task.Actions() {
			if action.Node() != nodeID {
				continue
			}
			if action.Type() == ActionTypeGrow {
				delta++
			} else if action.Type() == ActionTypeReduce {
				delta--
			}
		}
	}
	return delta
}

func (scheduler *taskScheduler) GetNodeSegmentCntDelta(nodeID int64) int {
	scheduler.rwmutex.RLock()
	defer scheduler.rwmutex.RUnlock()

	delta := 0
	for _, task := range scheduler.segmentTasks {
		for _, action := range task.Actions() {
			if action.Node() != nodeID {
				continue
			}
			segmentAction := action.(*SegmentAction)
			segment := scheduler.targetMgr.GetSegment(segmentAction.SegmentID())
			if action.Type() == ActionTypeGrow {
				delta += int(segment.GetNumOfRows())
			} else {
				delta -= int(segment.GetNumOfRows())
			}
		}
	}
	return delta
}

// schedule selects some tasks to execute, follow these steps for each started selected tasks:
// 1. check whether this task is stale, set status to failed if stale
// 2. step up the task's actions, set status to succeeded if all actions finished
// 3. execute the current action of task
func (scheduler *taskScheduler) schedule(node int64) {
	scheduler.rwmutex.Lock()
	defer scheduler.rwmutex.Unlock()

	if scheduler.tasks.Len() == 0 {
		return
	}

	log := log.With(
		zap.Int64("nodeID", node),
	)

	scheduler.tryPromoteAll()

	log.Debug("process tasks related to node",
		zap.Int("processing-task-num", scheduler.processQueue.Len()),
		zap.Int("waiting-task-num", scheduler.waitQueue.Len()),
		zap.Int("segment-task-num", len(scheduler.segmentTasks)),
		zap.Int("channel-task-num", len(scheduler.channelTasks)),
	)

	// Process tasks
	toRemove := make([]Task, 0)
	scheduler.processQueue.Range(func(task Task) bool {
		log.Debug("check task related",
			zap.Int64("task", task.ID()))
		if scheduler.isRelated(task, node) {
			scheduler.process(task)
		} else {
			log.Debug("task not related, skip it",
				zap.Int64("task", task.ID()),
				zap.Int64("taskActionNode", task.Actions()[0].Node()),
			)
		}

		if task.Status() != TaskStatusStarted {
			toRemove = append(toRemove, task)
		}

		return true
	})

	for _, task := range toRemove {
		scheduler.remove(task)
	}

	log.Info("processed tasks",
		zap.Int("toRemoveNum", len(toRemove)))

	log.Debug("process tasks related to node done",
		zap.Int("processing-task-num", scheduler.processQueue.Len()),
		zap.Int("waiting-task-num", scheduler.waitQueue.Len()),
		zap.Int("segment-task-num", len(scheduler.segmentTasks)),
		zap.Int("channel-task-num", len(scheduler.channelTasks)),
	)
}

func (scheduler *taskScheduler) isRelated(task Task, node int64) bool {
	for _, action := range task.Actions() {
		if action.Node() == node {
			return true
		}
		if task, ok := task.(*SegmentTask); ok {
			segment := scheduler.targetMgr.GetSegment(task.SegmentID())
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

// process processes the given task,
// return true if the task is started and succeeds to commit the current action
func (scheduler *taskScheduler) process(task Task) bool {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int32("type", GetTaskType(task)),
		zap.Int64("source", task.SourceID()),
	)

	if task.IsFinished(scheduler.distMgr) {
		task.SetStatus(TaskStatusSucceeded)
	} else if scheduler.checkCanceled(task) {
		task.SetStatus(TaskStatusCanceled)
		task.SetErr(ErrTaskCanceled)
	} else if scheduler.checkStale(task) {
		task.SetStatus(TaskStatusStale)
		task.SetErr(ErrTaskStale)
	}

	actions, step := task.Actions(), task.Step()
	log = log.With(zap.Int("step", step))
	switch task.Status() {
	case TaskStatusStarted:
		if scheduler.executor.Execute(task, step, actions[step]) {
			return true
		}

	case TaskStatusSucceeded:
		log.Info("task succeeded")

	case TaskStatusCanceled, TaskStatusStale:
		log.Warn("failed to execute task", zap.Error(task.Err()))

	default:
		panic(fmt.Sprintf("invalid task status: %v", task.Status()))
	}

	return false
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

func (scheduler *taskScheduler) remove(task Task) {
	log := log.With(
		zap.Int64("task", task.ID()),
	)
	task.Cancel()
	scheduler.tasks.Remove(task.ID())
	scheduler.waitQueue.Remove(task)
	scheduler.processQueue.Remove(task)

	switch task := task.(type) {
	case *SegmentTask:
		index := replicaSegmentIndex{task.ReplicaID(), task.SegmentID()}
		delete(scheduler.segmentTasks, index)
		log = log.With(zap.Int64("segment", task.SegmentID()))

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		delete(scheduler.channelTasks, index)
		log = log.With(zap.String("channel", task.Channel()))
	}

	log.Debug("task removed")
}

func (scheduler *taskScheduler) checkCanceled(task Task) bool {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("source", task.SourceID()),
	)

	select {
	case <-task.Context().Done():
		log.Warn("the task is timeout or canceled")
		return true

	default:
		return false
	}
}

func (scheduler *taskScheduler) checkStale(task Task) bool {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("source", task.SourceID()),
	)

	switch task := task.(type) {
	case *SegmentTask:
		if scheduler.checkSegmentTaskStale(task) {
			return true
		}

	case *ChannelTask:
		if scheduler.checkChannelTaskStale(task) {
			return true
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
			return true
		}
	}

	return false
}

func (scheduler *taskScheduler) checkSegmentTaskStale(task *SegmentTask) bool {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("source", task.SourceID()),
	)

	for _, action := range task.Actions() {
		switch action.Type() {
		case ActionTypeGrow:
			segment := scheduler.targetMgr.GetSegment(task.SegmentID())
			if segment == nil {
				log.Warn("task stale due tu the segment to load not exists in targets",
					zap.Int64("segment", task.segmentID))
				return true
			}
			replica := scheduler.meta.ReplicaManager.GetByCollectionAndNode(task.CollectionID(), action.Node())
			if replica == nil {
				log.Warn("task stale due to replica not found")
				return true
			}
			_, ok := scheduler.distMgr.GetShardLeader(replica, segment.GetInsertChannel())
			if !ok {
				log.Warn("task stale due to leader not found")
				return true
			}

		case ActionTypeReduce:
			// Do nothing here,
			// the task should succeeded if the segment not exists
			// sealed := scheduler.distMgr.SegmentDistManager.GetByNode(action.Node())
			// growing := scheduler.distMgr.LeaderViewManager.GetSegmentByNode(action.Node())
			// segments := make([]int64, 0, len(sealed)+len(growing))
			// for _, segment := range sealed {
			// 	segments = append(segments, segment.GetID())
			// }
			// segments = append(segments, growing...)
			// if !funcutil.SliceContain(segments, task.SegmentID()) {
			// 	log.Warn("the task is stale, the segment to release not exists in dist",
			// 		zap.Int64("segment", task.segmentID))
			// 	return true
			// }
		}
	}
	return false
}

func (scheduler *taskScheduler) checkChannelTaskStale(task *ChannelTask) bool {
	log := log.With(
		zap.Int64("task", task.ID()),
		zap.Int64("source", task.SourceID()),
	)

	for _, action := range task.Actions() {
		switch action.Type() {
		case ActionTypeGrow:
			if !scheduler.targetMgr.ContainDmChannel(task.Channel()) {
				log.Warn("the task is stale, the channel to subscribe not exists in targets",
					zap.String("channel", task.Channel()))
				return true
			}

		case ActionTypeReduce:
			// Do nothing here,
			// the task should succeeded if the channel not exists
			// hasChannel := false
			// views := scheduler.distMgr.LeaderViewManager.GetLeaderView(action.Node())
			// for _, view := range views {
			// 	if view.Channel == task.Channel() {
			// 		hasChannel = true
			// 		break
			// 	}
			// }
			// if !hasChannel {
			// 	log.Warn("the task is stale, the channel to unsubscribe not exists in dist",
			// 		zap.String("channel", task.Channel()))
			// 	return true
			// }
		}
	}
	return false
}
