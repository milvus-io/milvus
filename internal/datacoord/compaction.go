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

package datacoord

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO: we just warn about the long executing/queuing tasks
// need to get rid of long queuing tasks because the compaction tasks are local optimum.
var maxCompactionTaskExecutionDuration = map[datapb.CompactionType]time.Duration{
	datapb.CompactionType_MixCompaction:          30 * time.Minute,
	datapb.CompactionType_Level0DeleteCompaction: 30 * time.Minute,
	datapb.CompactionType_ClusteringCompaction:   60 * time.Minute,
}

//go:generate mockery --name=compactionPlanContext --structname=MockCompactionPlanContext --output=./  --filename=mock_compaction_plan_context.go --with-expecter --inpackage
type compactionPlanContext interface {
	start()
	stop()
	// enqueueCompaction start to enqueue compaction task and return immediately
	enqueueCompaction(task *datapb.CompactionTask) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksNumBySignalID(signalID int64) int
	getCompactionInfo(signalID int64) *compactionInfo
	removeTasksByChannel(channel string)
}

var (
	errChannelNotWatched = errors.New("channel is not watched")
	errChannelInBuffer   = errors.New("channel is in buffer")
)

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionInfo struct {
	state        commonpb.CompactionState
	executingCnt int
	completedCnt int
	failedCnt    int
	timeoutCnt   int
	mergeInfos   map[int64]*milvuspb.CompactionMergeInfo
}

type compactionPlanHandler struct {
	queueTasks CompactionQueue

	executingGuard lock.RWMutex
	executingTasks map[int64]CompactionTask // planID -> task

	cleaningGuard lock.RWMutex
	cleaningTasks map[int64]CompactionTask // planID -> task

	meta             CompactionMeta
	allocator        allocator
	chManager        ChannelManager
	sessions         SessionManager
	cluster          Cluster
	analyzeScheduler *taskScheduler
	handler          Handler

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func (c *compactionPlanHandler) getCompactionInfo(triggerID int64) *compactionInfo {
	tasks := c.meta.GetCompactionTasksByTriggerID(triggerID)
	return summaryCompactionState(tasks)
}

func summaryCompactionState(tasks []*datapb.CompactionTask) *compactionInfo {
	ret := &compactionInfo{}
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, analyzingCnt, indexingCnt, cleanedCnt, metaSavedCnt int
	mergeInfos := make(map[int64]*milvuspb.CompactionMergeInfo)

	for _, task := range tasks {
		if task == nil {
			continue
		}
		switch task.GetState() {
		case datapb.CompactionTaskState_executing:
			executingCnt++
		case datapb.CompactionTaskState_pipelining:
			pipeliningCnt++
		case datapb.CompactionTaskState_completed:
			completedCnt++
		case datapb.CompactionTaskState_failed:
			failedCnt++
		case datapb.CompactionTaskState_timeout:
			timeoutCnt++
		case datapb.CompactionTaskState_analyzing:
			analyzingCnt++
		case datapb.CompactionTaskState_indexing:
			indexingCnt++
		case datapb.CompactionTaskState_cleaned:
			cleanedCnt++
		case datapb.CompactionTaskState_meta_saved:
			metaSavedCnt++
		default:
		}
		mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
	}

	ret.executingCnt = executingCnt + pipeliningCnt + analyzingCnt + indexingCnt + metaSavedCnt
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt
	ret.mergeInfos = mergeInfos

	if ret.executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}

	log.Info("compaction states",
		zap.String("state", ret.state.String()),
		zap.Int("executingCnt", executingCnt),
		zap.Int("pipeliningCnt", pipeliningCnt),
		zap.Int("completedCnt", completedCnt),
		zap.Int("failedCnt", failedCnt),
		zap.Int("timeoutCnt", timeoutCnt),
		zap.Int("analyzingCnt", analyzingCnt),
		zap.Int("indexingCnt", indexingCnt),
		zap.Int("cleanedCnt", cleanedCnt),
		zap.Int("metaSavedCnt", metaSavedCnt))
	return ret
}

func (c *compactionPlanHandler) getCompactionTasksNumBySignalID(triggerID int64) int {
	cnt := 0
	c.queueTasks.ForEach(func(ct CompactionTask) {
		if ct.GetTriggerID() == triggerID {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if t.GetTriggerID() == triggerID {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

func newCompactionPlanHandler(cluster Cluster, sessions SessionManager, cm ChannelManager, meta CompactionMeta, allocator allocator, analyzeScheduler *taskScheduler, handler Handler,
) *compactionPlanHandler {
	// Higher capacity will have better ordering in priority, but consumes more memory.
	// TODO[GOOSE]: Higher capacity makes tasks waiting longer, which need to be get rid of.
	capacity := paramtable.Get().DataCoordCfg.CompactionTaskQueueCapacity.GetAsInt()
	return &compactionPlanHandler{
		queueTasks:       *NewCompactionQueue(capacity, getPrioritizer()),
		chManager:        cm,
		meta:             meta,
		sessions:         sessions,
		allocator:        allocator,
		stopCh:           make(chan struct{}),
		cluster:          cluster,
		executingTasks:   make(map[int64]CompactionTask),
		cleaningTasks:    make(map[int64]CompactionTask),
		analyzeScheduler: analyzeScheduler,
		handler:          handler,
	}
}

func (c *compactionPlanHandler) schedule() []CompactionTask {
	selected := make([]CompactionTask, 0)
	if c.queueTasks.Len() == 0 {
		return selected
	}
	var (
		parallelism = Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
		slots       map[int64]int64
	)

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	clusterLabelExcludes := typeutil.NewSet[string]()

	c.executingGuard.RLock()
	if len(c.executingTasks) >= parallelism {
		c.executingGuard.RUnlock()
		return selected
	}

	for _, t := range c.executingTasks {
		switch t.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			l0ChannelExcludes.Insert(t.GetChannel())
		case datapb.CompactionType_MixCompaction:
			mixChannelExcludes.Insert(t.GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
		case datapb.CompactionType_ClusteringCompaction:
			clusterChannelExcludes.Insert(t.GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
		}
	}
	c.executingGuard.RUnlock()

	excluded := make([]CompactionTask, 0)
	defer func() {
		// Add back the excluded tasks
		for _, t := range excluded {
			c.queueTasks.Enqueue(t)
		}
	}()

	p := getPrioritizer()
	if &c.queueTasks.prioritizer != &p {
		c.queueTasks.UpdatePrioritizer(p)
	}

	// The schedule loop will stop if either:
	// 1. no more task to schedule (the task queue is empty)
	// 2. the parallelism of running tasks is reached
	// 3. no avaiable slots
	for {
		t, err := c.queueTasks.Dequeue()
		if err != nil {
			break // 1. no more task to schedule
		}

		switch t.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if mixChannelExcludes.Contain(t.GetChannel()) ||
				clusterChannelExcludes.Contain(t.GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			l0ChannelExcludes.Insert(t.GetChannel())
			selected = append(selected, t)
		case datapb.CompactionType_MixCompaction:
			if l0ChannelExcludes.Contain(t.GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			mixChannelExcludes.Insert(t.GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		case datapb.CompactionType_ClusteringCompaction:
			if l0ChannelExcludes.Contain(t.GetChannel()) ||
				mixLabelExcludes.Contain(t.GetLabel()) ||
				clusterLabelExcludes.Contain(t.GetLabel()) {
				excluded = append(excluded, t)
				continue
			}
			clusterChannelExcludes.Insert(t.GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		}

		if t.NeedReAssignNodeID() {
			if slots == nil {
				slots = c.cluster.QuerySlots()
			}
			id := assignNodeID(slots, t)
			if id == NullNodeID {
				log.RatedWarn(10, "not enough slots for compaction task", zap.Int64("planID", t.GetPlanID()))
				selected = selected[:len(selected)-1]
				excluded = append(excluded, t)
				break // 3. no avaiable slots
			}
		}

		c.executingGuard.Lock()
		c.executingTasks[t.GetPlanID()] = t
		if len(c.executingTasks) >= parallelism {
			c.executingGuard.Unlock()
			break // 2. the parallelism of running tasks is reached
		}
		c.executingGuard.Unlock()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetNodeID()), t.GetType().String(), metrics.Executing).Inc()
	}
	return selected
}

func (c *compactionPlanHandler) start() {
	c.loadMeta()
	c.stopWg.Add(3)
	go c.loopSchedule()
	go c.loopCheck()
	go c.loopClean()
}

func (c *compactionPlanHandler) loadMeta() {
	// TODO: make it compatible to all types of compaction with persist meta
	triggers := c.meta.GetCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			state := task.GetState()
			if state == datapb.CompactionTaskState_completed ||
				state == datapb.CompactionTaskState_cleaned ||
				state == datapb.CompactionTaskState_timeout ||
				state == datapb.CompactionTaskState_unknown {
				log.Info("compactionPlanHandler loadMeta abandon compactionTask",
					zap.Int64("planID", task.GetPlanID()),
					zap.String("type", task.GetType().String()),
					zap.String("state", task.GetState().String()))
				continue
			} else {
				t, err := c.createCompactTask(task)
				if err != nil {
					log.Info("compactionPlanHandler loadMeta create compactionTask failed, try to clean it",
						zap.Int64("planID", task.GetPlanID()),
						zap.String("type", task.GetType().String()),
						zap.String("state", task.GetState().String()),
						zap.Error(err),
					)
					// ignore the drop error
					c.meta.DropCompactionTask(task)
					continue
				}
				if t.NeedReAssignNodeID() {
					if err = c.submitTask(t); err != nil {
						log.Info("compactionPlanHandler loadMeta submit task failed, try to clean it",
							zap.Int64("planID", task.GetPlanID()),
							zap.String("type", task.GetType().String()),
							zap.String("state", task.GetState().String()),
							zap.Error(err),
						)
						// ignore the drop error
						c.meta.DropCompactionTask(task)
						continue
					}
					log.Info("compactionPlanHandler loadMeta submitTask",
						zap.Int64("planID", t.GetPlanID()),
						zap.Int64("triggerID", t.GetTriggerID()),
						zap.Int64("collectionID", t.GetCollectionID()),
						zap.String("type", task.GetType().String()),
						zap.String("state", t.GetState().String()))
				} else {
					c.restoreTask(t)
					log.Info("compactionPlanHandler loadMeta restoreTask",
						zap.Int64("planID", t.GetPlanID()),
						zap.Int64("triggerID", t.GetTriggerID()),
						zap.Int64("collectionID", t.GetCollectionID()),
						zap.String("type", task.GetType().String()),
						zap.String("state", t.GetState().String()))
				}
			}
		}
	}
}

func (c *compactionPlanHandler) loopSchedule() {
	interval := paramtable.Get().DataCoordCfg.CompactionScheduleInterval.GetAsDuration(time.Millisecond)
	log.Info("compactionPlanHandler start loop schedule", zap.Duration("schedule interval", interval))
	defer c.stopWg.Done()

	scheduleTicker := time.NewTicker(interval)
	defer scheduleTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			log.Info("compactionPlanHandler quit loop schedule")
			return

		case <-scheduleTicker.C:
			c.schedule()
		}
	}
}

func (c *compactionPlanHandler) loopCheck() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	log.Info("compactionPlanHandler start loop check", zap.Any("check result interval", interval))
	defer c.stopWg.Done()
	checkResultTicker := time.NewTicker(interval)
	for {
		select {
		case <-c.stopCh:
			log.Info("compactionPlanHandler quit loop check")
			return

		case <-checkResultTicker.C:
			err := c.checkCompaction()
			if err != nil {
				log.Info("fail to update compaction", zap.Error(err))
			}
			c.cleanFailedTasks()
		}
	}
}

func (c *compactionPlanHandler) loopClean() {
	interval := Params.DataCoordCfg.CompactionGCIntervalInSeconds.GetAsDuration(time.Second)
	log.Info("compactionPlanHandler start clean check loop", zap.Any("gc interval", interval))
	defer c.stopWg.Done()
	cleanTicker := time.NewTicker(interval)
	defer cleanTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			log.Info("Compaction handler quit loopClean")
			return
		case <-cleanTicker.C:
			c.Clean()
		}
	}
}

func (c *compactionPlanHandler) Clean() {
	c.cleanCompactionTaskMeta()
	c.cleanPartitionStats()
}

func (c *compactionPlanHandler) cleanCompactionTaskMeta() {
	// gc clustering compaction tasks
	triggers := c.meta.GetCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State == datapb.CompactionTaskState_cleaned {
				duration := time.Since(time.Unix(task.StartTime, 0)).Seconds()
				if duration > float64(Params.DataCoordCfg.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds()) {
					// try best to delete meta
					err := c.meta.DropCompactionTask(task)
					log.Debug("drop compaction task meta", zap.Int64("planID", task.PlanID))
					if err != nil {
						log.Warn("fail to drop task", zap.Int64("planID", task.PlanID), zap.Error(err))
					}
				}
			}
		}
	}
}

func (c *compactionPlanHandler) cleanPartitionStats() error {
	log.Debug("start gc partitionStats meta and files")
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo)
	unusedPartStats := make([]*datapb.PartitionStatsInfo, 0)
	if c.meta.GetPartitionStatsMeta() == nil {
		return nil
	}
	infos := c.meta.GetPartitionStatsMeta().ListAllPartitionStatsInfos()
	for _, info := range infos {
		collInfo := c.meta.(*meta).GetCollection(info.GetCollectionID())
		if collInfo == nil {
			unusedPartStats = append(unusedPartStats, info)
			continue
		}
		channel := fmt.Sprintf("%d/%d/%s", info.CollectionID, info.PartitionID, info.VChannel)
		if _, ok := channelPartitionStatsInfos[channel]; !ok {
			channelPartitionStatsInfos[channel] = make([]*datapb.PartitionStatsInfo, 0)
		}
		channelPartitionStatsInfos[channel] = append(channelPartitionStatsInfos[channel], info)
	}
	log.Debug("channels with PartitionStats meta", zap.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		log.Debug("collection has been dropped, remove partition stats",
			zap.Int64("collID", info.GetCollectionID()))
		if err := c.meta.CleanPartitionStatsInfo(info); err != nil {
			log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
			return err
		}
	}

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		log.Debug("PartitionStats in channel", zap.String("channel", channel), zap.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				if err := c.meta.CleanPartitionStatsInfo(info); err != nil {
					log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
					return err
				}
			}
		}
	}
	return nil
}

func (c *compactionPlanHandler) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *compactionPlanHandler) removeTasksByChannel(channel string) {
	log.Info("removing tasks by channel", zap.String("channel", channel))
	c.queueTasks.RemoveAll(func(task CompactionTask) bool {
		if task.GetChannel() == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.GetPlanID()),
				zap.Int64("node", task.GetNodeID()),
			)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", task.GetNodeID()), task.GetType().String(), metrics.Pending).Dec()
			return true
		}
		return false
	})

	c.executingGuard.Lock()
	for id, task := range c.executingTasks {
		log.Info("Compaction handler removing tasks by channel",
			zap.String("channel", channel), zap.Int64("planID", id), zap.Any("task_channel", task.GetChannel()))
		if task.GetChannel() == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.GetPlanID()),
				zap.Int64("node", task.GetNodeID()),
			)
			delete(c.executingTasks, id)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", task.GetNodeID()), task.GetType().String(), metrics.Executing).Dec()
		}
	}
	c.executingGuard.Unlock()
}

func (c *compactionPlanHandler) submitTask(t CompactionTask) error {
	_, span := otel.Tracer(typeutil.DataCoordRole).Start(context.Background(), fmt.Sprintf("Compaction-%s", t.GetType()))
	t.SetSpan(span)
	if err := c.queueTasks.Enqueue(t); err != nil {
		return err
	}
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetType().String(), metrics.Pending).Inc()
	return nil
}

// restoreTask used to restore Task from etcd
func (c *compactionPlanHandler) restoreTask(t CompactionTask) {
	_, span := otel.Tracer(typeutil.DataCoordRole).Start(context.Background(), fmt.Sprintf("Compaction-%s", t.GetType()))
	t.SetSpan(span)
	c.executingGuard.Lock()
	c.executingTasks[t.GetPlanID()] = t
	c.executingGuard.Unlock()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetNodeID()), t.GetType().String(), metrics.Executing).Inc()
}

// getCompactionTask return compaction
func (c *compactionPlanHandler) getCompactionTask(planID int64) CompactionTask {
	var t CompactionTask = nil
	c.queueTasks.ForEach(func(task CompactionTask) {
		if task.GetPlanID() == planID {
			t = task
		}
	})
	if t != nil {
		return t
	}

	c.executingGuard.RLock()
	defer c.executingGuard.RUnlock()
	t = c.executingTasks[planID]
	return t
}

func (c *compactionPlanHandler) enqueueCompaction(task *datapb.CompactionTask) error {
	log := log.With(zap.Int64("planID", task.GetPlanID()), zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.String("type", task.GetType().String()))
	t, err := c.createCompactTask(task)
	if err != nil {
		// Conflict is normal
		if errors.Is(err, merr.ErrCompactionPlanConflict) {
			log.RatedInfo(60, "Failed to create compaction task, compaction plan conflict", zap.Error(err))
		} else {
			log.Warn("Failed to create compaction task, unable to create compaction task", zap.Error(err))
		}
		return err
	}

	t.SetTask(t.ShadowClone(setStartTime(time.Now().Unix())))
	err = t.SaveTaskMeta()
	if err != nil {
		c.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
		log.Warn("Failed to enqueue compaction task, unable to save task meta", zap.Error(err))
		return err
	}
	if err = c.submitTask(t); err != nil {
		log.Warn("submit compaction task failed", zap.Error(err))
		c.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
		return err
	}
	log.Info("Compaction plan submitted")
	return nil
}

// set segments compacting, one segment can only participate one compactionTask
func (c *compactionPlanHandler) createCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	var task CompactionTask
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		task = &mixCompactionTask{
			CompactionTask: t,
			meta:           c.meta,
			sessions:       c.sessions,
		}
	case datapb.CompactionType_Level0DeleteCompaction:
		task = &l0CompactionTask{
			CompactionTask: t,
			meta:           c.meta,
			sessions:       c.sessions,
		}
	case datapb.CompactionType_ClusteringCompaction:
		task = newClusteringCompactionTask(t, c.meta, c.sessions, c.handler, c.analyzeScheduler)
	default:
		return nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
	exist, succeed := c.meta.CheckAndSetSegmentsCompacting(t.GetInputSegments())
	if !exist {
		return nil, merr.WrapErrIllegalCompactionPlan("segment not exist")
	}
	if !succeed {
		return nil, merr.WrapErrCompactionPlanConflict("segment is compacting")
	}
	return task, nil
}

func assignNodeID(slots map[int64]int64, t CompactionTask) int64 {
	if len(slots) == 0 {
		return NullNodeID
	}

	nodeID, useSlot := pickAnyNode(slots, t)
	if nodeID == NullNodeID {
		log.Info("compactionHandler cannot find datanode for compaction task",
			zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()), zap.String("vchannel", t.GetChannel()))
		return NullNodeID
	}
	err := t.SetNodeID(nodeID)
	if err != nil {
		log.Info("compactionHandler assignNodeID failed",
			zap.Int64("planID", t.GetPlanID()), zap.String("vchannel", t.GetChannel()), zap.Error(err))
		return NullNodeID
	}
	// update the input nodeSlots
	slots[nodeID] = slots[nodeID] - useSlot
	log.Info("compactionHandler assignNodeID success",
		zap.Int64("planID", t.GetPlanID()), zap.String("vchannel", t.GetChannel()), zap.Any("nodeID", nodeID))
	return nodeID
}

// checkCompaction retrieves executing tasks and calls each task's Process() method
// to evaluate its state and progress through the state machine.
// Completed tasks are removed from executingTasks.
// Tasks that fail or timeout are moved from executingTasks to cleaningTasks,
// where task-specific clean logic is performed asynchronously.
func (c *compactionPlanHandler) checkCompaction() error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	// Assign node id if needed
	var slots map[int64]int64
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if t.NeedReAssignNodeID() {
			if slots == nil {
				slots = c.cluster.QuerySlots()
			}
			id := assignNodeID(slots, t)
			if id == NullNodeID {
				break
			}
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetType().String(), metrics.Executing).Dec()
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetNodeID()), t.GetType().String(), metrics.Executing).Inc()
		}
	}
	c.executingGuard.RUnlock()

	var finishedTasks []CompactionTask
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		c.checkDelay(t)
		finished := t.Process()
		if finished {
			finishedTasks = append(finishedTasks, t)
		}
	}
	c.executingGuard.RUnlock()

	// delete all finished
	c.executingGuard.Lock()
	for _, t := range finishedTasks {
		delete(c.executingTasks, t.GetPlanID())
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetNodeID()), t.GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetNodeID()), t.GetType().String(), metrics.Done).Inc()
	}
	c.executingGuard.Unlock()

	// insert task need to clean
	c.cleaningGuard.Lock()
	for _, t := range finishedTasks {
		if t.GetState() == datapb.CompactionTaskState_failed ||
			t.GetState() == datapb.CompactionTaskState_timeout ||
			t.GetState() == datapb.CompactionTaskState_completed {
			log.Ctx(context.TODO()).Info("task need to clean",
				zap.Int64("collectionID", t.GetCollectionID()),
				zap.Int64("planID", t.GetPlanID()),
				zap.String("state", t.GetState().String()))
			c.cleaningTasks[t.GetPlanID()] = t
		}
	}
	c.cleaningGuard.Unlock()

	return nil
}

// cleanFailedTasks performs task define Clean logic
// while compactionPlanHandler.Clean is to do garbage collection for cleaned tasks
func (c *compactionPlanHandler) cleanFailedTasks() {
	c.cleaningGuard.RLock()
	cleanedTasks := make([]CompactionTask, 0)
	for _, t := range c.cleaningTasks {
		clean := t.Clean()
		if clean {
			cleanedTasks = append(cleanedTasks, t)
		}
	}
	c.cleaningGuard.RUnlock()
	c.cleaningGuard.Lock()
	for _, t := range cleanedTasks {
		delete(c.cleaningTasks, t.GetPlanID())
	}
	c.cleaningGuard.Unlock()
}

func pickAnyNode(nodeSlots map[int64]int64, task CompactionTask) (nodeID int64, useSlot int64) {
	nodeID = NullNodeID
	var maxSlots int64 = -1

	switch task.GetType() {
	case datapb.CompactionType_ClusteringCompaction:
		useSlot = paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64()
	case datapb.CompactionType_MixCompaction:
		useSlot = paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
	case datapb.CompactionType_Level0DeleteCompaction:
		useSlot = paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
	}

	for id, slots := range nodeSlots {
		if slots >= useSlot && slots > maxSlots {
			nodeID = id
			maxSlots = slots
		}
	}

	return nodeID, useSlot
}

func (c *compactionPlanHandler) pickShardNode(nodeSlots map[int64]int64, t CompactionTask) int64 {
	nodeID, err := c.chManager.FindWatcher(t.GetChannel())
	if err != nil {
		log.Info("failed to find watcher", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return NullNodeID
	}

	if nodeSlots[nodeID] > 0 {
		return nodeID
	}
	return NullNodeID
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.queueTasks.Len() >= c.queueTasks.capacity
}

func (c *compactionPlanHandler) checkDelay(t CompactionTask) {
	log := log.Ctx(context.TODO()).WithRateGroup("compactionPlanHandler.checkDelay", 1.0, 60.0)
	maxExecDuration := maxCompactionTaskExecutionDuration[t.GetType()]
	startTime := time.Unix(t.GetStartTime(), 0)
	execDuration := time.Since(startTime)
	if execDuration >= maxExecDuration {
		log.RatedWarn(60, "compaction task is delay",
			zap.Int64("planID", t.GetPlanID()),
			zap.String("type", t.GetType().String()),
			zap.String("state", t.GetState().String()),
			zap.String("vchannel", t.GetChannel()),
			zap.Int64("nodeID", t.GetNodeID()),
			zap.Time("startTime", startTime),
			zap.Duration("execDuration", execDuration))
	}
}

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}
