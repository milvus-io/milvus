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
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// maxConcurrentCleanups bounds the cleanup fan-out. Cleanup is metadata work
// that may first wait out a worker callback, so it needs a ceiling, but it is
// also on the path that frees input segments, so the ceiling is not tight.
const maxConcurrentCleanups = 16

var maxCompactionTaskExecutionDuration = map[datapb.CompactionType]time.Duration{
	datapb.CompactionType_MixCompaction:               30 * time.Minute,
	datapb.CompactionType_Level0DeleteCompaction:      30 * time.Minute,
	datapb.CompactionType_ClusteringCompaction:        60 * time.Minute,
	datapb.CompactionType_SortCompaction:              20 * time.Minute,
	datapb.CompactionType_BumpSchemaVersionCompaction: 30 * time.Minute,
}

type CompactionInspector interface {
	start()
	stop()
	// enqueueCompaction start to enqueue compaction task and return immediately
	enqueueCompaction(task *datapb.CompactionTask) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksNumBySignalID(signalID int64) int
	getCompactionInfo(ctx context.Context, signalID int64) *compactionInfo
	removeTasksByChannel(channel string)
	getCompactionTasksNum(filters ...compactionTaskFilter) int
}

var _ CompactionInspector = (*compactionInspector)(nil)

type compactionInfo struct {
	state        commonpb.CompactionState
	executingCnt int
	completedCnt int
	failedCnt    int
	timeoutCnt   int
	mergeInfos   map[int64]*milvuspb.CompactionMergeInfo
}

type compactionInspector struct {
	queueTasks *CompactionQueue

	executingGuard lock.RWMutex
	executingTasks map[int64]CompactionTask // planID -> task

	cleaningGuard lock.RWMutex
	cleaningTasks map[int64]CompactionTask // planID -> task
	// cleaningInFlight holds the planIDs whose cleanup goroutine has not
	// finished, so a slow cleanup is not re-dispatched every schedule round.
	cleaningInFlight *typeutil.ConcurrentSet[int64]
	// cleanupLimiter caps how many cleanups run at once.
	cleanupLimiter chan struct{}
	// pendingWorkerDrops carries recovered terminal tasks whose worker-side plan
	// still needs dropping. Collected during loadMeta, drained after start() so
	// an unresponsive DataNode cannot delay DataCoord readiness.
	//
	// Only start() drains this, so with compaction disabled the drops are never
	// sent -- acceptable, since nothing is producing new worker entries either.
	// stop() waits for the drain, which can cost one in-flight RPC
	// (dataCoord.requestTimeoutSeconds); the loop checks stopCh between drops so
	// it gives up after that one rather than working through the backlog.
	pendingWorkerDrops []CompactionTask

	meta             CompactionMeta
	allocator        allocator.Allocator
	analyzeScheduler task.GlobalScheduler
	handler          Handler
	cluster          session.Cluster
	scheduler        task.GlobalScheduler
	ievm             IndexEngineVersionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func (c *compactionInspector) getCompactionInfo(ctx context.Context, triggerID int64) *compactionInfo {
	tasks := c.meta.GetCompactionTasksByTriggerID(ctx, triggerID)
	return summaryCompactionState(triggerID, tasks)
}

func summaryCompactionState(triggerID int64, tasks []*datapb.CompactionTask) *compactionInfo {
	ret := &compactionInfo{}
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, analyzingCnt, indexingCnt, cleanedCnt, metaSavedCnt, stats int
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
		case datapb.CompactionTaskState_statistic:
			stats++
		default:
		}
		mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
	}

	ret.executingCnt = executingCnt + pipeliningCnt + analyzingCnt + indexingCnt + metaSavedCnt + stats
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt
	ret.mergeInfos = mergeInfos

	if ret.executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}

	mlog.Info(context.TODO(), "compaction states",
		mlog.Int64("triggerID", triggerID),
		mlog.String("state", ret.state.String()),
		mlog.Int("executingCnt", executingCnt),
		mlog.Int("pipeliningCnt", pipeliningCnt),
		mlog.Int("completedCnt", completedCnt),
		mlog.Int("failedCnt", failedCnt),
		mlog.Int("timeoutCnt", timeoutCnt),
		mlog.Int("analyzingCnt", analyzingCnt),
		mlog.Int("indexingCnt", indexingCnt),
		mlog.Int("cleanedCnt", cleanedCnt),
		mlog.Int("metaSavedCnt", metaSavedCnt))
	return ret
}

func (c *compactionInspector) getCompactionTasksNumBySignalID(triggerID int64) int {
	cnt := 0
	c.queueTasks.ForEach(func(ct CompactionTask) {
		if ct.GetTaskProto().GetTriggerID() == triggerID {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if t.GetTaskProto().GetTriggerID() == triggerID {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

func newCompactionInspector(meta CompactionMeta,
	allocator allocator.Allocator, handler Handler, cluster session.Cluster, scheduler task.GlobalScheduler, analyzeScheduler task.GlobalScheduler, ievm IndexEngineVersionManager,
) *compactionInspector {
	capacity := paramtable.Get().DataCoordCfg.CompactionTaskQueueCapacity.GetAsInt()
	return &compactionInspector{
		queueTasks:       NewCompactionQueue(capacity, getPrioritizer()),
		meta:             meta,
		allocator:        allocator,
		stopCh:           make(chan struct{}),
		executingTasks:   make(map[int64]CompactionTask),
		cleaningTasks:    make(map[int64]CompactionTask),
		cleaningInFlight: typeutil.NewConcurrentSet[int64](),
		cleanupLimiter:   make(chan struct{}, maxConcurrentCleanups),
		handler:          handler,
		cluster:          cluster,
		scheduler:        scheduler,
		analyzeScheduler: analyzeScheduler,
		ievm:             ievm,
	}
}

func (c *compactionInspector) checkSchedule() {
	err := c.checkCompaction()
	if err != nil {
		mlog.Info(context.TODO(), "fail to update compaction", mlog.Err(err))
	}
	c.cleanFailedTasks()
	c.schedule()
}

func (c *compactionInspector) schedule() []CompactionTask {
	selected := make([]CompactionTask, 0)
	if c.queueTasks.Len() == 0 {
		return selected
	}

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	clusterLabelExcludes := typeutil.NewSet[string]()

	exclude := func(t CompactionTask) {
		switch t.GetTaskProto().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			l0ChannelExcludes.Insert(t.GetTaskProto().GetChannel())
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			mixChannelExcludes.Insert(t.GetTaskProto().GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
		case datapb.CompactionType_ClusteringCompaction:
			clusterChannelExcludes.Insert(t.GetTaskProto().GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
		}
	}

	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		exclude(t)
	}
	c.executingGuard.RUnlock()

	// A task awaiting cleanup has left executingTasks but still owns its input
	// segments until cleanup releases them, and a worker callback for it may
	// still be submitting results. Keep excluding its channel and label, or a
	// same-label task could start while the old one is still finishing.
	c.cleaningGuard.RLock()
	for _, t := range c.cleaningTasks {
		exclude(t)
	}
	c.cleaningGuard.RUnlock()

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
	// 2. no available slots
	for {
		t, err := c.queueTasks.Dequeue()
		if err != nil {
			break // 1. no more task to schedule
		}

		switch t.GetTaskProto().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if mixChannelExcludes.Contain(t.GetTaskProto().GetChannel()) ||
				clusterChannelExcludes.Contain(t.GetTaskProto().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			l0ChannelExcludes.Insert(t.GetTaskProto().GetChannel())
			selected = append(selected, t)
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			// BumpSchemaVersionCompaction shares the same exclusion rules as Mix/Sort:
			// - Channel-level mutual exclusion with L0 (L0 may write delta logs to any segment on the channel)
			// - Label-level exclusion registered for Clustering awareness
			if l0ChannelExcludes.Contain(t.GetTaskProto().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			mixChannelExcludes.Insert(t.GetTaskProto().GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		case datapb.CompactionType_ClusteringCompaction:
			if l0ChannelExcludes.Contain(t.GetTaskProto().GetChannel()) ||
				mixLabelExcludes.Contain(t.GetLabel()) ||
				clusterLabelExcludes.Contain(t.GetLabel()) {
				excluded = append(excluded, t)
				continue
			}
			clusterChannelExcludes.Insert(t.GetTaskProto().GetChannel())
			clusterLabelExcludes.Insert(t.GetLabel())
			selected = append(selected, t)
		}

		c.executingGuard.Lock()
		c.executingTasks[t.GetTaskProto().GetPlanID()] = t
		c.scheduler.Enqueue(t)
		mlog.Info(context.TODO(), "compaction task enqueued",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.String("type", t.GetTaskProto().GetType().String()),
			mlog.String("channel", t.GetTaskProto().GetChannel()),
			mlog.String("label", t.GetLabel()),
			mlog.Int64s("inputSegments", t.GetTaskProto().GetInputSegments()),
		)
		c.executingGuard.Unlock()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Executing).Inc()
	}
	return selected
}

func (c *compactionInspector) start() {
	c.stopWg.Add(2)
	go c.loopSchedule()
	go c.loopClean()
	if len(c.pendingWorkerDrops) > 0 {
		c.stopWg.Add(1)
		go c.dropRecoveredTasksOnWorker()
	}
}

func (c *compactionInspector) loadMeta() error {
	// TODO: make it compatible to all types of compaction with persist meta
	triggers := c.meta.GetCompactionTasks(context.TODO())
	cleanedSortInputs := make(map[int64]struct{})
	activeSortInputs := make(map[int64]struct{})
	recordSortInputs := func(target map[int64]struct{}, task *datapb.CompactionTask) {
		if task.GetType() != datapb.CompactionType_SortCompaction {
			return
		}
		for _, segmentID := range task.GetInputSegments() {
			target[segmentID] = struct{}{}
		}
	}
	for _, tasks := range triggers {
		for _, task := range tasks {
			if isCompactionTaskCleaned(task) {
				// Older DataCoord versions could persist cleaned before restoring a
				// rejected sort input's visibility. The task metadata is the durable
				// evidence that this invisible segment is safe to publish. Do not scan
				// arbitrary invisible originals here: a freshly flushed segment has
				// the same shape while it legitimately waits for sort compaction.
				if task.GetState() == datapb.CompactionTaskState_cleaned &&
					task.GetType() == datapb.CompactionType_SortCompaction {
					recordSortInputs(cleanedSortInputs, task)
				}
				mlog.Info(context.TODO(), "compactionInspector loadMeta abandon compactionTask",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", task.GetState().String()))
				continue
			} else {
				t, err := c.createCompactTask(task)
				if err != nil {
					mlog.Info(context.TODO(), "compactionInspector loadMeta create compactionTask failed, try to clean it",
						mlog.Int64("planID", task.GetPlanID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", task.GetState().String()),
						mlog.Err(err),
					)
					// ignore the drop error
					c.meta.DropCompactionTask(context.TODO(), task)
					continue
				}
				if isMixOrSortCompaction(task.GetType()) && compactionTaskNeedsCleanup(task.GetState()) {
					if !t.Clean() {
						return merr.WrapErrServiceInternalMsg(
							"failed to clean recovered terminal %s task %d in state %s",
							task.GetType().String(), task.GetPlanID(), task.GetState().String())
					}
					c.pendingWorkerDrops = append(c.pendingWorkerDrops, t)
					mlog.Info(context.TODO(), "compactionInspector loadMeta cleaned recovered terminal task",
						mlog.Int64("planID", task.GetPlanID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", task.GetState().String()))
					continue
				}
				if t.NeedReAssignNodeID() {
					if err = c.submitTask(t); err != nil {
						mlog.Info(context.TODO(), "compactionInspector loadMeta submit task failed, try to clean it",
							mlog.Int64("planID", task.GetPlanID()),
							mlog.String("type", task.GetType().String()),
							mlog.String("state", task.GetState().String()),
							mlog.Err(err),
						)
						// ignore the drop error
						c.meta.DropCompactionTask(context.Background(), task)
						continue
					}
					recordSortInputs(activeSortInputs, task)
					mlog.Info(context.TODO(), "compactionInspector loadMeta submitTask",
						mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
						mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
						mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", t.GetTaskProto().GetState().String()))
				} else {
					c.restoreTask(t)
					recordSortInputs(activeSortInputs, task)
					mlog.Info(context.TODO(), "compactionInspector loadMeta restoreTask",
						mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
						mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
						mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", t.GetTaskProto().GetState().String()))
				}
			}
		}
	}
	return c.restoreCleanedSortInputVisibility(cleanedSortInputs, activeSortInputs)
}

// dropRecoveredTasksOnWorker releases the worker-side entries of the recovered
// terminal tasks that this inspector cleaned synchronously instead of handing
// to the scheduler. DropCompaction is the only path that removes a non-L0 task
// from the DataNode executor, and the executor keeps a terminal entry -- result
// binlog lists included -- resident until it is dropped, so skipping the
// scheduler would leak one entry per recovered task on every DataCoord restart.
//
// This runs after start() rather than inside loadMeta: each drop is an RPC
// bounded only by dataCoord.requestTimeoutSeconds, and DataCoord readiness must
// not depend on an unresponsive DataNode.
//
// Best-effort by nature. The DataNode drops a terminal entry but keeps one that
// is still executing (datanode/compactor/executor.go RemoveTask) while still
// reporting success, so an entry for a task the worker is still running stays
// until that DataNode restarts. Closing that gap needs a durable, retryable
// worker-cleanup state on the DataNode side.
func (c *compactionInspector) dropRecoveredTasksOnWorker() {
	defer c.stopWg.Done()
	for _, t := range c.pendingWorkerDrops {
		select {
		case <-c.stopCh:
			return
		default:
		}
		c.dropTaskOnWorker(t)
	}
}

// dropTaskOnWorker releases the worker-side entry of a task this inspector has
// finished with. Best-effort by nature: the DataNode defers a drop it cannot
// honor yet, but a request lost to an unreachable node is not retried.
func (c *compactionInspector) dropTaskOnWorker(t CompactionTask) {
	if c.cluster == nil || t.GetTaskProto().GetNodeID() <= 0 {
		return
	}
	t.DropTaskOnWorker(c.cluster)
}

func isMixOrSortCompaction(compactionType datapb.CompactionType) bool {
	return compactionType == datapb.CompactionType_MixCompaction ||
		compactionType == datapb.CompactionType_SortCompaction
}

func compactionTaskNeedsCleanup(state datapb.CompactionTaskState) bool {
	return state == datapb.CompactionTaskState_completed ||
		state == datapb.CompactionTaskState_failed ||
		state == datapb.CompactionTaskState_timeout
}

func (c *compactionInspector) restoreCleanedSortInputVisibility(
	cleanedSortInputs map[int64]struct{},
	activeSortInputs map[int64]struct{},
) error {
	segmentIDs := make([]int64, 0, len(cleanedSortInputs))
	for segmentID := range cleanedSortInputs {
		if _, active := activeSortInputs[segmentID]; !active {
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	if len(segmentIDs) == 0 {
		return nil
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

	operators := make([]UpdateOperator, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		operators = append(operators, RestoreSegmentVisibilityForTerminatedSortCompaction(segmentID))
	}
	ctx := context.TODO()
	if err := c.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
		mlog.Warn(ctx, "failed to restore cleaned sort compaction input visibility",
			mlog.Int64s("segmentIDs", segmentIDs),
			mlog.Err(err))
		return merr.Wrap(err, "restore cleaned sort compaction input visibility")
	}
	mlog.Info(ctx, "restored cleaned sort compaction input visibility",
		mlog.Int64s("segmentIDs", segmentIDs))
	return nil
}

func (c *compactionInspector) loopSchedule() {
	interval := paramtable.Get().DataCoordCfg.CompactionScheduleInterval.GetAsDuration(time.Millisecond)
	mlog.Info(context.TODO(), "compactionInspector start loop schedule", mlog.Duration("schedule interval", interval))
	defer c.stopWg.Done()

	scheduleTicker := time.NewTicker(interval)
	defer scheduleTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(context.TODO(), "compactionInspector quit loop schedule")
			return

		case <-scheduleTicker.C:
			c.checkSchedule()
		}
	}
}

func (c *compactionInspector) loopClean() {
	interval := Params.DataCoordCfg.CompactionGCIntervalInSeconds.GetAsDuration(time.Second)
	mlog.Info(context.TODO(), "compactionInspector start clean check loop", mlog.Any("gc interval", interval))
	defer c.stopWg.Done()
	cleanTicker := time.NewTicker(interval)
	defer cleanTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(context.TODO(), "Compaction inspector quit loopClean")
			return
		case <-cleanTicker.C:
			c.Clean()
		}
	}
}

func (c *compactionInspector) Clean() {
	c.cleanCompactionTaskMeta()
	c.cleanPartitionStats()
}

func (c *compactionInspector) cleanCompactionTaskMeta() {
	// gc clustering compaction tasks
	triggers := c.meta.GetCompactionTasks(context.TODO())
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State == datapb.CompactionTaskState_cleaned {
				duration := time.Since(time.Unix(task.StartTime, 0)).Seconds()
				if duration > Params.DataCoordCfg.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds() {
					// try best to delete meta
					err := c.meta.DropCompactionTask(context.TODO(), task)
					mlog.Debug(context.TODO(), "drop compaction task meta", mlog.Int64("planID", task.PlanID))
					if err != nil {
						mlog.Warn(context.TODO(), "fail to drop task", mlog.Int64("planID", task.PlanID), mlog.Err(err))
					}
				}
			}
		}
	}
}

func (c *compactionInspector) cleanPartitionStats() error {
	mlog.Debug(context.TODO(), "start gc partitionStats meta and files")
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
	mlog.Debug(context.TODO(), "channels with PartitionStats meta", mlog.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		mlog.Debug(context.TODO(), "collection has been dropped, remove partition stats",
			mlog.Int64("collID", info.GetCollectionID()))
		if err := c.meta.CleanPartitionStatsInfo(context.TODO(), info); err != nil {
			mlog.Warn(context.TODO(), "gcPartitionStatsInfo fail", mlog.Err(err))
			return err
		}
	}

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		mlog.Debug(context.TODO(), "PartitionStats in channel", mlog.String("channel", channel), mlog.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				if err := c.meta.CleanPartitionStatsInfo(context.TODO(), info); err != nil {
					mlog.Warn(context.TODO(), "gcPartitionStatsInfo fail", mlog.Err(err))
					return err
				}
			}
		}
	}
	return nil
}

func (c *compactionInspector) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *compactionInspector) removeTasksByChannel(channel string) {
	mlog.Info(context.TODO(), "removing tasks by channel", mlog.String("channel", channel))
	c.queueTasks.RemoveAll(func(task CompactionTask) bool {
		if task.GetTaskProto().GetChannel() == channel {
			mlog.Info(context.TODO(), "Compaction inspector removing tasks by channel",
				mlog.String("channel", channel),
				mlog.Int64("planID", task.GetTaskProto().GetPlanID()),
				mlog.Int64("node", task.GetTaskProto().GetNodeID()),
			)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", task.GetTaskProto().GetNodeID()), task.GetTaskProto().GetType().String(), metrics.Pending).Dec()
			return true
		}
		return false
	})

	c.executingGuard.Lock()
	for id, task := range c.executingTasks {
		mlog.Info(context.TODO(), "Compaction inspector removing tasks by channel",
			mlog.String("channel", channel), mlog.Int64("planID", id), mlog.Any("task_channel", task.GetTaskProto().GetChannel()))
		if task.GetTaskProto().GetChannel() == channel {
			mlog.Info(context.TODO(), "Compaction inspector removing tasks by channel",
				mlog.String("channel", channel),
				mlog.Int64("planID", task.GetTaskProto().GetPlanID()),
				mlog.Int64("node", task.GetTaskProto().GetNodeID()),
			)
			delete(c.executingTasks, id)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", task.GetTaskProto().GetNodeID()), task.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		}
	}
	c.executingGuard.Unlock()
}

func (c *compactionInspector) submitTask(t CompactionTask) error {
	if err := c.queueTasks.Enqueue(t); err != nil {
		return err
	}
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
	return nil
}

// restoreTask used to restore Task from etcd
func (c *compactionInspector) restoreTask(t CompactionTask) {
	c.executingGuard.Lock()
	c.executingTasks[t.GetTaskProto().GetPlanID()] = t
	c.scheduler.Enqueue(t)
	c.executingGuard.Unlock()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Executing).Inc()
}

// getCompactionTask return compaction
func (c *compactionInspector) getCompactionTask(planID int64) CompactionTask {
	var t CompactionTask = nil
	c.queueTasks.ForEach(func(task CompactionTask) {
		if task.GetTaskProto().GetPlanID() == planID {
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

func (c *compactionInspector) enqueueCompaction(task *datapb.CompactionTask) error {
	log := mlog.With(mlog.Int64("planID", task.GetPlanID()), mlog.Int64("triggerID", task.GetTriggerID()), mlog.FieldCollectionID(task.GetCollectionID()), mlog.String("type", task.GetType().String()))
	t, err := c.createCompactTask(task)
	if err != nil {
		// Conflict is normal
		if errors.Is(err, merr.ErrCompactionPlanConflict) {
			log.RatedInfo(context.TODO(), rate.Limit(60), "Failed to create compaction task, compaction plan conflict", mlog.Err(err))
		} else {
			log.Warn(context.TODO(), "Failed to create compaction task, unable to create compaction task", mlog.Err(err))
		}
		return err
	}

	t.SetTask(t.ShadowClone(setStartTime(time.Now().Unix())))
	err = t.SaveTaskMeta()
	if err != nil {
		c.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
		log.Warn(context.TODO(), "Failed to enqueue compaction task, unable to save task meta", mlog.Err(err))
		return err
	}
	if err = c.submitTask(t); err != nil {
		log.Warn(context.TODO(), "submit compaction task failed", mlog.Err(err))
		c.meta.SetSegmentsCompacting(context.Background(), t.GetTaskProto().GetInputSegments(), false)
		return err
	}
	log.Info(context.TODO(), "Compaction plan submitted")
	return nil
}

// set segments compacting, one segment can only participate one compactionTask
func (c *compactionInspector) createCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	var task CompactionTask
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction:
		task = newMixCompactionTask(t, c.allocator, c.meta, c.ievm)
	case datapb.CompactionType_Level0DeleteCompaction:
		task = newL0CompactionTask(t, c.allocator, c.meta)
	case datapb.CompactionType_ClusteringCompaction:
		task = newClusteringCompactionTask(t, c.allocator, c.meta, c.handler, c.analyzeScheduler, c.ievm)
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		task = newBumpSchemaVersionTask(t, c.allocator, c.meta, c.ievm)
	default:
		return nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
	exist, succeed := c.meta.CheckAndSetSegmentsCompacting(context.TODO(), t.GetInputSegments())
	if !exist {
		return nil, merr.WrapErrIllegalCompactionPlan("segment not exist")
	}
	if !succeed {
		return nil, merr.WrapErrCompactionPlanConflict("segment is compacting")
	}
	return task, nil
}

// checkCompaction retrieves executing tasks and calls each task's Process() method
// to evaluate its state and progress through the state machine.
// Completed tasks are removed from executingTasks.
// Tasks that fail or timeout are moved from executingTasks to cleaningTasks,
// where task-specific clean logic is performed asynchronously.
func (c *compactionInspector) checkCompaction() error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	type finishedCompactionTask struct {
		task         CompactionTask
		needsCleanup bool
	}
	var finishedTasks []finishedCompactionTask
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		c.checkDelay(t)
		// Decide cleanup from immutable snapshots taken around Process, never from
		// a later re-read. Between this loop and the cleaningTasks insert below the
		// lock is released, and an in-flight scheduler callback can rewrite a
		// terminal state back to pipelining when it fails to probe its worker
		// (compaction_task_l0.go:153, compaction_task_mix.go:144). A task removed
		// from executingTasks without entering cleaningTasks is never cleaned, so
		// its input segments stay isCompacting until DataCoord restarts.
		// The scheduler owns this task's state; borrow it under its per-task
		// lock so Process cannot interleave with a worker callback. Never wait:
		// a task whose callback is mid-RPC simply gets its round skipped.
		planID := t.GetTaskProto().GetPlanID()
		var stateBeforeProcess, stateAfterProcess datapb.CompactionTaskState
		var finished bool
		if !c.scheduler.TryUpdate(planID, func() {
			stateBeforeProcess = t.GetTaskProto().GetState()
			finished = t.Process()
			stateAfterProcess = t.GetTaskProto().GetState()
		}) {
			continue
		}
		needsCleanup := compactionTaskNeedsCleanup(stateBeforeProcess) ||
			compactionTaskNeedsCleanup(stateAfterProcess)
		if finished || needsCleanup {
			finishedTasks = append(finishedTasks, finishedCompactionTask{
				task:         t,
				needsCleanup: needsCleanup,
			})
		}
	}
	c.executingGuard.RUnlock()

	// delete all finished
	c.executingGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		delete(c.executingTasks, t.GetTaskProto().GetPlanID())
		mlog.Info(context.TODO(), "compaction task finished",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.String("type", t.GetTaskProto().GetType().String()),
			mlog.String("state", t.GetTaskProto().GetState().String()),
			mlog.String("channel", t.GetTaskProto().GetChannel()),
			mlog.String("label", t.GetLabel()),
			mlog.FieldNodeID(t.GetTaskProto().GetNodeID()),
			mlog.Int64s("inputSegments", t.GetTaskProto().GetInputSegments()),
			mlog.String("reason", t.GetTaskProto().GetFailReason()),
		)
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Done).Inc()
	}
	c.executingGuard.Unlock()

	// insert task need to clean
	c.cleaningGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		if finishedTask.needsCleanup {
			mlog.Info(context.TODO(), "task need to clean",
				mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
				mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
				mlog.String("state", t.GetTaskProto().GetState().String()))
			c.cleaningTasks[t.GetTaskProto().GetPlanID()] = t
		}
	}
	c.cleaningGuard.Unlock()

	return nil
}

// cleanFailedTasks performs task define Clean logic
// while compactionInspector.Clean is to do garbage collection for cleaned tasks
func (c *compactionInspector) cleanFailedTasks() {
	c.cleaningGuard.RLock()
	tasks := lo.Values(c.cleaningTasks)
	c.cleaningGuard.RUnlock()

	for _, t := range tasks {
		planID := t.GetTaskProto().GetPlanID()
		select {
		case <-c.stopCh:
			return
		default:
		}
		// Cleanup persists metadata and may have to wait out an in-flight worker
		// callback, so it must not run inline: the caller is the single
		// checkSchedule goroutine that also drives checkCompaction and schedule
		// for every other compaction task. One slow cleanup would stall them all.
		// cleaningInFlight keeps a task from being dispatched twice while its
		// previous attempt is still running.
		if !c.cleaningInFlight.Insert(planID) {
			continue
		}
		// Take the slot before spawning, not inside the goroutine: the queue holds
		// up to CompactionTaskQueueCapacity tasks, and acquiring afterwards would
		// bound only how many run at once, leaving that many goroutines parked on
		// the semaphore and draining at shutdown.
		select {
		case c.cleanupLimiter <- struct{}{}:
		default:
			c.cleaningInFlight.Remove(planID)
			continue
		}
		// In production this runs on the loopSchedule goroutine, which holds a
		// stopWg count of its own, so Add never races the Wait inside stop().
		c.stopWg.Add(1)
		go func(t CompactionTask, planID int64) {
			defer c.stopWg.Done()
			defer c.cleaningInFlight.Remove(planID)
			defer func() { <-c.cleanupLimiter }()
			// Finalize drops the task from dispatch first, so no further plan can
			// be handed to a worker, then runs cleanup under the same per-task
			// lock as the callbacks -- waiting for any in-flight one to drain.
			cleaned := false
			c.scheduler.Finalize(planID, func() { cleaned = t.Clean() })
			if !cleaned {
				return
			}
			// Finalize took the task out of dispatch, so the scheduler's own
			// terminal-state branch will never run DropTaskOnWorker for it. Send
			// the drop here instead, or the worker keeps the plan and its result
			// binlogs until that DataNode restarts. Outside the per-task lock: it
			// is an RPC, and the task is ours now.
			c.dropTaskOnWorker(t)
			c.cleaningGuard.Lock()
			delete(c.cleaningTasks, planID)
			c.cleaningGuard.Unlock()
		}(t, planID)
	}
}

// isFull return true if the task pool is full
func (c *compactionInspector) isFull() bool {
	return c.queueTasks.Len() >= c.queueTasks.capacity
}

func (c *compactionInspector) checkDelay(t CompactionTask) {
	maxExecDuration := maxCompactionTaskExecutionDuration[t.GetTaskProto().GetType()]
	startTime := time.Unix(t.GetTaskProto().GetStartTime(), 0)
	execDuration := time.Since(startTime)
	if execDuration >= maxExecDuration {
		mlog.RatedWarn(context.TODO(), rate.Limit(60), "compaction task is delay",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.String("type", t.GetTaskProto().GetType().String()),
			mlog.String("state", t.GetTaskProto().GetState().String()),
			mlog.FieldVChannel(t.GetTaskProto().GetChannel()),
			mlog.FieldNodeID(t.GetTaskProto().GetNodeID()),
			mlog.Time("startTime", startTime),
			mlog.Duration("execDuration", execDuration))
	}
}

func (c *compactionInspector) getCompactionTasksNum(filters ...compactionTaskFilter) int {
	cnt := 0
	isMatch := func(task CompactionTask) bool {
		for _, f := range filters {
			if !f(task) {
				return false
			}
		}
		return true
	}
	c.queueTasks.ForEach(func(task CompactionTask) {
		if isMatch(task) {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if isMatch(t) {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

type compactionTaskFilter func(task CompactionTask) bool

func CollectionIDCompactionTaskFilter(collectionID int64) compactionTaskFilter {
	return func(task CompactionTask) bool {
		return task.GetTaskProto().GetCollectionID() == collectionID
	}
}

func L0CompactionCompactionTaskFilter() compactionTaskFilter {
	return func(task CompactionTask) bool {
		return task.GetTaskProto().GetType() == datapb.CompactionType_Level0DeleteCompaction
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
