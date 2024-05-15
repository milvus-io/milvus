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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO this num should be determined by resources of datanode, for now, we set to a fixed value for simple
// TODO we should split compaction into different priorities, small compaction helps to merge segment, large compaction helps to handle delta and expiration of large segments
const (
	tsTimeout = uint64(1)
)

//go:generate mockery --name=compactionPlanContext --structname=MockCompactionPlanContext --output=./  --filename=mock_compaction_plan_context.go --with-expecter --inpackage
type compactionPlanContext interface {
	start()
	stop()
	// execCompactionPlan start to execute plan and return immediately
	execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan)
	// getCompaction return compaction task. If planId does not exist, return nil.
	getCompaction(planID int64) *compactionTask
	// updateCompaction set the compaction state to timeout or completed
	updateCompaction(ts Timestamp) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksBySignalID(signalID int64) []*compactionTask
	removeTasksByChannel(channel string)
}

type compactionTaskState int8

const (
	executing compactionTaskState = iota + 1
	pipelining
	completed
	failed
	timeout
)

var (
	errChannelNotWatched = errors.New("channel is not watched")
	errChannelInBuffer   = errors.New("channel is in buffer")
)

type CompactionMeta interface {
	SelectSegments(filters ...SegmentFilter) []*SegmentInfo
	GetHealthySegment(segID UniqueID) *SegmentInfo
	UpdateSegmentsInfo(operators ...UpdateOperator) error
	SetSegmentCompacting(segmentID int64, compacting bool)

	CompleteCompactionMutation(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error)
}

var _ CompactionMeta = (*meta)(nil)

type compactionTask struct {
	triggerInfo *compactionSignal
	plan        *datapb.CompactionPlan
	state       compactionTaskState
	dataNodeID  int64
	result      *datapb.CompactionPlanResult
	span        trace.Span
}

func (t *compactionTask) shadowClone(opts ...compactionTaskOpt) *compactionTask {
	task := &compactionTask{
		triggerInfo: t.triggerInfo,
		plan:        t.plan,
		state:       t.state,
		dataNodeID:  t.dataNodeID,
		span:        t.span,
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionPlanHandler struct {
	mu    lock.RWMutex
	plans map[int64]*compactionTask // planID -> task

	meta      CompactionMeta
	allocator allocator
	chManager ChannelManager
	scheduler Scheduler
	sessions  SessionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func newCompactionPlanHandler(cluster Cluster, sessions SessionManager, cm ChannelManager, meta CompactionMeta, allocator allocator,
) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:     make(map[int64]*compactionTask),
		chManager: cm,
		meta:      meta,
		sessions:  sessions,
		allocator: allocator,
		scheduler: NewCompactionScheduler(cluster),
	}
}

func (c *compactionPlanHandler) checkResult() {
	// deal results
	ts, err := c.GetCurrentTS()
	if err != nil {
		log.Warn("fail to check result", zap.Error(err))
		return
	}
	err = c.updateCompaction(ts)
	if err != nil {
		log.Warn("fail to update compaction", zap.Error(err))
		return
	}
}

func (c *compactionPlanHandler) GetCurrentTS() (Timestamp, error) {
	interval := Params.DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	ts, err := c.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
		return 0, err
	}
	return ts, nil
}

func (c *compactionPlanHandler) schedule() {
	// schedule queuing tasks
	tasks := c.scheduler.Schedule()
	if len(tasks) > 0 {
		c.notifyTasks(tasks)
		c.scheduler.LogStatus()
	}
}

func (c *compactionPlanHandler) start() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	c.stopCh = make(chan struct{})
	c.stopWg.Add(3)

	go func() {
		defer c.stopWg.Done()
		checkResultTicker := time.NewTicker(interval)
		log.Info("Compaction handler check result loop start", zap.Any("check result interval", interval))
		defer checkResultTicker.Stop()
		for {
			select {
			case <-c.stopCh:
				log.Info("compaction handler check result loop quit")
				return
			case <-checkResultTicker.C:
				c.checkResult()
			}
		}
	}()

	// saperate check results and schedule goroutine so that check results doesn't
	// influence the schedule
	go func() {
		defer c.stopWg.Done()
		scheduleTicker := time.NewTicker(2 * time.Second)
		defer scheduleTicker.Stop()
		log.Info("compaction handler start schedule")
		for {
			select {
			case <-c.stopCh:
				log.Info("Compaction handler quit schedule")
				return

			case <-scheduleTicker.C:
				c.schedule()
			}
		}
	}()

	go func() {
		defer c.stopWg.Done()
		cleanTicker := time.NewTicker(30 * time.Minute)
		defer cleanTicker.Stop()
		for {
			select {
			case <-c.stopCh:
				log.Info("Compaction handler quit clean")
				return
			case <-cleanTicker.C:
				c.Clean()
			}
		}
	}()
}

func (c *compactionPlanHandler) Clean() {
	current := tsoutil.GetCurrentTime()
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, task := range c.plans {
		if task.state == executing || task.state == pipelining {
			continue
		}
		// after timeout + 1h, the plan will be cleaned
		if c.isTimeout(current, task.plan.GetStartTime(), task.plan.GetTimeoutInSeconds()+60*60) {
			delete(c.plans, id)
		}
	}
}

func (c *compactionPlanHandler) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *compactionPlanHandler) removeTasksByChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, task := range c.plans {
		if task.triggerInfo.channel == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.plan.GetPlanID()),
				zap.Int64("node", task.dataNodeID),
			)
			c.scheduler.Finish(task.dataNodeID, task.plan)
			delete(c.plans, id)
		}
	}
}

func (c *compactionPlanHandler) updateTask(planID int64, opts ...compactionTaskOpt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if plan, ok := c.plans[planID]; ok {
		c.plans[planID] = plan.shadowClone(opts...)
	}
}

func (c *compactionPlanHandler) enqueuePlan(signal *compactionSignal, plan *datapb.CompactionPlan) {
	log := log.With(zap.Int64("planID", plan.GetPlanID()))
	c.setSegmentsCompacting(plan, true)

	_, span := otel.Tracer(typeutil.DataCoordRole).Start(context.Background(), fmt.Sprintf("Compaction-%s", plan.GetType()))

	task := &compactionTask{
		triggerInfo: signal,
		plan:        plan,
		state:       pipelining,
		span:        span,
	}
	c.mu.Lock()
	c.plans[plan.PlanID] = task
	c.mu.Unlock()

	c.scheduler.Submit(task)
	log.Info("Compaction plan submitted")
}

func (c *compactionPlanHandler) RefreshPlan(task *compactionTask) error {
	plan := task.plan
	log := log.With(zap.Int64("taskID", task.triggerInfo.id), zap.Int64("planID", plan.GetPlanID()))
	if plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
		// Fill in deltalogs for L0 segments
		for _, seg := range plan.GetSegmentBinlogs() {
			if seg.GetLevel() == datapb.SegmentLevel_L0 {
				segInfo := c.meta.GetHealthySegment(seg.GetSegmentID())
				if segInfo == nil {
					return merr.WrapErrSegmentNotFound(seg.GetSegmentID())
				}
				seg.Deltalogs = segInfo.GetDeltalogs()
			}
		}

		// Select sealed L1 segments for LevelZero compaction that meets the condition:
		// dmlPos < triggerInfo.pos
		// TODO: select L2 segments too
		sealedSegments := c.meta.SelectSegments(WithCollection(task.triggerInfo.collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
			return (task.triggerInfo.partitionID == -1 || info.GetPartitionID() == task.triggerInfo.partitionID) &&
				info.GetInsertChannel() == plan.GetChannel() &&
				isFlushState(info.GetState()) &&
				!info.isCompacting &&
				!info.GetIsImporting() &&
				info.GetLevel() != datapb.SegmentLevel_L0 &&
				info.GetDmlPosition().GetTimestamp() < task.triggerInfo.pos.GetTimestamp()
		}))
		if len(sealedSegments) == 0 {
			return errors.Errorf("Selected zero L1/L2 segments for the position=%v", task.triggerInfo.pos)
		}

		sealedSegBinlogs := lo.Map(sealedSegments, func(info *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
			return &datapb.CompactionSegmentBinlogs{
				SegmentID:           info.GetID(),
				FieldBinlogs:        nil,
				Field2StatslogPaths: info.GetStatslogs(),
				Deltalogs:           nil,
				InsertChannel:       info.GetInsertChannel(),
				Level:               info.GetLevel(),
				CollectionID:        info.GetCollectionID(),
				PartitionID:         info.GetPartitionID(),
			}
		})

		plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
		log.Info("Compaction handler refreshed level zero compaction plan",
			zap.Any("target position", task.triggerInfo.pos),
			zap.Any("target segments count", len(sealedSegBinlogs)))
		return nil
	}

	if plan.GetType() == datapb.CompactionType_MixCompaction {
		segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
		for _, seg := range plan.GetSegmentBinlogs() {
			info := c.meta.GetHealthySegment(seg.GetSegmentID())
			if info == nil {
				return merr.WrapErrSegmentNotFound(seg.GetSegmentID())
			}
			seg.Deltalogs = info.GetDeltalogs()
			segIDMap[seg.SegmentID] = info.GetDeltalogs()
		}
		log.Info("Compaction handler refreshed mix compaction plan", zap.Any("segID2DeltaLogs", segIDMap))
	}
	return nil
}

func (c *compactionPlanHandler) notifyTasks(tasks []*compactionTask) {
	for _, task := range tasks {
		// avoid closure capture iteration variable
		innerTask := task
		err := c.RefreshPlan(innerTask)
		if err != nil {
			c.updateTask(innerTask.plan.GetPlanID(), setState(failed), endSpan())
			c.scheduler.Finish(innerTask.dataNodeID, innerTask.plan)
			log.Warn("failed to refresh task",
				zap.Int64("plan", task.plan.PlanID),
				zap.Error(err))
			continue
		}
		getOrCreateIOPool().Submit(func() (any, error) {
			ctx := tracer.SetupSpan(context.Background(), innerTask.span)
			plan := innerTask.plan
			log := log.Ctx(ctx).With(zap.Int64("planID", plan.GetPlanID()), zap.Int64("nodeID", innerTask.dataNodeID))
			log.Info("Notify compaction task to DataNode")
			ts, err := c.allocator.allocTimestamp(ctx)
			if err != nil {
				log.Warn("Alloc start time for CompactionPlan failed", zap.Error(err))
				// update plan ts to TIMEOUT ts
				c.updateTask(plan.PlanID, setState(executing), setStartTime(tsTimeout))
				return nil, err
			}
			c.updateTask(plan.PlanID, setStartTime(ts))

			err = c.sessions.Compaction(ctx, innerTask.dataNodeID, plan)

			c.updateTask(plan.PlanID, setState(executing))
			if err != nil {
				log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
				return nil, err
			}
			log.Info("Compaction start")
			return nil, nil
		})
	}
}

// execCompactionPlan start to execute plan and return immediately
func (c *compactionPlanHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) {
	c.enqueuePlan(signal, plan)
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		c.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

// complete a compaction task
// not threadsafe, only can be used internally
func (c *compactionPlanHandler) completeCompaction(result *datapb.CompactionPlanResult) error {
	planID := result.PlanID
	if _, ok := c.plans[planID]; !ok {
		return fmt.Errorf("plan %d is not found", planID)
	}

	if c.plans[planID].state != executing {
		return fmt.Errorf("plan %d's state is %v", planID, c.plans[planID].state)
	}

	plan := c.plans[planID].plan
	nodeID := c.plans[planID].dataNodeID
	defer c.scheduler.Finish(nodeID, plan)
	switch plan.GetType() {
	case datapb.CompactionType_MergeCompaction, datapb.CompactionType_MixCompaction:
		if err := c.handleMergeCompactionResult(plan, result); err != nil {
			return err
		}
	case datapb.CompactionType_Level0DeleteCompaction:
		if err := c.handleL0CompactionResult(plan, result); err != nil {
			return err
		}
	default:
		return errors.New("unknown compaction type")
	}
	UpdateCompactionSegmentSizeMetrics(result.GetSegments())
	c.plans[planID] = c.plans[planID].shadowClone(setState(completed), setResult(result), cleanLogPath(), endSpan())
	return nil
}

func (c *compactionPlanHandler) handleL0CompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, AddBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs()))
	}

	levelZeroSegments := lo.Filter(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) bool {
		return b.GetLevel() == datapb.SegmentLevel_L0
	})

	for _, seg := range levelZeroSegments {
		operators = append(operators, UpdateStatusOperator(seg.GetSegmentID(), commonpb.SegmentState_Dropped), UpdateCompactedOperator(seg.GetSegmentID()))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", plan.GetPlanID()),
	)
	return c.meta.UpdateSegmentsInfo(operators...)
}

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) error {
	log := log.With(zap.Int64("planID", plan.GetPlanID()))
	if len(result.GetSegments()) == 0 || len(result.GetSegments()) > 1 {
		// should never happen
		log.Warn("illegal compaction results")
		return fmt.Errorf("Illegal compaction results: %v", result)
	}

	// Merge compaction has one and only one segment
	newSegmentInfo := c.meta.GetHealthySegment(result.GetSegments()[0].SegmentID)
	if newSegmentInfo != nil {
		log.Info("meta has already been changed, skip meta change and retry sync segments")
	} else {
		// Also prepare metric updates.
		_, metricMutation, err := c.meta.CompleteCompactionMutation(plan, result)
		if err != nil {
			return err
		}
		// Apply metrics after successful meta update.
		metricMutation.commit()
	}

	nodeID := c.plans[plan.GetPlanID()].dataNodeID
	req := &datapb.SyncSegmentsRequest{
		PlanID: plan.PlanID,
	}

	log.Info("handleCompactionResult: syncing segments with node", zap.Int64("nodeID", nodeID))
	if err := c.sessions.SyncSegments(nodeID, req); err != nil {
		log.Warn("handleCompactionResult: fail to sync segments with node",
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	log.Info("handleCompactionResult: success to handle merge compaction result")
	return nil
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (c *compactionPlanHandler) getCompaction(planID int64) *compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.plans[planID]
}

func (c *compactionPlanHandler) updateCompaction(ts Timestamp) error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.
	executingTasks := c.getTasksByState(executing)
	timeoutTasks := c.getTasksByState(timeout)
	planStates, err := c.sessions.GetCompactionPlansResults()
	if err != nil {
		// if there is a data node alive but we failed to get info,
		log.Warn("failed to get compaction plans from all nodes", zap.Error(err))
		return err
	}
	cachedPlans := []int64{}

	// TODO reduce the lock range
	c.mu.Lock()
	for _, task := range executingTasks {
		log := log.With(
			zap.Int64("planID", task.plan.PlanID),
			zap.Int64("nodeID", task.dataNodeID),
			zap.String("channel", task.plan.GetChannel()))
		planID := task.plan.PlanID
		cachedPlans = append(cachedPlans, planID)

		if nodePlan, ok := planStates[planID]; ok {
			planResult := nodePlan.B
			switch planResult.GetState() {
			case commonpb.CompactionState_Completed:
				log.Info("start to complete compaction")

				// channels are balanced to other nodes, yet the old datanode still have the compaction results
				// task.dataNodeID == planState.A, but
				// task.dataNodeID not match with channel
				// Mark this compaction as failure and skip processing the meta
				if !c.chManager.Match(task.dataNodeID, task.plan.GetChannel()) {
					// Sync segments without CompactionFrom segmentsIDs to make sure DN clear the task
					// without changing the meta
					log.Warn("compaction failed for channel nodeID not match")
					if err := c.sessions.SyncSegments(task.dataNodeID, &datapb.SyncSegmentsRequest{PlanID: planID}); err != nil {
						log.Warn("compaction failed to sync segments with node", zap.Error(err))
						continue
					}
					c.plans[planID] = c.plans[planID].shadowClone(setState(failed), endSpan())
					c.setSegmentsCompacting(task.plan, false)
					c.scheduler.Finish(task.dataNodeID, task.plan)
				}

				if err := c.completeCompaction(planResult); err != nil {
					log.Warn("fail to complete compaction", zap.Error(err))
				}

			case commonpb.CompactionState_Executing:
				if c.isTimeout(ts, task.plan.GetStartTime(), task.plan.GetTimeoutInSeconds()) {
					log.Warn("compaction timeout",
						zap.Int32("timeout in seconds", task.plan.GetTimeoutInSeconds()),
						zap.Uint64("startTime", task.plan.GetStartTime()),
						zap.Uint64("now", ts),
					)
					c.plans[planID] = c.plans[planID].shadowClone(setState(timeout), endSpan())
				}
			}
		} else {
			// compaction task in DC but not found in DN means the compaction plan has failed
			log.Info("compaction failed")
			c.plans[planID] = c.plans[planID].shadowClone(setState(failed), endSpan())
			c.setSegmentsCompacting(task.plan, false)
			c.scheduler.Finish(task.dataNodeID, task.plan)
		}
	}

	// Timeout tasks will be timeout and failed in DataNode
	// need to wait for DataNode reporting failure and clean the status.
	for _, task := range timeoutTasks {
		log := log.With(
			zap.Int64("planID", task.plan.PlanID),
			zap.Int64("nodeID", task.dataNodeID),
			zap.String("channel", task.plan.GetChannel()),
		)

		planID := task.plan.PlanID
		cachedPlans = append(cachedPlans, planID)
		if nodePlan, ok := planStates[planID]; ok {
			if nodePlan.B.GetState() == commonpb.CompactionState_Executing {
				log.RatedInfo(1, "compaction timeout in DataCoord yet DataNode is still running")
			}
		} else {
			// compaction task in DC but not found in DN means the compaction plan has failed
			log.Info("compaction failed for timeout")
			c.plans[planID] = c.plans[planID].shadowClone(setState(failed), endSpan())
			c.setSegmentsCompacting(task.plan, false)
			c.scheduler.Finish(task.dataNodeID, task.plan)
		}
	}
	c.mu.Unlock()

	// Compaction plans in DN but not in DC are unknown plans, need to notify DN to clear it.
	// No locks needed, because no changes in DC memeory
	completedPlans := lo.PickBy(planStates, func(planID int64, planState *typeutil.Pair[int64, *datapb.CompactionPlanResult]) bool {
		return planState.B.GetState() == commonpb.CompactionState_Completed
	})

	unkonwnPlansInWorker, _ := lo.Difference(lo.Keys(completedPlans), cachedPlans)
	for _, planID := range unkonwnPlansInWorker {
		if nodeUnkonwnPlan, ok := completedPlans[planID]; ok {
			nodeID, plan := nodeUnkonwnPlan.A, nodeUnkonwnPlan.B
			log := log.With(zap.Int64("planID", planID), zap.Int64("nodeID", nodeID), zap.String("channel", plan.GetChannel()))

			// Sync segments without CompactionFrom segmentsIDs to make sure DN clear the task
			// without changing the meta
			log.Info("compaction syncing unknown plan with node")
			if err := c.sessions.SyncSegments(nodeID, &datapb.SyncSegmentsRequest{
				PlanID: planID,
			}); err != nil {
				log.Warn("compaction failed to sync segments with node", zap.Error(err))
				return err
			}
		}
	}

	return nil
}

func (c *compactionPlanHandler) isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	startTime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(startTime).Seconds()) >= timeout
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.scheduler.GetTaskCount() >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
}

func (c *compactionPlanHandler) getTasksByState(state compactionTaskState) []*compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tasks := make([]*compactionTask, 0, len(c.plans))
	for _, plan := range c.plans {
		if plan.state == state {
			tasks = append(tasks, plan)
		}
	}
	return tasks
}

// get compaction tasks by signal id; if signalID == 0 return all tasks
func (c *compactionPlanHandler) getCompactionTasksBySignalID(signalID int64) []*compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tasks []*compactionTask
	for _, t := range c.plans {
		if signalID == 0 {
			tasks = append(tasks, t)
			continue
		}
		if t.triggerInfo.id != signalID {
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks
}

type compactionTaskOpt func(task *compactionTask)

func setState(state compactionTaskState) compactionTaskOpt {
	return func(task *compactionTask) {
		task.state = state
	}
}

func endSpan() compactionTaskOpt {
	return func(task *compactionTask) {
		if task.span != nil {
			task.span.End()
		}
	}
}

func setStartTime(startTime uint64) compactionTaskOpt {
	return func(task *compactionTask) {
		task.plan.StartTime = startTime
	}
}

func setResult(result *datapb.CompactionPlanResult) compactionTaskOpt {
	return func(task *compactionTask) {
		task.result = result
	}
}

// cleanLogPath clean the log info in the compactionTask object for avoiding the memory leak
func cleanLogPath() compactionTaskOpt {
	return func(task *compactionTask) {
		if task.plan.GetSegmentBinlogs() != nil {
			for _, binlogs := range task.plan.GetSegmentBinlogs() {
				binlogs.FieldBinlogs = nil
				binlogs.Field2StatslogPaths = nil
				binlogs.Deltalogs = nil
			}
		}
		if task.result.GetSegments() != nil {
			for _, segment := range task.result.GetSegments() {
				segment.InsertLogs = nil
				segment.Deltalogs = nil
				segment.Field2StatslogPaths = nil
			}
		}
	}
}

// 0.5*min(8, NumCPU/2)
func calculateParallel() int {
	// TODO after node memory management enabled, use this config as hard limit
	return Params.DataCoordCfg.CompactionWorkerParalleTasks.GetAsInt()
	//cores := hardware.GetCPUNum()
	//if cores < 16 {
	//return 4
	//}
	//return cores / 2
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
