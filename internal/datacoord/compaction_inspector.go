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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// maxConcurrentCleanups bounds the cleanup fan-out. Cleanup is metadata work
// that may first wait out a worker callback, so it needs a ceiling, but it is
// also on the path that frees input segments, so the ceiling is not tight.
const maxConcurrentCleanups = 16

// maxCompactionTaskExecutionDuration is how long each type is expected to take.
// It drives the "this task is running slow" warning only -- an unreachable
// worker abandons the attempt at the first unanswered query -- so the values
// are per-type: what counts as slow depends on the work.
var maxCompactionTaskExecutionDuration = map[datapb.CompactionType]time.Duration{
	datapb.CompactionType_MixCompaction:               30 * time.Minute,
	datapb.CompactionType_Level0DeleteCompaction:      30 * time.Minute,
	datapb.CompactionType_ClusteringCompaction:        60 * time.Minute,
	datapb.CompactionType_SortCompaction:              20 * time.Minute,
	datapb.CompactionType_BumpSchemaVersionCompaction: 30 * time.Minute,
}

type CompactionInspector interface {
	// start launches every background loop unconditionally. The inspector never
	// consults dataCoord.enableCompaction -- that switch gates the producers of
	// new work, not execution; see start() on compactionInspector.
	start()
	stop()
	// enqueueCompaction start to enqueue compaction task and return immediately
	enqueueCompaction(task *datapb.CompactionTask) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	getCompactionTasksNumByTriggerID(triggerID int64) int
	getCompactionInfo(ctx context.Context, triggerID int64) *compactionInfo
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
	// ctx is the process-lifetime context handed to CreateServer. Stop() never
	// cancels it -- it cancels serverLoopCtx, a child -- so during a graceful
	// stop the in-flight debt writes (the replacement record, the cleaned
	// state) still complete under it; only the process being torn down aborts
	// them, and recovery redoes what was lost. Every outgoing call is
	// additionally bounded below this layer: etcdKV wraps each op in its
	// request timeout, worker RPCs in dataCoord.requestTimeoutSeconds, and the
	// allocator in its own bound.
	ctx context.Context

	queueTasks *CompactionQueue

	executingGuard lock.RWMutex
	executingTasks map[int64]CompactionTask // planID -> task

	cleaningGuard lock.RWMutex
	cleaningTasks map[int64]CompactionTask // planID -> task
	// cleaningInFlight, also under cleaningGuard, holds the planIDs whose
	// cleanup goroutine has not finished. Membership is the cleanup slot: it
	// keeps a slow cleanup from being re-dispatched every schedule round, and
	// its size bounds the fan-out at maxConcurrentCleanups.
	cleaningInFlight map[int64]struct{}

	meta      CompactionMeta
	allocator allocator.Allocator
	handler   Handler
	cluster   session.Cluster
	scheduler task.GlobalScheduler
	ievm      IndexEngineVersionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func (c *compactionInspector) getCompactionInfo(ctx context.Context, triggerID int64) *compactionInfo {
	tasks := c.meta.GetCompactionTasksByTriggerID(ctx, triggerID)
	return summaryCompactionState(ctx, triggerID, tasks)
}

func summaryCompactionState(ctx context.Context, triggerID int64, tasks []*datapb.CompactionTask) *compactionInfo {
	ret := &compactionInfo{}
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, analyzingCnt, indexingCnt, cleanedCnt, metaSavedCnt, stats, awaitingReplanCnt int
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
		case datapb.CompactionTaskState_retrying:
			// A task still owed a rebuild stands for work that is owed, not work
			// that is over: this trigger keeps its ID across the rebuild, so
			// reporting the failure now would tell the caller the compaction had
			// settled while cleanup is about to retry it.
			awaitingReplanCnt++
		default:
		}
		mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
	}

	ret.executingCnt = executingCnt + pipeliningCnt + analyzingCnt + indexingCnt + metaSavedCnt + stats + awaitingReplanCnt
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt
	ret.mergeInfos = mergeInfos

	if ret.executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}

	mlog.Info(ctx, "compaction states",
		mlog.Int64("triggerID", triggerID),
		mlog.String("state", ret.state.String()),
		mlog.Int("executingCnt", executingCnt),
		mlog.Int("pipeliningCnt", pipeliningCnt),
		mlog.Int("completedCnt", completedCnt),
		mlog.Int("failedCnt", failedCnt),
		mlog.Int("analyzingCnt", analyzingCnt),
		mlog.Int("indexingCnt", indexingCnt),
		mlog.Int("timeoutCnt", timeoutCnt),
		mlog.Int("cleanedCnt", cleanedCnt),
		mlog.Int("metaSavedCnt", metaSavedCnt),
		mlog.Int("awaitingReplanCnt", awaitingReplanCnt))
	return ret
}

func (c *compactionInspector) getCompactionTasksNumByTriggerID(triggerID int64) int {
	cnt := 0
	c.queueTasks.ForEach(func(ct CompactionTask) {
		if ct.GetTask().GetTriggerID() == triggerID {
			cnt += 1
		}
	})
	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
		if t.GetTask().GetTriggerID() == triggerID {
			cnt += 1
		}
	}
	c.executingGuard.RUnlock()
	return cnt
}

func newCompactionInspector(ctx context.Context, meta CompactionMeta,
	allocator allocator.Allocator, handler Handler, cluster session.Cluster, scheduler task.GlobalScheduler, ievm IndexEngineVersionManager,
) *compactionInspector {
	capacity := paramtable.Get().DataCoordCfg.CompactionTaskQueueCapacity.GetAsInt()
	return &compactionInspector{
		ctx:              ctx,
		queueTasks:       NewCompactionQueue(capacity, getPrioritizer()),
		meta:             meta,
		allocator:        allocator,
		stopCh:           make(chan struct{}),
		executingTasks:   make(map[int64]CompactionTask),
		cleaningTasks:    make(map[int64]CompactionTask),
		cleaningInFlight: make(map[int64]struct{}),
		handler:          handler,
		cluster:          cluster,
		scheduler:        scheduler,
		ievm:             ievm,
	}
}

func (c *compactionInspector) checkSchedule() {
	c.checkCompaction()
	c.cleanFailedTasks()
	c.resumePendingTasks()
	c.schedule()
}

func (c *compactionInspector) schedule() []CompactionTask {
	selected := make([]CompactionTask, 0)

	// Sync before the empty-queue early return, so a configuration change made
	// while the queue happens to be empty is still adopted. The cost on an
	// empty queue is one lock acquisition and one string compare -- the
	// re-prioritize loop iterates zero times.
	c.queueTasks.SyncPrioritizer(getPrioritizerName())

	if c.queueTasks.Len() == 0 {
		return selected
	}

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	clusterLabelExcludes := typeutil.NewSet[string]()

	exclude := func(t CompactionTask) {
		switch t.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			l0ChannelExcludes.Insert(t.GetTask().GetChannel())
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			mixChannelExcludes.Insert(t.GetTask().GetChannel())
			mixLabelExcludes.Insert(compactionTaskLabel(t.GetTask()))
		case datapb.CompactionType_ClusteringCompaction:
			clusterChannelExcludes.Insert(t.GetTask().GetChannel())
			clusterLabelExcludes.Insert(compactionTaskLabel(t.GetTask()))
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
		// Add back the excluded tasks. Enqueue always accepts, so a task that
		// was popped for an exclusion decision can always go home.
		for _, t := range excluded {
			c.queueTasks.Enqueue(t)
		}
	}()

	// The schedule loop will stop if either:
	// 1. no more task to schedule (the task queue is empty)
	// 2. no available slots
	for {
		t, err := c.queueTasks.Dequeue()
		if err != nil {
			break // 1. no more task to schedule
		}

		switch t.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if mixChannelExcludes.Contain(t.GetTask().GetChannel()) ||
				clusterChannelExcludes.Contain(t.GetTask().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			l0ChannelExcludes.Insert(t.GetTask().GetChannel())
			selected = append(selected, t)
		case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
			// BumpSchemaVersionCompaction shares the same exclusion rules as Mix/Sort:
			// - Channel-level mutual exclusion with L0 (L0 may write delta logs to any segment on the channel)
			// - Label-level exclusion registered for Clustering awareness
			if l0ChannelExcludes.Contain(t.GetTask().GetChannel()) {
				excluded = append(excluded, t)
				continue
			}
			mixChannelExcludes.Insert(t.GetTask().GetChannel())
			mixLabelExcludes.Insert(compactionTaskLabel(t.GetTask()))
			selected = append(selected, t)
		case datapb.CompactionType_ClusteringCompaction:
			if l0ChannelExcludes.Contain(t.GetTask().GetChannel()) ||
				mixLabelExcludes.Contain(compactionTaskLabel(t.GetTask())) ||
				clusterLabelExcludes.Contain(compactionTaskLabel(t.GetTask())) {
				excluded = append(excluded, t)
				continue
			}
			clusterChannelExcludes.Insert(t.GetTask().GetChannel())
			clusterLabelExcludes.Insert(compactionTaskLabel(t.GetTask()))
			selected = append(selected, t)
		}

		// Read the node this task is counted under before Enqueue: dispatch is
		// asynchronous, so the scheduler can assign one the moment the task is
		// handed over, and the increment below has to name the same bucket the
		// pending decrement and the eventual executing decrement do.
		metricNode := t.GetTask().GetNodeID()
		c.executingGuard.Lock()
		c.executingTasks[t.GetTask().GetPlanID()] = t
		c.scheduler.Enqueue(t)
		mlog.Info(c.ctx, "compaction task enqueued",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.FieldVChannel(t.GetTask().GetChannel()),
			mlog.String("label", compactionTaskLabel(t.GetTask())),
			mlog.Int64s("inputSegments", t.GetTask().GetInputSegments()),
		)
		c.executingGuard.Unlock()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Executing).Inc()
	}
	return selected
}

// start launches both background loops unconditionally: the inspector is a
// pure executor and never consults dataCoord.enableCompaction. That switch
// lives at every producer of brand-new work -- the triggers are not started
// and ManualCompaction is rejected while it is off -- so whatever reaches
// queueTasks is either inherited debt (recovered tasks, replans of them) or
// work correctness requires regardless of the switch (an import's sort
// compaction, without which the imported segments never leave IsInvisible).
// Freezing any of those would hold their input segments' compacting locks
// until restart: canTriggerSortCompaction requires !isCompacting, so an input
// whose task never finishes can never be re-sorted, and a sort input that is
// never re-sorted never leaves the growing query path.
func (c *compactionInspector) start() {
	c.stopWg.Add(2)
	go c.loopSchedule()
	go c.loopClean()
}

// loadMeta rebuilds in-memory state from the persisted compaction tasks. It
// fails startup on writes that transition recoverable work, but it does not
// block readiness deleting records whose task type cannot even be built. Those
// records own no process-local inputs or worker task, so a tracked background
// cleanup can safely erase them while preserving the retry-on-next-restart
// behavior when a deletion fails.
func (c *compactionInspector) loadMeta() error {
	// TODO: make it compatible to all types of compaction with persist meta
	triggers := c.meta.GetCompactionTasks(c.ctx)
	failedTasks := make([]*datapb.CompactionTask, 0)

	for _, tasks := range triggers {
		for _, task := range tasks {
			deferCleanup, err := c.recoverTask(task)
			if err != nil {
				return err
			}
			if deferCleanup {
				failedTasks = append(failedTasks, task)
			}
		}
	}
	if len(failedTasks) > 0 {
		c.stopWg.Add(1)
		go c.cleanupFailedCompactionTasks(failedTasks)
	}
	return nil
}

// recoverTask decides what one persisted record becomes on startup: forgotten,
// queued for cleanup, left for the business retry loop, erased, or handed back
// to the queue or the scheduler. The bool asks loadMeta to erase an unbuildable
// record outside the readiness path.
func (c *compactionInspector) recoverTask(task *datapb.CompactionTask) (bool, error) {
	log := mlog.With(
		mlog.Int64("planID", task.GetPlanID()),
		mlog.String("type", task.GetType().String()),
		mlog.String("state", task.GetState().String()))

	if isCompactionTaskCleaned(task) {
		log.Info(c.ctx, "compactionInspector loadMeta abandon compactionTask")
		return false, nil
	}

	t, err := c.buildCompactTask(task)
	if err != nil {
		log.Info(c.ctx, "compactionInspector loadMeta build compactionTask failed, defer cleanup", mlog.Err(err))
		return true, nil
	}

	// Terminal tasks are queued for cleanup before admission is even considered.
	// Admission rejects a task whose inputs a snapshot protects -- which is
	// exactly the state that terminated a sort task in the first place -- and
	// dropping the task there would leave its inputs locked, so nothing would
	// ever re-sort them.
	if needsCleanup(task.GetState()) {
		// Hand the task to the same asynchronous cleanup the runtime path uses,
		// rather than cleaning it here: loadMeta runs before DataCoord reports
		// ready, and the backlog is unbounded -- terminal tasks are only ever
		// GC'd after they reach cleaned, so a cluster that ran with compaction
		// disabled accumulates them indefinitely. Blocking readiness on that
		// backlog would hold the whole coordinator behind metastore writes that
		// the retry loop can just as well make afterwards. Cleanup also gets the
		// channel and label exclusion it would not have had here.
		// isCompacting is intentionally process-local, so recovery has to
		// reconstruct the claim before producers are started. Merely putting the
		// task in cleaningTasks only excludes dispatch: a trigger can still plan
		// and admit the same inputs, after which this old task's
		// resetSegmentCompacting would release the new owner's claim. Terminal
		// cleanup bypasses normal admission (a snapshot may be why it
		// terminated), so reserve the surviving inputs directly.
		c.meta.SetSegmentsCompacting(c.ctx, task.GetInputSegments(), true)
		c.cleaningGuard.Lock()
		c.cleaningTasks[task.GetPlanID()] = t
		c.cleaningGuard.Unlock()
		log.Info(c.ctx, "compactionInspector loadMeta queued recovered terminal task for cleanup")
		return false, nil
	}

	// These states have already finished their worker-side segment mutation.
	// Their inputs may therefore be Dropped, so normal admission is no longer
	// applicable; restore the process-local claim and let Process continue the
	// persisted state machine.
	if task.GetState() == datapb.CompactionTaskState_meta_saved ||
		task.GetState() == datapb.CompactionTaskState_statistic ||
		task.GetState() == datapb.CompactionTaskState_indexing {
		c.meta.SetSegmentsCompacting(c.ctx, task.GetInputSegments(), true)
		c.restoreTask(t)
		log.Info(c.ctx, "compactionInspector restored post-worker task")
		return false, nil
	}

	if err := c.admitCompactTask(task); err != nil {
		if task.GetState() == datapb.CompactionTaskState_pipelining {
			// A persisted pipelining record is durable business-layer retry
			// debt. Snapshot protection and an in-flight input claim are
			// transient, so leave the record for resumePendingTasks instead of
			// dropping it during startup.
			log.Info(c.ctx, "compactionInspector loadMeta deferred a blocked task to the retry loop", mlog.Err(err))
			return false, nil
		}
		// Executing records cannot be deferred: the retry loop drives only
		// pipelining records. End the attempt exactly as an unanswered worker
		// round would; cleanup rebuilds the work under a fresh plan ID.
		if saveErr := t.(replannableTask).updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
			return false, saveErr
		}
		c.meta.SetSegmentsCompacting(c.ctx, task.GetInputSegments(), true)
		c.cleaningGuard.Lock()
		c.cleaningTasks[task.GetPlanID()] = t
		c.cleaningGuard.Unlock()
		return false, nil
	}

	if task.GetState() != datapb.CompactionTaskState_pipelining || task.GetNodeID() > 0 {
		c.restoreTask(t)
		log.Info(c.ctx, "compactionInspector loadMeta restoreTask",
			mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
			mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
		return false, nil
	}

	// Recovery always re-enqueues durable work; process-local admission pressure
	// cannot invalidate a persisted task.
	c.submitTask(t)
	log.Info(c.ctx, "compactionInspector loadMeta submitTask",
		mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	return false, nil
}

func (c *compactionInspector) cleanupFailedCompactionTasks(tasks []*datapb.CompactionTask) {
	defer c.stopWg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	total := len(tasks)
	mlog.Info(ctx, "start cleaning up failed compaction tasks", mlog.Int("total", total))
	cleaned := 0
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "failed compaction task cleanup aborted",
				mlog.Int("cleaned", cleaned),
				mlog.Int("remaining", total-cleaned))
			return
		default:
		}
		if err := c.meta.DropCompactionTask(ctx, task); err != nil {
			mlog.Warn(ctx, "drop failed compaction task failed",
				mlog.Int64("planID", task.GetPlanID()), mlog.Err(err))
			continue
		}
		cleaned++
	}
	mlog.Info(ctx, "failed compaction task cleanup finished",
		mlog.Int("cleaned", cleaned),
		mlog.Int("total", total))
}

// releaseWorkerResources best-effort drops the old compaction plan and analyze
// child before replacement. Both operations are idempotent; durable terminal
// cleanup and the DataNode sweeper remain the fallback. The goroutine is tracked
// by stopWg so worker RPC latency does not block the inspector loop.
func (c *compactionInspector) releaseWorkerResources(task *datapb.CompactionTask) {
	analyzeTaskID := task.GetAnalyzeTaskID()
	dropPlan := c.cluster != nil && task.GetNodeID() > 0
	abortAnalyze := c.scheduler != nil && analyzeTaskID > 0
	if !dropPlan && !abortAnalyze {
		return
	}
	c.stopWg.Add(1)
	go func() {
		defer c.stopWg.Done()
		if dropPlan {
			if err := c.cluster.DropCompaction(task.GetNodeID(), task.GetPlanID()); err != nil {
				mlog.Warn(c.ctx, "failed to drop compaction plan on worker",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.FieldNodeID(task.GetNodeID()),
					mlog.Err(err))
			}
		}
		if abortAnalyze {
			// Idempotent: aborting an unknown or already-finished analyze task
			// is a no-op, which is the common case -- a clustering compaction
			// cannot complete before its analysis does.
			c.scheduler.AbortAndRemoveTask(analyzeTaskID)
		}
	}()
}

// buildReplacement preserves the client-visible trigger and input segments but
// allocates fresh plan, output, and analyze identities. An unfinished old worker
// therefore cannot overwrite replacement artifacts. The caller persists the
// swap; a build failure leaves the retry record intact for the next round.
func (c *compactionInspector) buildReplacement(t CompactionTask) *datapb.CompactionTask {
	old := t.GetTask()
	log := mlog.With(
		mlog.Int64("planID", old.GetPlanID()),
		mlog.Int64("triggerID", old.GetTriggerID()),
		mlog.String("type", old.GetType().String()))

	// Whether a rebuild is owed at all was decided by the caller from the
	// persisted retrying state; a nil return here means only that it could not be
	// built right now, which is why the caller retries the round rather than
	// cleaning without one.

	planID, err := c.allocator.AllocID(c.ctx)
	if err != nil {
		log.Warn(c.ctx, "failed to allocate planID for compaction replan", mlog.Err(err))
		return nil
	}

	// Reserve the same number of output segment IDs the trigger sized this task
	// for, without reproducing any of the per-type sizing rules: the count is the
	// only thing that matters, and the old range already carries it.
	newProto := proto.Clone(old).(*datapb.CompactionTask)
	if r := old.GetPreAllocatedSegmentIDs(); r != nil && r.GetEnd() > r.GetBegin() {
		begin, end, err := c.allocator.AllocN(r.GetEnd() - r.GetBegin())
		if err != nil {
			log.Warn(c.ctx, "failed to allocate output segment IDs for compaction replan", mlog.Err(err))
			return nil
		}
		newProto.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: begin, End: end}
	}

	newProto.PlanID = planID
	newProto.State = datapb.CompactionTaskState_pipelining
	newProto.NodeID = NullNodeID
	newProto.RetryTimes = old.GetRetryTimes() + 1
	newProto.FailReason = ""
	newProto.EndTime = 0
	// Results belong to the attempt that produced them. Carrying them over would
	// let the new attempt's completion path adopt segments the old worker wrote
	// under the old IDs.
	newProto.ResultSegments = nil
	newProto.TmpSegments = nil
	// A clustering retry gets its own analyze task and re-runs the analysis.
	//
	// Carrying the old analyze task over would save a k-means pass, but it makes
	// the old record's cleanup conditional on this function succeeding: cleanup
	// has to be told to leave the analyze record alone BEFORE the replacement is
	// known to exist, and this function has four ways to return without creating
	// one -- including losing the race for the input segments, which is a normal
	// outcome. Every one of those would strand the analyze record and its files
	// with nothing left to reference them. Re-running the analysis costs a rare
	// clustering retry some time; getting the ownership handoff wrong leaks
	// forever, and only on the paths hardest to test.
	if old.GetAnalyzeTaskID() > 0 {
		analyzeTaskID, err := c.allocator.AllocID(c.ctx)
		if err != nil {
			log.Warn(c.ctx, "failed to allocate analyze task ID for compaction replan", mlog.Err(err))
			return nil
		}
		newProto.AnalyzeTaskID = analyzeTaskID
		// Clear the result of the previous analysis so the state machine runs it
		// again: CreateTaskOnWorker only calls doAnalyze while AnalyzeVersion is 0.
		newProto.AnalyzeVersion = 0
	}

	taskCreateTS, err := c.allocator.AllocTimestamp(c.ctx)
	if err != nil {
		log.Warn(c.ctx, "failed to allocate create timestamp for compaction replan", mlog.Err(err))
		return nil
	}
	newProto.CreateTs = taskCreateTS
	newProto.StartTime = tsoutil.PhysicalTime(taskCreateTS).Unix()

	// Name both ends of the handover. A replan changes the plan ID, so without
	// the pair a reader following one piece of work across ten failed attempts
	// has no way to link the attempts to each other -- the log says a new plan
	// appeared, never which one it replaced or why.
	log.Info(c.ctx, "built a compaction replan under a fresh plan ID",
		mlog.Int64("oldPlanID", old.GetPlanID()),
		mlog.Int64("newPlanID", planID),
		mlog.Int("attempt", int(newProto.GetRetryTimes())),
		mlog.String("reason", old.GetFailReason()))

	return newProto
}

// resumePendingTasks is the compaction business-layer retry for a persisted
// pipelining task that has no in-memory owner. The normal retry path swaps the
// old and new metadata and immediately queues the new task; this scan only
// matters after a restart, or while snapshot protection temporarily blocks
// startup recovery.
func (c *compactionInspector) resumePendingTasks() {
	known := typeutil.NewUniqueSet()
	c.queueTasks.ForEach(func(t CompactionTask) {
		known.Insert(t.GetTask().GetPlanID())
	})
	c.executingGuard.RLock()
	for planID := range c.executingTasks {
		known.Insert(planID)
	}
	c.executingGuard.RUnlock()
	c.cleaningGuard.RLock()
	for planID := range c.cleaningTasks {
		known.Insert(planID)
	}
	c.cleaningGuard.RUnlock()

	var pending []*datapb.CompactionTask
	c.meta.GetCompactionTaskMeta().Range(func(task *datapb.CompactionTask) bool {
		if task.GetState() == datapb.CompactionTaskState_pipelining &&
			!known.Contain(task.GetPlanID()) {
			pending = append(pending, proto.Clone(task).(*datapb.CompactionTask))
		}
		return true
	})

	for _, taskProto := range pending {
		log := mlog.With(
			mlog.Int64("planID", taskProto.GetPlanID()),
			mlog.Int64("triggerID", taskProto.GetTriggerID()),
			mlog.String("type", taskProto.GetType().String()))
		t, err := c.createCompactTask(taskProto)
		if err != nil {
			if errors.Is(err, merr.ErrCompactionPlanConflict) || errors.Is(err, merr.ErrCompactionBlocked) {
				log.RatedInfo(c.ctx, rate.Limit(1.0/60), "pending compaction task is temporarily blocked", mlog.Err(err))
				continue
			}
			log.Info(c.ctx, "pending compaction task is no longer runnable, erasing it", mlog.Err(err))
			if dropErr := c.meta.DropCompactionTask(c.ctx, taskProto); dropErr != nil {
				log.Warn(c.ctx, "failed to erase pending compaction task", mlog.Err(dropErr))
			}
			continue
		}
		c.submitTask(t)
		log.Info(c.ctx, "resumed pending compaction task")
	}
}

func (c *compactionInspector) loopSchedule() {
	interval := paramtable.Get().DataCoordCfg.CompactionScheduleInterval.GetAsDuration(time.Millisecond)
	mlog.Info(c.ctx, "compactionInspector start loop schedule", mlog.Duration("schedule interval", interval))
	defer c.stopWg.Done()

	scheduleTicker := time.NewTicker(interval)
	defer scheduleTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(c.ctx, "compactionInspector quit loop schedule")
			return
		// The process-lifetime context: never canceled by a graceful stop
		// (that path closes stopCh), only by the process being torn down.
		case <-c.ctx.Done():
			mlog.Info(c.ctx, "compactionInspector quit loop schedule, context done")
			return
		case <-scheduleTicker.C:
			c.checkSchedule()
		}
	}
}

func (c *compactionInspector) loopClean() {
	interval := Params.DataCoordCfg.CompactionGCIntervalInSeconds.GetAsDuration(time.Second)
	mlog.Info(c.ctx, "compactionInspector start clean check loop", mlog.Duration("gcInterval", interval))
	defer c.stopWg.Done()
	cleanTicker := time.NewTicker(interval)
	defer cleanTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			mlog.Info(c.ctx, "Compaction inspector quit loopClean")
			return
		case <-c.ctx.Done():
			mlog.Info(c.ctx, "Compaction inspector quit loopClean, context done")
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
	triggers := c.meta.GetCompactionTasks(c.ctx)
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State == datapb.CompactionTaskState_cleaned {
				duration := time.Since(time.Unix(task.StartTime, 0)).Seconds()
				if duration > Params.DataCoordCfg.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds() {
					if !c.dropCompactionBeforeMetaGC(task) {
						continue
					}
					err := c.meta.DropCompactionTask(c.ctx, task)
					mlog.Debug(c.ctx, "drop compaction task meta", mlog.Int64("planID", task.PlanID))
					if err != nil {
						mlog.Warn(c.ctx, "fail to drop task", mlog.Int64("planID", task.PlanID), mlog.Err(err))
					}
				}
			}
		}
	}
}

func (c *compactionInspector) dropCompactionBeforeMetaGC(task *datapb.CompactionTask) bool {
	if task.GetNodeID() <= 0 {
		return true
	}
	if c.cluster == nil {
		mlog.Warn(c.ctx, "cannot drop assigned compaction plan before metadata GC",
			mlog.Int64("planID", task.GetPlanID()),
			mlog.FieldNodeID(task.GetNodeID()))
		return false
	}
	if err := c.cluster.DropCompaction(task.GetNodeID(), task.GetPlanID()); err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		mlog.Warn(c.ctx, "failed to drop compaction plan before metadata GC",
			mlog.Int64("planID", task.GetPlanID()),
			mlog.FieldNodeID(task.GetNodeID()),
			mlog.Err(err))
		return false
	}
	return true
}

func (c *compactionInspector) cleanPartitionStats() {
	mlog.Debug(c.ctx, "start gc partitionStats meta and files")
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo)
	unusedPartStats := make([]*datapb.PartitionStatsInfo, 0)
	if c.meta.GetPartitionStatsMeta() == nil {
		return
	}
	infos := c.meta.GetPartitionStatsMeta().ListAllPartitionStatsInfos()
	for _, info := range infos {
		collInfo, err := c.handler.GetCollection(c.ctx, info.GetCollectionID())
		if err != nil {
			if errors.Is(err, merr.ErrCollectionNotFound) {
				unusedPartStats = append(unusedPartStats, info)
				continue
			}
			mlog.Warn(c.ctx, "skip partition stats GC because collection lookup failed",
				mlog.FieldCollectionID(info.GetCollectionID()),
				mlog.Err(err))
			continue
		}
		if collInfo == nil {
			mlog.Warn(c.ctx, "skip partition stats GC because collection lookup returned no result",
				mlog.FieldCollectionID(info.GetCollectionID()))
			continue
		}
		channel := fmt.Sprintf("%d/%d/%s", info.CollectionID, info.PartitionID, info.VChannel)
		if _, ok := channelPartitionStatsInfos[channel]; !ok {
			channelPartitionStatsInfos[channel] = make([]*datapb.PartitionStatsInfo, 0)
		}
		channelPartitionStatsInfos[channel] = append(channelPartitionStatsInfos[channel], info)
	}
	mlog.Debug(c.ctx, "channels with PartitionStats meta", mlog.Int("len", len(channelPartitionStatsInfos)))

	for _, info := range unusedPartStats {
		mlog.Debug(c.ctx, "collection has been dropped, remove partition stats",
			mlog.Int64("collID", info.GetCollectionID()))
		if !c.finalizeAnalyzeTaskCleanup(info.GetAnalyzeTaskID(), func() bool {
			if err := c.meta.CleanPartitionStatsInfo(c.ctx, info); err != nil {
				mlog.Warn(c.ctx, "gcPartitionStatsInfo fail", mlog.Err(err))
				return false
			}
			return true
		}) {
			continue
		}
	}

	for channel, infos := range channelPartitionStatsInfos {
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Version > infos[j].Version
		})
		mlog.Debug(c.ctx, "PartitionStats in channel", mlog.String("partitionStatsKey", channel), mlog.Int("len", len(infos)))
		if len(infos) > 2 {
			for i := 2; i < len(infos); i++ {
				info := infos[i]
				if !c.finalizeAnalyzeTaskCleanup(info.GetAnalyzeTaskID(), func() bool {
					if err := c.meta.CleanPartitionStatsInfo(c.ctx, info); err != nil {
						mlog.Warn(c.ctx, "gcPartitionStatsInfo fail", mlog.Err(err))
						return false
					}
					return true
				}) {
					continue
				}
			}
		}
	}
}

func (c *compactionInspector) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	// Pending-task recovery runs only on the schedule loop and cleanup runs only
	// on tracked goroutines, so this waits for every in-flight round to settle.
	c.stopWg.Wait()
}

func (c *compactionInspector) removeTasksByChannel(channel string) {
	mlog.Info(c.ctx, "ending compaction tasks for dropped channel", mlog.FieldVChannel(channel))

	// Do not erase runtime ownership here. A task owns its input segments until
	// the existing failed -> cleaning -> cleaned path releases them, and an
	// executing task may still be owned by the global scheduler. Removing only
	// the inspector entries would strand both the durable task and the input
	// claims while the scheduler continued to poll its worker.
	tasks := make(map[int64]CompactionTask)
	c.queueTasks.ForEach(func(t CompactionTask) {
		if t.GetTask().GetChannel() == channel {
			tasks[t.GetTask().GetPlanID()] = t
		}
	})
	c.executingGuard.RLock()
	for planID, t := range c.executingTasks {
		if t.GetTask().GetChannel() == channel {
			tasks[planID] = t
		}
	}
	c.executingGuard.RUnlock()

	for planID, t := range tasks {
		var saveErr error
		c.scheduler.Update(planID, func() {
			// Terminal tasks are already converging through the same cleanup path.
			// In particular, do not rewrite completed into failed.
			if isTerminalState(t.GetTask().GetState()) {
				return
			}
			saveErr = t.(replannableTask).updateAndSaveTaskMeta(
				setState(datapb.CompactionTaskState_failed),
				setFailReason("channel dropped"),
			)
		})
		if saveErr != nil {
			// Keep the task in its current owner. Its normal state machine will
			// retry the metadata write or reject a result against the now-dropped
			// input segments; forgetting it here would leave no cleanup path.
			mlog.Warn(c.ctx, "failed to end compaction task for dropped channel",
				mlog.FieldVChannel(channel),
				mlog.Int64("planID", planID),
				mlog.Err(saveErr))
			continue
		}
		mlog.Info(c.ctx, "compaction task ended for dropped channel",
			mlog.FieldVChannel(channel),
			mlog.Int64("planID", planID),
			mlog.FieldNodeID(t.GetTask().GetNodeID()))
	}
}

// submitTask publishes a task the caller has already persisted. It cannot fail:
// the queue accepts unconditionally, because the alternative is a durable record
// that nothing in memory drives.
func (c *compactionInspector) submitTask(t CompactionTask) {
	c.queueTasks.Enqueue(t)
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Pending).Inc()
}

// restoreTask resumes a persisted task that already owns a worker.
func (c *compactionInspector) restoreTask(t CompactionTask) {
	metricNode := t.GetTask().GetNodeID()
	c.executingGuard.Lock()
	c.executingTasks[t.GetTask().GetPlanID()] = t
	c.scheduler.Enqueue(t)
	c.executingGuard.Unlock()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(metricNode), t.GetTask().GetType().String(), metrics.Executing).Inc()
}

// getCompactionTask return compaction
func (c *compactionInspector) getCompactionTask(planID int64) CompactionTask {
	var t CompactionTask = nil
	c.queueTasks.ForEach(func(task CompactionTask) {
		if task.GetTask().GetPlanID() == planID {
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
			log.RatedInfo(c.ctx, rate.Limit(1.0/60), "Failed to create compaction task, compaction plan conflict", mlog.Err(err))
		} else {
			log.Warn(c.ctx, "Failed to create compaction task, unable to create compaction task", mlog.Err(err))
		}
		return err
	}

	// Check capacity before persisting, and treat the answer as advice. The
	// trigger manager and import checker enqueue concurrently, so a few
	// producers can pass this check together and push the queue slightly past
	// the limit -- which is fine. The limit exists to stop unbounded growth of
	// persisted compaction tasks, each of which holds its inputs compacting;
	// being exact about it would mean making the queue refuse a task that is
	// already durable, and a refused durable task has no runtime owner until
	// the next restart. Overshoot by a handful beats that trade every time.
	if c.queueTasks.IsFull() {
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.RatedInfo(c.ctx, rate.Limit(1.0/60), "compaction task queue is full, not enqueuing")
		return merr.WrapErrServiceQuotaExceeded("compaction task queue is full")
	}

	taskCreateTS, err := c.allocator.AllocTimestamp(c.ctx)
	if err != nil {
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.Warn(c.ctx, "Failed to enqueue compaction task, unable to allocate task create timestamp", mlog.Err(err))
		return err
	}
	startTime := tsoutil.PhysicalTime(taskCreateTS).Unix()
	t.SetTask(cloneCompactionTask(t.GetTask(), setStartTime(startTime), setCreateTs(taskCreateTS)))
	err = c.meta.SaveCompactionTask(c.ctx, t.GetTask())
	if err != nil {
		// A failed catalog response is ambiguous: the pipelining record may
		// already be durable even though it was not installed in process-local
		// metadata. Releasing the input claim in a live process would let another
		// plan for the same segments be persisted beside it. Restart and reload the
		// authoritative catalog before admitting any more work. Fatal does not
		// return in production; a test hook may replace it, so return immediately
		// without releasing the claim on this path.
		if c.ctx != nil && c.ctx.Err() == nil {
			mlog.Fatal(c.ctx, "initial compaction task publication failed; terminating process",
				mlog.Int64("planID", t.GetTask().GetPlanID()),
				mlog.Int64s("inputSegments", t.GetTask().GetInputSegments()),
				mlog.Err(err))
			return err
		}
		c.meta.SetSegmentsCompacting(c.ctx, t.GetTask().GetInputSegments(), false)
		log.Warn(c.ctx, "Failed to enqueue compaction task, unable to save task meta", mlog.Err(err))
		return err
	}
	c.queueTasks.Enqueue(t)
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Pending).Inc()
	log.Info(c.ctx, "Compaction plan submitted")
	return nil
}

// buildCompactTask wraps a persisted task in its behavior, and nothing more. It
// is deliberately separate from admission: a terminal task recovered at startup
// still owes its inputs their segment lock, and must be reachable even when it
// could no longer be admitted.
func (c *compactionInspector) buildCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction:
		return newMixCompactionTask(c.ctx, t, c.allocator, c.meta, c.ievm), nil
	case datapb.CompactionType_Level0DeleteCompaction:
		return newL0CompactionTask(c.ctx, t, c.allocator, c.meta), nil
	case datapb.CompactionType_ClusteringCompaction:
		return newClusteringCompactionTask(c.ctx, t, c.allocator, c.meta, c.handler, c.scheduler, c.ievm), nil
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		return newBumpSchemaVersionTask(c.ctx, t, c.allocator, c.meta, c.ievm), nil
	default:
		return nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
}

// admitCompactTask decides whether a task may execute and claims its inputs.
// Only tasks that are going to run go through it.
func (c *compactionInspector) admitCompactTask(t *datapb.CompactionTask) error {
	// Revalidate input and snapshot state at admission so a protection change
	// after planning cannot enter the task queue unchecked.
	if err := c.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t); err != nil {
		return err
	}
	exist, succeed := c.meta.CheckAndSetSegmentsCompacting(c.ctx, t.GetInputSegments())
	if !exist {
		return merr.WrapErrIllegalCompactionPlan("segment not exist")
	}
	if !succeed {
		return merr.WrapErrCompactionPlanConflict("segment is compacting")
	}
	return nil
}

func (c *compactionInspector) createCompactTask(t *datapb.CompactionTask) (CompactionTask, error) {
	task, err := c.buildCompactTask(t)
	if err != nil {
		return nil, err
	}
	if err := c.admitCompactTask(t); err != nil {
		return nil, err
	}
	return task, nil
}

// checkCompaction retrieves executing tasks and calls each task's Process() method
// to evaluate its state and progress through the state machine.
// Completed tasks are removed from executingTasks.
// Tasks that fail or timeout are moved from executingTasks to cleaningTasks,
// where task-specific clean logic is performed asynchronously.
func (c *compactionInspector) checkCompaction() {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	type finishedCompactionTask struct {
		task         CompactionTask
		needsCleanup bool
	}
	var finishedTasks []finishedCompactionTask
	// Snapshot under the read lock, process after releasing it. Each
	// scheduler.Update below waits for that task's in-flight worker callback,
	// bounded by dataCoord.requestTimeoutSeconds -- holding executingGuard
	// across those waits would block every writer (removeTasksByChannel,
	// restoreTask) and, through Go's writer-preference, every later reader for
	// up to 30s per hung node. The map itself is only mutated under the write
	// lock, so the snapshot is a consistent point-in-time view; a task removed
	// concurrently is simply processed one extra time, which the terminal-state
	// guards make a no-op.
	c.executingGuard.RLock()
	executingSnapshot := make([]CompactionTask, 0, len(c.executingTasks))
	for _, t := range c.executingTasks {
		executingSnapshot = append(executingSnapshot, t)
	}
	c.executingGuard.RUnlock()

	for _, t := range executingSnapshot {
		c.checkDelay(t)
		// Decide cleanup from immutable snapshots taken around Process, never from
		// a later re-read. Between this loop and the cleaningTasks insert below the
		// lock is released, and a scheduler callback that fails to probe its
		// worker historically rewrote a terminal state back to pipelining. The
		// scheduler's lock-internal state check now stops that, while the snapshot
		// keeps this decision independent of any later write. A task removed
		// from executingTasks without entering cleaningTasks is never cleaned, so
		// its input segments stay isCompacting until DataCoord restarts.
		// The scheduler owns this task's state; borrow it under its per-task
		// lock so Process cannot interleave with a worker callback. This waits
		// for an in-flight callback, bounded by dataCoord.requestTimeoutSeconds.
		planID := t.GetTask().GetPlanID()
		var stateBeforeProcess, stateAfterProcess datapb.CompactionTaskState
		var finished bool
		c.scheduler.Update(planID, func() {
			stateBeforeProcess = t.GetTask().GetState()
			finished = t.Process()
			stateAfterProcess = t.GetTask().GetState()
		})
		needsCleanup := needsCleanup(stateBeforeProcess) ||
			needsCleanup(stateAfterProcess)
		if finished || needsCleanup {
			finishedTasks = append(finishedTasks, finishedCompactionTask{
				task:         t,
				needsCleanup: needsCleanup,
			})
		}
	}

	// delete all finished
	c.executingGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		delete(c.executingTasks, t.GetTask().GetPlanID())
		mlog.Info(c.ctx, "compaction task finished",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.String("state", t.GetTask().GetState().String()),
			mlog.FieldVChannel(t.GetTask().GetChannel()),
			mlog.String("label", compactionTaskLabel(t.GetTask())),
			mlog.FieldNodeID(t.GetTask().GetNodeID()),
			mlog.Int64s("inputSegments", t.GetTask().GetInputSegments()),
			mlog.String("reason", t.GetTask().GetFailReason()),
		)
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(t.GetTask().GetNodeID()), t.GetTask().GetType().String(), metrics.Done).Inc()
	}
	c.executingGuard.Unlock()

	// Queue tasks that need cleanup.
	c.cleaningGuard.Lock()
	for _, finishedTask := range finishedTasks {
		t := finishedTask.task
		if finishedTask.needsCleanup {
			mlog.Info(c.ctx, "compaction task needs cleanup",
				mlog.FieldCollectionID(t.GetTask().GetCollectionID()),
				mlog.Int64("planID", t.GetTask().GetPlanID()),
				mlog.String("state", t.GetTask().GetState().String()))
			c.cleaningTasks[t.GetTask().GetPlanID()] = t
		}
	}
	c.cleaningGuard.Unlock()
}

// cleanFailedTasks runs each task's cleanup logic. The inspector's periodic GC
// later removes task metadata after cleanup reaches the cleaned state.
func (c *compactionInspector) cleanFailedTasks() {
	c.cleaningGuard.RLock()
	tasks := lo.Values(c.cleaningTasks)
	c.cleaningGuard.RUnlock()

	for _, t := range tasks {
		planID := t.GetTask().GetPlanID()
		select {
		case <-c.stopCh:
			return
		default:
		}
		// Cleanup persists metadata and may have to wait out an in-flight worker
		// callback, so it must not run inline: the caller is the single
		// checkSchedule goroutine that also drives checkCompaction and schedule
		// for every other compaction task. One slow cleanup would stall them all.
		//
		// Membership in cleaningInFlight IS the cleanup slot, taken under the
		// guard before spawning: the same check bounds the fan-out and keeps a
		// task from being dispatched twice while its previous attempt still
		// runs, and a dispatch that finds no slot spawns nothing -- there is no
		// goroutine parked on a semaphore to drain at shutdown.
		c.cleaningGuard.Lock()
		_, busy := c.cleaningInFlight[planID]
		full := len(c.cleaningInFlight) >= maxConcurrentCleanups
		if !busy && !full {
			c.cleaningInFlight[planID] = struct{}{}
		}
		c.cleaningGuard.Unlock()
		if busy || full {
			continue
		}
		// In production this runs on the loopSchedule goroutine, which holds a
		// stopWg count of its own, so Add never races the Wait inside stop().
		c.stopWg.Add(1)
		go func(t CompactionTask, planID int64) {
			defer c.stopWg.Done()
			defer func() {
				c.cleaningGuard.Lock()
				delete(c.cleaningInFlight, planID)
				c.cleaningGuard.Unlock()
			}()
			// retrying is the persisted rebuild decision made when the attempt
			// ended. Do not re-derive it from RetryTimes and the refreshable cap;
			// legacy timeout records are settled and only release their inputs.
			retrying := t.GetTask().GetState() == datapb.CompactionTaskState_retrying
			var replacementProto *datapb.CompactionTask
			var replacementTask CompactionTask
			if retrying {
				// Allocate and validate the fresh attempt before touching the old
				// attempt's artifacts. No metadata is written here.
				replacementProto = c.buildReplacement(t)
				if replacementProto == nil {
					return
				}
				var err error
				replacementTask, err = c.buildCompactTask(replacementProto)
				if err != nil {
					mlog.Warn(c.ctx, "failed to build replacement compaction task",
						mlog.Int64("oldPlanID", planID),
						mlog.Int64("newPlanID", replacementProto.GetPlanID()),
						mlog.Err(err))
					return
				}
			}

			// Finalize drops the task from dispatch first, so no further plan can
			// be handed to a worker, then runs cleanup under the same per-task
			// lock as the callbacks -- waiting for any in-flight one to drain.
			cleaned := false
			c.scheduler.Finalize(planID, func() {
				if !retrying {
					task := t.GetTask()
					if task.GetType() == datapb.CompactionType_ClusteringCompaction &&
						task.GetState() != datapb.CompactionTaskState_completed {
						cleaned = c.finalizeAnalyzeTaskCleanup(task.GetAnalyzeTaskID(), t.Clean)
					} else {
						cleaned = t.Clean()
					}
					return
				}

				// Only clustering compaction can publish intermediate artifacts
				// before the attempt finishes. Remove those while the old metadata
				// and input claim still identify their owner. Fence its Analyze child
				// before cleanRetry removes that child's metadata, just as the
				// non-retry cleanup path does above.
				retryCleanup := func() bool {
					cleaner, ok := t.(interface{ cleanRetry() bool })
					return !ok || cleaner.cleanRetry()
				}
				if task := t.GetTask(); task.GetType() == datapb.CompactionType_ClusteringCompaction {
					if !c.finalizeAnalyzeTaskCleanup(task.GetAnalyzeTaskID(), retryCleanup) {
						return
					}
				} else if !retryCleanup() {
					return
				}
				if err := c.meta.ReplaceCompactionTask(c.ctx, t.GetTask(), replacementProto); err != nil {
					return
				}
				cleaned = true
			})
			if !cleaned {
				// The predecessor remains the only persisted owner of the work and
				// retains its input claim. The next business interval retries.
				return
			}

			// Publish the replacement to the in-memory queue before lifting the
			// old task's channel/label exclusion. The input claim was deliberately
			// retained across the catalog swap, so no admission gap exists.
			if retrying {
				c.submitTask(replacementTask)
			}
			c.cleaningGuard.Lock()
			delete(c.cleaningTasks, planID)
			c.cleaningGuard.Unlock()

			// Cleanup is complete at this point. Send an opportunistic worker
			// release without holding the cleanup slot or delaying a replacement.
			// Non-retry cleanup already fenced Analyze metadata deletion, and the
			// retained cleaned compaction record gates its eventual metadata GC.
			// Finalize took the task out of dispatch, so no further scheduler
			// round can reach this task. A round that already observed the
			// terminal state may have sent its own drop before that; see
			// releaseWorkerResources on why a second one is harmless.
			c.releaseWorkerResources(t.GetTask())
		}(t, planID)
	}
}

// finalizeAnalyzeTaskCleanup fences scheduler ownership and runs cleanup before
// releasing that fence. If cleanup deletes the Analyze meta, the inspector
// cannot observe and enqueue the old task in a Drop-to-delete gap.
func (c *compactionInspector) finalizeAnalyzeTaskCleanup(analyzeTaskID int64, cleanup func() bool) bool {
	if analyzeTaskID <= 0 {
		return cleanup()
	}
	analyzeMeta := c.meta.GetAnalyzeMeta()
	if analyzeMeta == nil {
		return cleanup()
	}
	analyzeTask := analyzeMeta.GetTask(analyzeTaskID)
	if analyzeTask == nil {
		return cleanup()
	}
	if c.scheduler == nil {
		mlog.Warn(c.ctx, "cannot fence analyze task before metadata cleanup",
			mlog.FieldTaskID(analyzeTaskID))
		return false
	}
	var (
		dropErr                error
		assignedWithoutCluster bool
		nodeID                 int64
		cleaned                bool
	)
	// Fence the analyze scheduler callback before deleting the metadata it may
	// still update. This is a different scheduler key from the enclosing
	// compaction Finalize, so the two ownership handoffs do not self-deadlock.
	c.scheduler.Finalize(analyzeTaskID, func() {
		// Assign/Create may have completed while Finalize waited for an in-flight
		// callback. Re-read only after ownership is fenced; the pre-Finalize
		// snapshot is not authoritative for worker ownership.
		latest := analyzeMeta.GetTask(analyzeTaskID)
		if latest == nil || latest.GetNodeID() <= 0 {
			cleaned = cleanup()
			return
		}
		nodeID = latest.GetNodeID()
		if c.cluster == nil {
			assignedWithoutCluster = true
			return
		}
		dropErr = c.cluster.DropAnalyze(nodeID, analyzeTaskID)
		if dropErr == nil || errors.Is(dropErr, merr.ErrNodeNotFound) {
			cleaned = cleanup()
		}
	})
	if assignedWithoutCluster {
		mlog.Warn(c.ctx, "cannot drop assigned analyze task before metadata cleanup",
			mlog.FieldTaskID(analyzeTaskID),
			mlog.FieldNodeID(nodeID))
		return false
	}
	if dropErr != nil && !errors.Is(dropErr, merr.ErrNodeNotFound) {
		mlog.Warn(c.ctx, "failed to drop analyze task before metadata cleanup",
			mlog.FieldTaskID(analyzeTaskID),
			mlog.FieldNodeID(nodeID),
			mlog.Err(dropErr))
		return false
	}
	return cleaned
}

// isFull reports whether the queue can take no more work. Capacity 0 means
// unbounded -- the same reading CompactionQueue.Enqueue uses, and the two must
// not disagree: a stricter isFull would reject every new compaction while
// Enqueue happily accepted the recovered ones.
func (c *compactionInspector) isFull() bool {
	return c.queueTasks.IsFull()
}

func (c *compactionInspector) checkDelay(t CompactionTask) {
	maxExecDuration := maxCompactionTaskExecutionDuration[t.GetTask().GetType()]
	startTime := time.Unix(t.GetTask().GetStartTime(), 0)
	execDuration := time.Since(startTime)
	if execDuration >= maxExecDuration {
		mlog.RatedWarn(c.ctx, rate.Limit(1.0/60), "compaction task is delayed",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("type", t.GetTask().GetType().String()),
			mlog.String("state", t.GetTask().GetState().String()),
			mlog.FieldVChannel(t.GetTask().GetChannel()),
			mlog.FieldNodeID(t.GetTask().GetNodeID()),
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
		return task.GetTask().GetCollectionID() == collectionID
	}
}

func L0CompactionCompactionTaskFilter() compactionTaskFilter {
	return func(task CompactionTask) bool {
		return task.GetTask().GetType() == datapb.CompactionType_Level0DeleteCompaction
	}
}
