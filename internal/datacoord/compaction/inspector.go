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

package compaction

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/analyze"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
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

var maxCompactionTaskExecutionDuration = map[datapb.CompactionType]time.Duration{
	datapb.CompactionType_MixCompaction:               30 * time.Minute,
	datapb.CompactionType_Level0DeleteCompaction:      30 * time.Minute,
	datapb.CompactionType_ClusteringCompaction:        60 * time.Minute,
	datapb.CompactionType_SortCompaction:              20 * time.Minute,
	datapb.CompactionType_BumpSchemaVersionCompaction: 30 * time.Minute,
}

type CompactionInspector interface {
	Start()
	Stop()
	// enqueueCompaction start to enqueue compaction task and return immediately
	EnqueueCompaction(ctx context.Context, task *datapb.CompactionTask, options AdmissionOptions) error
	// isFull return true if the task pool is full
	isFull() bool
	attemptLimit(ctx context.Context, producer Producer, plans int) (int, string)
	recordAdmissionDeferral(producer Producer, reason string)
	// get compaction tasks by signal id
	GetCompactionTasksNumBySignalID(signalID int64) int
	GetCompactionInfo(ctx context.Context, signalID int64) *policy.CompactionInfo
	RemoveTasksByChannel(channel string)
	getCompactionTasksNum(filters ...compactionTaskFilter) int
}

var _ CompactionInspector = (*Inspector)(nil)

type Inspector struct {
	queueTasks *CompactionQueue
	admission  *compactionAdmission

	executingGuard lock.RWMutex
	executingTasks map[int64]ctask.CompactionTask // planID -> task

	cleaningGuard lock.RWMutex
	cleaningTasks map[int64]ctask.CompactionTask // planID -> task

	meta                 meta.CompactionMeta
	compactionTaskMeta   *ctask.CompactionTaskMeta
	indexMeta            *index.IndexMeta
	analyzeMeta          *analyze.AnalyzeMeta
	partitionStatsMeta   *partitionstats.PartitionStatsMeta
	compactionTargetMeta *targets.CompactionTargetMeta

	// partitionStatsCleaner removes stale partition stats versions; clustering tasks share it.
	partitionStatsCleaner partitionstats.PartitionStatsCleaner
	allocator             allocator.Allocator
	analyzeScheduler      task.GlobalScheduler
	handler               meta.Handler
	scheduler             task.GlobalScheduler
	ievm                  index.IndexEngineVersionManager

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup
}

func (c *Inspector) GetCompactionInfo(ctx context.Context, triggerID int64) *policy.CompactionInfo {
	tasks := c.compactionTaskMeta.GetCompactionTasksByTriggerID(triggerID)
	return policy.SummaryCompactionState(triggerID, tasks)
}

func (c *Inspector) GetCompactionTasksNumBySignalID(triggerID int64) int {
	cnt := 0
	c.queueTasks.ForEach(func(ct ctask.CompactionTask) {
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

func NewCompactionInspector(mt meta.CompactionMeta,
	compactionTaskMeta *ctask.CompactionTaskMeta, indexMeta *index.IndexMeta, analyzeMeta *analyze.AnalyzeMeta, partitionStatsMeta *partitionstats.PartitionStatsMeta,
	partitionStatsCleaner partitionstats.PartitionStatsCleaner, allocator allocator.Allocator, handler meta.Handler, scheduler task.GlobalScheduler, analyzeScheduler task.GlobalScheduler, ievm index.IndexEngineVersionManager,
) *Inspector {
	capacity := paramtable.Get().DataCoordCfg.CompactionTaskQueueCapacity.GetAsInt()
	return &Inspector{
		queueTasks:         NewCompactionQueue(capacity, getPrioritizer()),
		meta:               mt,
		compactionTaskMeta: compactionTaskMeta,
		indexMeta:          indexMeta,
		analyzeMeta:        analyzeMeta,
		partitionStatsMeta: partitionStatsMeta,

		partitionStatsCleaner: partitionStatsCleaner,
		allocator:             allocator,
		stopCh:                make(chan struct{}),
		executingTasks:        make(map[int64]ctask.CompactionTask),
		cleaningTasks:         make(map[int64]ctask.CompactionTask),
		handler:               handler,
		scheduler:             scheduler,
		analyzeScheduler:      analyzeScheduler,
		ievm:                  ievm,
	}
}

// InitAdmission installs the shared new-task bucket before DataCoord starts
// admitting work.
func (c *Inspector) InitAdmission() error {
	config, err := readAdmissionConfig()
	if err != nil {
		return err
	}
	c.admission = newCompactionAdmission(config)
	return nil
}

func (c *Inspector) checkSchedule() {
	err := c.checkCompaction()
	if err != nil {
		mlog.Info(context.TODO(), "fail to update compaction", mlog.Err(err))
	}
	c.cleanFailedTasks()
	c.schedule()
}

func (c *Inspector) schedule() []ctask.CompactionTask {
	selected := make([]ctask.CompactionTask, 0)
	if c.queueTasks.Len() == 0 {
		return selected
	}

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	clusterLabelExcludes := typeutil.NewSet[string]()

	c.executingGuard.RLock()
	for _, t := range c.executingTasks {
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
	c.executingGuard.RUnlock()

	excluded := make([]ctask.CompactionTask, 0)
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
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", meta.NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Executing).Inc()
	}
	return selected
}

func (c *Inspector) Start() {
	c.stopWg.Add(2)
	go c.loopSchedule()
	go c.loopClean()
}

func (c *Inspector) LoadMeta() {
	// TODO: make it compatible to all types of compaction with persist meta
	triggers := c.compactionTaskMeta.GetCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if ctask.IsCompactionTaskCleaned(task) {
				mlog.Info(context.TODO(), "compactionInspector loadMeta abandon compactionTask",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.String("type", task.GetType().String()),
					mlog.String("state", task.GetState().String()))
				continue
			} else {
				t, err := c.createCompactTask(context.TODO(), task)
				if err != nil {
					mlog.Info(context.TODO(), "compactionInspector loadMeta create compactionTask failed, try to clean it",
						mlog.Int64("planID", task.GetPlanID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", task.GetState().String()),
						mlog.Err(err),
					)
					// ignore the drop error
					c.compactionTaskMeta.DropCompactionTask(context.TODO(), task)
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
						c.meta.SetSegmentsCompacting(context.TODO(), task.GetInputSegments(), false)
						// ignore the drop error
						c.compactionTaskMeta.DropCompactionTask(context.Background(), task)
						continue
					}
					mlog.Info(context.TODO(), "compactionInspector loadMeta submitTask",
						mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
						mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
						mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
						mlog.String("type", task.GetType().String()),
						mlog.String("state", t.GetTaskProto().GetState().String()))
				} else {
					c.restoreTask(t)
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
}

func (c *Inspector) loopSchedule() {
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

func (c *Inspector) loopClean() {
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

func (c *Inspector) Clean() {
	c.cleanCompactionTaskMeta()
	c.cleanPartitionStats()
}

func (c *Inspector) cleanCompactionTaskMeta() {
	// gc clustering compaction tasks
	triggers := c.compactionTaskMeta.GetCompactionTasks()
	for _, tasks := range triggers {
		for _, task := range tasks {
			if task.State == datapb.CompactionTaskState_cleaned {
				retentionStart := task.GetEndTime()
				if retentionStart == 0 {
					// Tasks persisted before end_time was populated retain the
					// historical start-time behavior after an upgrade.
					retentionStart = task.GetStartTime()
				}
				duration := time.Since(time.Unix(retentionStart, 0)).Seconds()
				if duration > Params.DataCoordCfg.CompactionDropToleranceInSeconds.GetAsDuration(time.Second).Seconds() {
					// try best to delete meta
					err := c.compactionTaskMeta.DropCompactionTask(context.TODO(), task)
					mlog.Debug(context.TODO(), "drop compaction task meta", mlog.Int64("planID", task.PlanID))
					if err != nil {
						mlog.Warn(context.TODO(), "fail to drop task", mlog.Int64("planID", task.PlanID), mlog.Err(err))
					}
				}
			}
		}
	}
}

func (c *Inspector) cleanPartitionStats() error {
	mlog.Debug(context.TODO(), "start gc partitionStats meta and files")
	// gc partition stats
	channelPartitionStatsInfos := make(map[string][]*datapb.PartitionStatsInfo)
	unusedPartStats := make([]*datapb.PartitionStatsInfo, 0)
	if c.partitionStatsMeta == nil {
		return nil
	}
	infos := c.partitionStatsMeta.ListAllPartitionStatsInfos()
	for _, info := range infos {
		collInfo := c.meta.(*meta.Meta).GetCollection(info.GetCollectionID())
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
		if err := c.partitionStatsCleaner.CleanPartitionStatsInfo(context.TODO(), info); err != nil {
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
				if err := c.partitionStatsCleaner.CleanPartitionStatsInfo(context.TODO(), info); err != nil {
					mlog.Warn(context.TODO(), "gcPartitionStatsInfo fail", mlog.Err(err))
					return err
				}
			}
		}
	}
	return nil
}

func (c *Inspector) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *Inspector) RemoveTasksByChannel(channel string) {
	mlog.Info(context.TODO(), "removing tasks by channel", mlog.String("channel", channel))
	c.queueTasks.RemoveAll(func(task ctask.CompactionTask) bool {
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

func (c *Inspector) submitTask(t ctask.CompactionTask) error {
	if err := c.queueTasks.Enqueue(t); err != nil {
		return err
	}
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", meta.NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
	return nil
}

// restoreTask used to restore Task from etcd
func (c *Inspector) restoreTask(t ctask.CompactionTask) {
	c.executingGuard.Lock()
	c.executingTasks[t.GetTaskProto().GetPlanID()] = t
	c.scheduler.Enqueue(t)
	c.executingGuard.Unlock()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", t.GetTaskProto().GetNodeID()), t.GetTaskProto().GetType().String(), metrics.Executing).Inc()
}

// getCompactionTask return compaction
func (c *Inspector) getCompactionTask(planID int64) ctask.CompactionTask {
	var t ctask.CompactionTask = nil
	c.queueTasks.ForEach(func(task ctask.CompactionTask) {
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

func (c *Inspector) SetCompactionTargetMeta(targets *targets.CompactionTargetMeta) {
	c.compactionTargetMeta = targets
}

func (c *Inspector) EnqueueCompaction(ctx context.Context, task *datapb.CompactionTask, options AdmissionOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.Producer != ProducerTBC {
		metrics.DataCoordCompactionAdmission.WithLabelValues(options.Producer.String(), admissionTaskType(task.GetType()), "plan", "").Inc()
	}
	queued := false
	if c.admission != nil {
		if options.Producer >= meta.CompactionProducerCount {
			return merr.WrapErrServiceInternalMsg("unknown compaction producer %d", options.Producer)
		}
		permit, reason := c.admission.acquire(ctx, options.Producer, c.remainingCapacity(options.DependencySort))
		if permit == nil {
			return c.admissionLimited(ctx, task, options.Producer, reason)
		}
		defer func() { permit.settle(queued) }()
	}
	if c.compactionTargetMeta != nil {
		defer c.compactionTargetMeta.LockCollectionAdmission(task.GetCollectionID())()
	}
	if err := c.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(task); err != nil {
		return err
	}
	if task.GetOutputSegmentCountHint() != 0 {
		task = proto.CloneOf(task)
		task.OutputSegmentCountHint = 0
	}
	log := mlog.With(mlog.Int64("planID", task.GetPlanID()), mlog.Int64("triggerID", task.GetTriggerID()), mlog.FieldCollectionID(task.GetCollectionID()), mlog.String("type", task.GetType().String()))
	t, err := c.createCompactTask(ctx, task)
	if err != nil {
		// Conflict is normal
		if errors.Is(err, merr.ErrCompactionPlanConflict) {
			log.RatedInfo(ctx, rate.Limit(60), "Failed to create compaction task, compaction plan conflict", mlog.Err(err))
		} else {
			log.Warn(ctx, "Failed to create compaction task, unable to create compaction task", mlog.Err(err))
		}
		return err
	}

	admitted := t.ShadowClone(ctask.SetStartTime(time.Now().Unix()))
	switch task.GetType() {
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction,
		datapb.CompactionType_ClusteringCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
		admitted.CreateTs, err = c.allocator.AllocTimestamp(ctx)
		if err == nil && admitted.GetCreateTs() == 0 {
			err = merr.WrapErrServiceInternalMsg("allocator returned a zero compaction admission timestamp")
		}
		if err != nil {
			c.meta.SetSegmentsCompacting(context.WithoutCancel(ctx), task.GetInputSegments(), false)
			log.Warn(ctx, "Failed to enqueue compaction task, unable to allocate task create timestamp", mlog.Err(err))
			return err
		}
	}
	t.SetTask(admitted)
	err = t.SaveTaskMeta()
	if err != nil {
		c.meta.SetSegmentsCompacting(context.WithoutCancel(ctx), t.GetTaskProto().GetInputSegments(), false)
		log.Warn(ctx, "Failed to enqueue compaction task, unable to save task meta", mlog.Err(err))
		return err
	}
	if err = c.submitTask(t); err != nil {
		log.Warn(ctx, "submit compaction task failed", mlog.Err(err))
		c.meta.SetSegmentsCompacting(context.WithoutCancel(ctx), t.GetTaskProto().GetInputSegments(), false)
		if c.admission != nil && errors.Is(err, errFull) {
			return c.admissionLimited(ctx, task, options.Producer, admissionCapacityLimit)
		}
		return err
	}
	queued = true
	metrics.DataCoordCompactionAdmission.WithLabelValues(options.Producer.String(), admissionTaskType(task.GetType()), "enqueue", "").Inc()
	log.Info(ctx, "Compaction plan submitted")
	return nil
}

// set segments compacting, one segment can only participate one compactionTask
func (c *Inspector) createCompactTask(ctx context.Context, t *datapb.CompactionTask) (ctask.CompactionTask, error) {
	var task ctask.CompactionTask
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction:
		task = ctask.NewMixCompactionTask(t, c.allocator, c.meta, c.compactionTaskMeta, c.ievm)
	case datapb.CompactionType_Level0DeleteCompaction:
		task = ctask.NewL0CompactionTask(t, c.allocator, c.meta, c.compactionTaskMeta)
	case datapb.CompactionType_ClusteringCompaction:
		task = ctask.NewClusteringCompactionTask(t, c.allocator, c.meta, c.compactionTaskMeta, c.indexMeta, c.analyzeMeta, c.partitionStatsMeta, c.partitionStatsCleaner, c.handler, c.analyzeScheduler, c.ievm)
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		task = ctask.NewBumpSchemaVersionTask(t, c.allocator, c.meta, c.compactionTaskMeta, c.ievm)
	default:
		return nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
	exist, succeed := c.meta.CheckAndSetSegmentsCompacting(ctx, t.GetInputSegments())
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
func (c *Inspector) checkCompaction() error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	var finishedTasks []ctask.CompactionTask
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
	for _, t := range finishedTasks {
		if t.GetTaskProto().GetState() == datapb.CompactionTaskState_failed ||
			t.GetTaskProto().GetState() == datapb.CompactionTaskState_timeout ||
			t.GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
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
func (c *Inspector) cleanFailedTasks() {
	c.cleaningGuard.RLock()
	cleanedTasks := make([]ctask.CompactionTask, 0)
	for _, t := range c.cleaningTasks {
		clean := t.Clean()
		if clean {
			cleanedTasks = append(cleanedTasks, t)
		}
	}
	c.cleaningGuard.RUnlock()
	c.cleaningGuard.Lock()
	for _, t := range cleanedTasks {
		delete(c.cleaningTasks, t.GetTaskProto().GetPlanID())
	}
	c.cleaningGuard.Unlock()
}

// isFull is the soft backlog check for legacy selectors.
func (c *Inspector) isFull() bool {
	return c.remainingCapacity(false) == 0
}

func (c *Inspector) remainingCapacity(dependencySort bool) int {
	pending := c.queueTasks.Len()
	available := int(^uint(0) >> 1)
	if c.queueTasks.capacity > 0 {
		available = max(0, c.queueTasks.capacity-pending)
	}
	if maxTasks := Params.DataCoordCfg.CompactionMaxTaskNum.GetAsInt(); !dependencySort && maxTasks > 0 {
		c.executingGuard.RLock()
		running := len(c.executingTasks)
		c.executingGuard.RUnlock()
		available = min(available, max(0, maxTasks-pending-running))
	}
	return available
}

func (c *Inspector) attemptLimit(ctx context.Context, producer Producer, plans int) (int, string) {
	capacity := c.remainingCapacity(false)
	if c.admission == nil {
		return min(capacity, plans), admissionCapacityLimit
	}
	return c.admission.attemptLimit(ctx, producer, capacity, plans)
}

func (c *Inspector) recordAdmissionDeferral(producer Producer, reason string) {
	if c.admission != nil && (reason == admissionRateLimit || reason == admissionShareLimit) {
		c.admission.recordDeferral(producer)
	}
}

func (c *Inspector) admissionLimited(ctx context.Context, task *datapb.CompactionTask, producer Producer, reason string) error {
	metrics.DataCoordCompactionAdmission.WithLabelValues(producer.String(), admissionTaskType(task.GetType()), "admission", reason).Inc()
	mlog.RatedInfo(ctx, rate.Limit(1), "compaction task deferred at admission",
		mlog.String("producer", producer.String()), mlog.String("reason", reason),
		mlog.FieldCollectionID(task.GetCollectionID()), mlog.Int64("planID", task.GetPlanID()))
	return ErrAdmissionLimited
}

func admissionTaskType(typ datapb.CompactionType) string {
	if _, known := maxCompactionTaskExecutionDuration[typ]; !known {
		return "other"
	}
	return typ.String()
}

func (c *Inspector) checkDelay(t ctask.CompactionTask) {
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

func (c *Inspector) getCompactionTasksNum(filters ...compactionTaskFilter) int {
	cnt := 0
	isMatch := func(task ctask.CompactionTask) bool {
		for _, f := range filters {
			if !f(task) {
				return false
			}
		}
		return true
	}
	c.queueTasks.ForEach(func(task ctask.CompactionTask) {
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

type compactionTaskFilter func(task ctask.CompactionTask) bool

func CollectionIDCompactionTaskFilter(collectionID int64) compactionTaskFilter {
	return func(task ctask.CompactionTask) bool {
		return task.GetTaskProto().GetCollectionID() == collectionID
	}
}

func L0CompactionCompactionTaskFilter() compactionTaskFilter {
	return func(task ctask.CompactionTask) bool {
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
