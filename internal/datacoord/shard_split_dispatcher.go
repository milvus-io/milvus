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
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// hashSplitRetryBackoffBase is how long a source segment waits after its
	// first failed rewrite plan before it is dispatched again; every further
	// failed plan compaction meta still holds for it doubles the wait.
	hashSplitRetryBackoffBase = 30 * time.Second
	// hashSplitRetryBackoffMax caps that wait.
	hashSplitRetryBackoffMax = 30 * time.Minute
	// hashSplitUnscheduledPlanGrace is how long a plan compaction meta records
	// as running may go unheld by the inspector before it is taken as lost.
	hashSplitUnscheduledPlanGrace = 5 * time.Minute
)

// compactionDispatcher is the part of the compaction inspector the rewrite
// dispatches through.
type compactionDispatcher interface {
	enqueueCompaction(task *datapb.CompactionTask) error
	// hasCompactionTask reports whether the inspector holds the plan, queued
	// or executing.
	hasCompactionTask(planID int64) bool
}

// hasCompactionTask reports whether the plan is executing or queued.
func (c *compactionInspector) hasCompactionTask(planID int64) bool {
	c.executingGuard.RLock()
	_, executing := c.executingTasks[planID]
	c.executingGuard.RUnlock()
	return executing || c.getCompactionTask(planID) != nil
}

// compactionPlanReader looks up dispatched plans to judge their state. It
// reads digests, not the plans: every plan carries the collection schema, and
// a split's trigger holds every plan it ever dispatched.
type compactionPlanReader interface {
	GetCompactionTaskDigestsByTriggerID(ctx context.Context, triggerID int64) []compactionTaskDigest
}

// hashSplitPlanIndex is one read of a split task's plans, indexed for a
// rewrite round: by plan id for the harvest, by input segment for dispatch.
type hashSplitPlanIndex struct {
	byPlan    map[int64]*compactionTaskDigest
	bySegment map[int64][]*compactionTaskDigest
}

func newHashSplitPlanIndex(digests []compactionTaskDigest) *hashSplitPlanIndex {
	index := &hashSplitPlanIndex{
		byPlan:    make(map[int64]*compactionTaskDigest, len(digests)),
		bySegment: make(map[int64][]*compactionTaskDigest, len(digests)),
	}
	for i := range digests {
		index.add(&digests[i])
	}
	return index
}

func (x *hashSplitPlanIndex) add(plan *compactionTaskDigest) {
	x.byPlan[plan.PlanID] = plan
	if plan.Type == datapb.CompactionType_HashSplitCompaction && len(plan.InputSegments) == 1 {
		segmentID := plan.InputSegments[0]
		x.bySegment[segmentID] = append(x.bySegment[segmentID], plan)
	}
}

// inspectorRewriteDispatcher is the production rewritePlanDispatcher: it turns
// one source segment into a HashSplitCompaction plan and hands it to the
// compaction inspector, then reads the plan's state back from compaction meta.
//
// The split task id doubles as the plans' TriggerID, so every rewrite plan of a
// task is found with one lookup, and a datacoord restart re-discovers the
// dispatched plans from meta instead of from memory.
type inspectorRewriteDispatcher struct {
	ctx        context.Context
	meta       *meta
	inspector  compactionDispatcher
	planReader compactionPlanReader
	alloc      allocator.Allocator
	// taskID scopes plan lookups to one split task (forTask).
	taskID int64
	// plans is the scoped task's plans, read once per scope -- one rewrite
	// round -- on first use; nil on an unscoped dispatcher, which reads fresh
	// every time.
	plans  *hashSplitPlanIndex
	scoped bool
}

func newInspectorRewriteDispatcher(
	ctx context.Context,
	m *meta,
	inspector compactionDispatcher,
	planReader compactionPlanReader,
	alloc allocator.Allocator,
) *inspectorRewriteDispatcher {
	return &inspectorRewriteDispatcher{
		ctx:        ctx,
		meta:       m,
		inspector:  inspector,
		planReader: planReader,
		alloc:      alloc,
	}
}

// forTask scopes the dispatcher to one split task for the duration of a
// rewrite round.
//
// A round reads the task's plans once. The trigger entry holds every plan the
// split ever dispatched, until housekeeping drops the cleaned ones, so a read
// per harvested plan and per dispatch made a round quadratic in the split's
// own progress.
func (d *inspectorRewriteDispatcher) forTask(taskID int64) *inspectorRewriteDispatcher {
	scoped := *d
	scoped.taskID = taskID
	scoped.plans = nil
	scoped.scoped = true
	return &scoped
}

// plansOf returns the plans of a split task: the scope's one read for the
// scoped task, a fresh read otherwise.
func (d *inspectorRewriteDispatcher) plansOf(taskID int64) *hashSplitPlanIndex {
	if !d.scoped || taskID != d.taskID {
		return newHashSplitPlanIndex(d.planReader.GetCompactionTaskDigestsByTriggerID(d.ctx, taskID))
	}
	if d.plans == nil {
		d.plans = newHashSplitPlanIndex(d.planReader.GetCompactionTaskDigestsByTriggerID(d.ctx, taskID))
	}
	return d.plans
}

// DispatchHashSplit enqueues the rewrite of one source segment.
//
// Idempotent per (task, segment): a plan already enqueued for the segment and
// not failed is returned instead of enqueuing a second one, so a repeated round
// -- or a restart that replays the work list -- does not fan out duplicate
// rewrites of the same segment.
//
// A segment whose earlier plans failed is backed off (hashSplitRetryDelay):
// while it waits, the call returns plan id 0 and no error, allocating nothing.
// A rewrite that fails deterministically would otherwise mint a new plan id,
// two segment ids and a persisted compaction task every round, forever.
//
// Every refusal is a System error: the plan is built from what the split task
// recorded and what datacoord's meta holds, never from a request.
func (d *inspectorRewriteDispatcher) DispatchHashSplit(task *datapb.SplitShardTask, segmentID int64) (int64, error) {
	existing, retryAt := d.livePlanFor(task.GetTaskId(), segmentID)
	if existing != 0 {
		return existing, nil
	}
	if time.Now().Before(retryAt) {
		return 0, nil
	}
	segment := d.meta.GetSegment(d.ctx, segmentID)
	if segment == nil {
		return 0, merr.WrapErrSegmentNotFound(segmentID, "cannot rewrite a segment absent from meta")
	}
	if source := splitTaskSource(task); segment.GetInsertChannel() != source {
		return 0, merr.WrapErrServiceInternalMsg(
			"refuse to rewrite segment %d of channel %s for shard split %d, whose source is %s",
			segmentID, segment.GetInsertChannel(), task.GetTaskId(), source)
	}
	collection := d.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		return 0, merr.WrapErrCollectionNotFound(task.GetCollectionId())
	}
	// The targets' residues mean nothing without the modulus they are taken
	// against: a plan missing either would partition rows by a rule no writer
	// follows, so it is refused before anything is allocated.
	if task.GetRoutingModulus() == 0 {
		return 0, merr.WrapErrServiceInternalMsg(
			"refuse to rewrite segment %d for shard split %d: the task carries no routing modulus", segmentID, task.GetTaskId())
	}
	if !splitTargetsAllocated(task) {
		return 0, merr.WrapErrServiceInternalMsg(
			"refuse to rewrite segment %d for shard split %d: the task does not name two target vchannels", segmentID, task.GetTaskId())
	}
	// The rewrite drops the rows the collection's TTL expired, as every other
	// compaction of the collection does.
	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		return 0, merr.WrapErrServiceInternalErr(err, "read the TTL of collection %d to rewrite segment %d", task.GetCollectionId(), segmentID)
	}

	planID, err := d.alloc.AllocID(d.ctx)
	if err != nil {
		return 0, err
	}
	// One output per target. Pre-allocating the ids keeps the datanode from
	// calling back for them mid-rewrite; a half is never larger than its input,
	// so one segment per target is enough.
	segIDBegin, segIDEnd, err := d.alloc.AllocN(int64(len(task.GetTargets())))
	if err != nil {
		return 0, err
	}
	now := time.Now().Unix()
	plan := &datapb.CompactionTask{
		PlanID:        planID,
		TriggerID:     task.GetTaskId(),
		State:         datapb.CompactionTaskState_pipelining,
		StartTime:     now,
		CollectionTtl: collectionTTL.Nanoseconds(),
		Type:          datapb.CompactionType_HashSplitCompaction,
		CollectionID:  task.GetCollectionId(),
		PartitionID:   segment.GetPartitionID(),
		// The plan runs on the SOURCE channel: that is where its input lives.
		// Its outputs are put on the targets through HashSplitTargets.
		Channel:                segment.GetInsertChannel(),
		InputSegments:          []int64{segmentID},
		ResultSegments:         []int64{},
		TotalRows:              segment.GetNumOfRows(),
		Schema:                 collection.Schema,
		LastStateStartTime:     now,
		MaxSize:                getExpectedSegmentSize(d.meta, task.GetCollectionId(), collection.Schema),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: segIDBegin, End: segIDEnd},
		HashSplitTargets:       task.GetTargets(),
		HashSplitModulus:       task.GetRoutingModulus(),
	}
	if err := d.inspector.enqueueCompaction(plan); err != nil {
		return 0, err
	}
	if d.scoped && task.GetTaskId() == d.taskID && d.plans != nil {
		// The rest of the round sees the plan it just enqueued.
		d.plans.add(&compactionTaskDigest{
			PlanID:        planID,
			Type:          plan.GetType(),
			State:         plan.GetState(),
			InputSegments: plan.GetInputSegments(),
			StartTime:     plan.GetStartTime(),
		})
	}
	return planID, nil
}

// HashSplitPlanState reports whether a dispatched plan committed (done), is
// still in flight (running), or is neither. inputSegments names the plan's own
// input segments when the plan record is still found but is neither done nor
// running; it is nil only when the record itself is gone.
//
// A completed plan is moved to "cleaned" by routine compaction-meta
// housekeeping shortly after it commits, and that transition alone does not
// say whether the plan committed or failed. So "neither" is not "lost" by
// itself: inputSegments lets the caller check the outputs' lineage to tell the
// two apart. The state is read from compaction meta, so the answer survives a
// datacoord restart.
func (d *inspectorRewriteDispatcher) HashSplitPlanState(planID int64) (done bool, running bool, inputSegments []int64) {
	plan, ok := d.plansOf(d.taskID).byPlan[planID]
	if !ok {
		return false, false, nil
	}
	done, running = d.planState(plan)
	if done || running {
		return done, running, nil
	}
	return false, false, plan.InputSegments
}

// planState is hashSplitPlanTerminalState, except that a plan compaction meta
// records as running but the inspector does not hold, past a grace period, is
// neither: enqueueCompaction saves a plan before it queues it, and a plan the
// queue refused would otherwise read as running until a restart revived it,
// its segment never rewritten meanwhile. The grace covers a plan read between
// its save and its queueing; the inspector is only asked about plans past it.
func (d *inspectorRewriteDispatcher) planState(plan *compactionTaskDigest) (done bool, running bool) {
	done, running = hashSplitPlanTerminalState(plan.State)
	if !running || time.Since(time.Unix(plan.StartTime, 0)) < hashSplitUnscheduledPlanGrace {
		return done, running
	}
	return false, d.inspector.hasCompactionTask(plan.PlanID)
}

// hashSplitPlanTerminalState maps a compaction task state onto the
// (done, running) pair the rewrite rounds consume.
func hashSplitPlanTerminalState(state datapb.CompactionTaskState) (done bool, running bool) {
	switch state {
	case datapb.CompactionTaskState_completed:
		return true, false
	case datapb.CompactionTaskState_pipelining,
		datapb.CompactionTaskState_executing,
		datapb.CompactionTaskState_meta_saved,
		datapb.CompactionTaskState_statistic,
		datapb.CompactionTaskState_indexing:
		return false, true
	default:
		// failed, timeout, cleaned, unknown: the plan will not commit (or
		// already did, which the caller tells from the lineage). A rewrite is
		// deterministic, so a retry reproduces the same outputs.
		return false, false
	}
}

// livePlanFor returns the id of a plan already dispatched for this segment that
// is done or still running, or 0 when there is none; and, when there is none,
// the earliest time the segment may be dispatched again after its failed
// plans (zero when it has none).
func (d *inspectorRewriteDispatcher) livePlanFor(taskID, segmentID int64) (int64, time.Time) {
	failed := 0
	var lastFailure int64
	for _, plan := range d.plansOf(taskID).bySegment[segmentID] {
		if done, running := d.planState(plan); done || running {
			return plan.PlanID, time.Time{}
		}
		// Neither done nor running for an input that is still to rewrite: the
		// plan failed. A committed plan dropped its input, which is never
		// dispatched again.
		failed++
		lastFailure = max(lastFailure, plan.StartTime, plan.EndTime)
	}
	if failed == 0 {
		return 0, time.Time{}
	}
	return 0, time.Unix(lastFailure, 0).Add(hashSplitRetryDelay(failed))
}

// hashSplitRetryDelay is how long a segment with this many failed rewrite
// plans waits after the last one: the base, doubled per further failure,
// capped. Failed plans leave compaction meta a while after they are cleaned,
// so the count, and the delay, relax over time.
func hashSplitRetryDelay(failedPlans int) time.Duration {
	delay := hashSplitRetryBackoffBase
	for i := 1; i < failedPlans && delay < hashSplitRetryBackoffMax; i++ {
		delay *= 2
	}
	return min(delay, hashSplitRetryBackoffMax)
}

// hashSplitDeleteSourceBinlogs turns the L0 segments a rewrite plan folds into
// plan segment entries carrying their deltalogs.
//
// The rewrite commit drops its input, and the source's L0s are retired once no
// data is left on the source to fold them. So the rewrite is the last chance to
// apply a delete that flushed into a source L0 and never reached an L1
// deltalog: every plan carries every healthy source L0 in its input's
// partition or in AllPartitions -- the scope the L0 compaction view uses, since
// a delete of another partition's rows must not touch this input -- and the
// datanode folds them while it routes rows, so the outputs it writes are
// already delete-applied.
//
// They are delete sources, not inputs: they are NOT on the task's
// InputSegments, so the commit neither rewrites nor drops them and the
// inspector never marks them compacting. Several plans of one task share them.
//
// Resolved at plan-build time rather than recorded at dispatch: a plan is
// rebuilt every time it is assigned to a worker, so a retry after a restart
// re-reads the set from meta. Nothing is dispatched before the source is past
// its T_switch, from which point its L0 set is final.
func hashSplitDeleteSourceBinlogs(segments []*SegmentInfo) []*datapb.CompactionSegmentBinlogs {
	return lo.Map(segments, func(info *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
		return &datapb.CompactionSegmentBinlogs{
			SegmentID:     info.GetID(),
			CollectionID:  info.GetCollectionID(),
			PartitionID:   info.GetPartitionID(),
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: info.GetInsertChannel(),
			Deltalogs:     info.GetDeltalogs(),
			Manifest:      info.GetManifestPath(),
		}
	})
}

// isHashSplitDeleteSource is the one definition of which L0 segment a rewrite
// plan on a channel and partition folds.
func isHashSplitDeleteSource(info *SegmentInfo, partitionID int64) bool {
	return isSegmentHealthy(info) &&
		info.GetLevel() == datapb.SegmentLevel_L0 &&
		(info.GetPartitionID() == common.AllPartitionsID || info.GetPartitionID() == partitionID)
}

// hashSplitDeleteSourceSegments lists the L0 segments a rewrite plan on this
// channel and partition folds, in id order so the same plan rebuilt twice is
// identical.
func hashSplitDeleteSourceSegments(ctx context.Context, m CompactionMeta, channel string, partitionID int64) []*SegmentInfo {
	segments := m.SelectSegments(ctx, WithChannel(channel), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return isHashSplitDeleteSource(info, partitionID)
	}))
	sortSegmentsByID(segments)
	return segments
}

// hashSplitPlanSegments reads, in ONE meta scan of the source channel (one
// segMu acquisition), a rewrite plan's healthy inputs by id and the L0 segments
// it folds, in id order.
//
// One snapshot is what keeps the plan's delete set whole: an L0 compaction
// commit appends the L0s' deletes to the input's deltalogs and retires those
// L0s in the same write, so reading the input before such a commit and the L0
// set after it would miss the deletes on both sides. An input that is not on
// the channel, or not healthy, is absent from the map.
func hashSplitPlanSegments(
	ctx context.Context,
	m CompactionMeta,
	channel string,
	partitionID int64,
	inputIDs []int64,
) (map[int64]*SegmentInfo, []*SegmentInfo) {
	wanted := typeutil.NewSet(inputIDs...)
	inputs := make(map[int64]*SegmentInfo, len(inputIDs))
	deleteSources := make([]*SegmentInfo, 0)
	for _, info := range m.SelectSegments(ctx, WithChannel(channel), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (wanted.Contain(info.GetID()) && isSegmentHealthy(info)) || isHashSplitDeleteSource(info, partitionID)
	})) {
		if wanted.Contain(info.GetID()) {
			inputs[info.GetID()] = info
			continue
		}
		deleteSources = append(deleteSources, info)
	}
	sortSegmentsByID(deleteSources)
	return inputs, deleteSources
}

func sortSegmentsByID(segments []*SegmentInfo) {
	slices.SortFunc(segments, func(a, b *SegmentInfo) int { return cmp.Compare(a.GetID(), b.GetID()) })
}

// hashSplitDeleteSourceRows counts the delete entries a rewrite plan on this
// channel and partition folds.
func hashSplitDeleteSourceRows(ctx context.Context, m CompactionMeta, channel string, partitionID int64) int64 {
	return lo.SumBy(hashSplitDeleteSourceSegments(ctx, m, channel, partitionID),
		func(info *SegmentInfo) int64 { return info.getDeltaCount() })
}

// hashSplitDeleteSourceSlot prices the delete set a rewrite folds, on top of
// the flat cost of rewriting one segment.
//
// The datanode builds one pk -> ts map over the folded deletes per plan and
// probes it per row, the work an L0 compaction does, priced the same way
// (l0CompactionTask.GetTaskSlot): the L0 slot factor over the delete rows, per
// bloom-filter apply batch. No minimum of one, unlike the L0 task: this is an
// addend, and the flat mix cost is already the floor. Pure in its inputs, so
// the caller reads the configuration once.
func hashSplitDeleteSourceSlot(deleteRows, applyBatchSize, slotFactor int64) int64 {
	if deleteRows <= 0 || applyBatchSize <= 0 {
		return 0
	}
	return slotFactor * deleteRows / applyBatchSize
}
