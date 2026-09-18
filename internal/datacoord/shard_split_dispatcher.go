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
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// compactionDispatcher is the part of the compaction inspector the rewrite
// dispatches through.
type compactionDispatcher interface {
	enqueueCompaction(task *datapb.CompactionTask) error
}

// compactionPlanReader looks up dispatched plans to judge their state.
type compactionPlanReader interface {
	GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask
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
func (d *inspectorRewriteDispatcher) forTask(taskID int64) *inspectorRewriteDispatcher {
	scoped := *d
	scoped.taskID = taskID
	return &scoped
}

// DispatchHashSplit enqueues the rewrite of one source segment.
//
// Idempotent per (task, segment): a plan already enqueued for the segment and
// not failed is returned instead of enqueuing a second one, so a repeated round
// -- or a restart that replays the work list -- does not fan out duplicate
// rewrites of the same segment.
//
// Every refusal is a System error: the plan is built from what the split task
// recorded and what datacoord's meta holds, never from a request.
func (d *inspectorRewriteDispatcher) DispatchHashSplit(task *datapb.SplitShardTask, segmentID int64) (int64, error) {
	if existing := d.livePlanFor(task.GetTaskId(), segmentID); existing != 0 {
		return existing, nil
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
		PlanID:       planID,
		TriggerID:    task.GetTaskId(),
		State:        datapb.CompactionTaskState_pipelining,
		StartTime:    now,
		Type:         datapb.CompactionType_HashSplitCompaction,
		CollectionID: task.GetCollectionId(),
		PartitionID:  segment.GetPartitionID(),
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
	for _, plan := range d.planReader.GetCompactionTasksByTriggerID(d.ctx, d.taskID) {
		if plan.GetPlanID() != planID {
			continue
		}
		done, running = hashSplitPlanTerminalState(plan.GetState())
		if done || running {
			return done, running, nil
		}
		return false, false, plan.GetInputSegments()
	}
	return false, false, nil
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
// is done or still running, or 0 when there is none.
func (d *inspectorRewriteDispatcher) livePlanFor(taskID, segmentID int64) int64 {
	for _, plan := range d.planReader.GetCompactionTasksByTriggerID(d.ctx, taskID) {
		if plan.GetType() != datapb.CompactionType_HashSplitCompaction {
			continue
		}
		if len(plan.GetInputSegments()) != 1 || plan.GetInputSegments()[0] != segmentID {
			continue
		}
		if done, running := hashSplitPlanTerminalState(plan.GetState()); done || running {
			return plan.GetPlanID()
		}
	}
	return 0
}
