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

package split

import (
	"fmt"
	"slices"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file decides, from the segments of a split source, what the rewrite of
// the source does next, when its window ends, and what its adoption moves.
// It reads only the record of the split and a snapshot of the segments, so
// every rule can be tested without a DataCoord.
//
// The source is fenced: from T_switch on it takes no DML, and no import or
// snapshot is admitted while the split is in its window. Its segments only
// change by flush of the residual, by L0 compaction, by the rewrite, and by
// the single-input compactions that keep a label (sort, schema bump).

// SourceSegment is a segment of a split source as the rewrite sees it.
type SourceSegment struct {
	Info *datapb.SegmentInfo
	// Compacting is true while a compaction task holds the segment.
	Compacting bool
	// Size is the size of the segment in bytes, used to size a batch.
	Size int64
}

// RewriteBatch is the input of one rewrite compaction: segments of one
// partition of the source, none labeled, none with an L0 of the source that
// still applies to it.
type RewriteBatch struct {
	PartitionID int64
	SegmentIDs  []int64
	Rows        int64
	Size        int64
}

// RewriteStep is what the rewrite of a split does next.
type RewriteStep struct {
	// FoldL0 is true while the source has L0 segments: ordinary L0
	// compaction must fold them into the segments of the source first.
	FoldL0 bool
	// Batches are the rewrites that can start now.
	Batches []RewriteBatch
	// Ended is true when the window of the split has ended on this cluster:
	// the residual has flushed and every segment of the source carries a
	// label.
	Ended bool
	// Waiting says, when the window has not ended, what it waits for.
	Waiting string
}

// IsHealthy reports whether a segment still holds data: not dropped, not
// absent.
func IsHealthy(segment *datapb.SegmentInfo) bool {
	switch segment.GetState() {
	case commonpb.SegmentState_SegmentStateNone, commonpb.SegmentState_NotExist, commonpb.SegmentState_Dropped:
		return false
	default:
		return true
	}
}

// CheckRewritable reports whether the record of a split carries what its
// rewrite needs: the routing modulus and the residues of every target, as
// the SplitShard message gave them.
func CheckRewritable(task *datapb.SplitShardTask) error {
	if task.GetRoutingModulus() == 0 {
		return merr.WrapErrServiceInternalMsg("shard split task %d has no routing modulus, its source cannot be rewritten", task.GetTaskId())
	}
	if len(task.GetTargets()) < 2 {
		return merr.WrapErrServiceInternalMsg("shard split task %d has %d targets, a rewrite needs two", task.GetTaskId(), len(task.GetTargets()))
	}
	owners := make(map[uint64]string)
	channels := make(map[string]bool)
	for _, target := range task.GetTargets() {
		if target.GetVchannel() == "" || channels[target.GetVchannel()] {
			return merr.WrapErrServiceInternalMsg("shard split task %d carries an empty or repeated target", task.GetTaskId())
		}
		channels[target.GetVchannel()] = true
		if len(target.GetResidues()) == 0 {
			return merr.WrapErrServiceInternalMsg("target %q of shard split task %d owns no residue", target.GetVchannel(), task.GetTaskId())
		}
		for _, residue := range target.GetResidues() {
			if residue >= task.GetRoutingModulus() {
				return merr.WrapErrServiceInternalMsg("target %q of shard split task %d owns residue %d, not below the modulus %d",
					target.GetVchannel(), task.GetTaskId(), residue, task.GetRoutingModulus())
			}
			if owner, ok := owners[residue]; ok {
				return merr.WrapErrServiceInternalMsg("targets %q and %q of shard split task %d both own residue %d",
					owner, target.GetVchannel(), task.GetTaskId(), residue)
			}
			owners[residue] = target.GetVchannel()
		}
	}
	return nil
}

// RewritePlan returns the routing a rewrite compaction of the split carries.
func RewritePlan(task *datapb.SplitShardTask) *datapb.SplitShardRewritePlan {
	plan := &datapb.SplitShardRewritePlan{RoutingModulus: task.GetRoutingModulus(), TtlCutoffTimetick: task.GetSwitchTimeTick()}
	for _, target := range task.GetTargets() {
		plan.Targets = append(plan.Targets, &datapb.SplitShardTaskTarget{
			Vchannel: target.GetVchannel(),
			Residues: slices.Clone(target.GetResidues()),
		})
	}
	return plan
}

func isTarget(task *datapb.SplitShardTask, vchannel string) bool {
	for _, target := range task.GetTargets() {
		if target.GetVchannel() == vchannel {
			return true
		}
	}
	return false
}

func isUnflushed(segment *datapb.SegmentInfo) bool {
	switch segment.GetState() {
	case commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
		return true
	default:
		return false
	}
}

// PlanRewrite decides the next step of the rewrite of a split from the
// segments of its source. segments must be every segment whose insert
// channel is the source.
//
// A segment is taken by a rewrite only once no L0 segment of the source
// applies to it: an L0 of its partition, or of all partitions. The source is
// frozen, so ordinary L0 compaction folds every L0 of the source in a finite
// number of steps; afterwards the cursor of the segment on the stream of the
// source is at T_switch and nothing of the source remains to be applied.
//
// Batches hold segments of one partition, at most maxBatchSize bytes unless
// one segment alone is larger. A segment that a compaction holds, or that is
// already labeled, is not taken.
func PlanRewrite(task *datapb.SplitShardTask, segments []SourceSegment, maxBatchSize int64) (RewriteStep, error) {
	if task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskRewriteDone {
		return RewriteStep{Ended: true}, nil
	}
	if task.GetState() < datapb.SplitShardTaskState_SplitShardTaskResidualFlushed {
		return RewriteStep{Waiting: "the residual of the source has not flushed"}, nil
	}
	if err := CheckRewritable(task); err != nil {
		return RewriteStep{}, err
	}

	l0Partitions := make(map[int64]struct{})
	var unflushed, importing, unlabelled int
	candidates := make(map[int64][]SourceSegment)
	for _, segment := range segments {
		info := segment.Info
		if info.GetInsertChannel() != task.GetSourceVchannel() || !IsHealthy(info) {
			continue
		}
		if info.GetIsImporting() {
			importing++
			continue
		}
		if info.GetLevel() == datapb.SegmentLevel_L0 {
			l0Partitions[info.GetPartitionID()] = struct{}{}
			continue
		}
		if isUnflushed(info) {
			unflushed++
			continue
		}
		if label := info.GetSplitTarget(); label != "" {
			if !isTarget(task, label) {
				return RewriteStep{}, merr.WrapErrDataIntegrityMsg("segment %d of split source %s is labeled with %q, which is not a target of shard split task %d",
					info.GetID(), task.GetSourceVchannel(), label, task.GetTaskId())
			}
			continue
		}
		unlabelled++
		candidates[info.GetPartitionID()] = append(candidates[info.GetPartitionID()], segment)
	}

	step := RewriteStep{FoldL0: len(l0Partitions) > 0}
	_, allPartitionsL0 := l0Partitions[common.AllPartitionsID]
	partitions := make([]int64, 0, len(candidates))
	for partitionID := range candidates {
		partitions = append(partitions, partitionID)
	}
	slices.Sort(partitions)
	for _, partitionID := range partitions {
		if _, ok := l0Partitions[partitionID]; ok || allPartitionsL0 {
			continue
		}
		step.Batches = append(step.Batches, batchSegments(partitionID, candidates[partitionID], maxBatchSize)...)
	}

	switch {
	case unflushed > 0:
		step.Waiting = fmt.Sprintf("%d segments of the source are not flushed", unflushed)
	case importing > 0:
		step.Waiting = fmt.Sprintf("%d segments of the source are importing", importing)
	case step.FoldL0:
		step.Waiting = fmt.Sprintf("L0 segments of the source remain in %d partitions", len(l0Partitions))
	case unlabelled > 0:
		step.Waiting = fmt.Sprintf("%d segments of the source carry no label", unlabelled)
	default:
		step.Ended = true
	}
	return step, nil
}

func batchSegments(partitionID int64, segments []SourceSegment, maxBatchSize int64) []RewriteBatch {
	sort.Slice(segments, func(i, j int) bool { return segments[i].Info.GetID() < segments[j].Info.GetID() })
	var batches []RewriteBatch
	var current RewriteBatch
	for _, segment := range segments {
		if segment.Compacting {
			continue
		}
		if len(current.SegmentIDs) > 0 && maxBatchSize > 0 && current.Size+segment.Size > maxBatchSize {
			batches = append(batches, current)
			current = RewriteBatch{}
		}
		current.PartitionID = partitionID
		current.SegmentIDs = append(current.SegmentIDs, segment.Info.GetID())
		current.Rows += segment.Info.GetNumOfRows()
		current.Size += segment.Size
	}
	if len(current.SegmentIDs) > 0 {
		batches = append(batches, current)
	}
	return batches
}

// AdoptionMove is one segment the adoption of a split moves from the source to
// a target.
type AdoptionMove struct {
	SegmentID   int64
	PartitionID int64
	Target      string
	// TransformStart is the cursor of the segment on the stream of its
	// target: the pin of the target. The segment holds no row and no delete
	// of the target, and every event of the target is above the pin.
	TransformStart uint64
}

// PlanAdoption returns the moves of the adoption of a split, or an error
// that says why the split cannot be adopted now.
//
// The preconditions are those of the end of the window, checked again on the
// segments of the moment: the rewrite is done (state RewriteDone), and every
// healthy segment of the source is flushed, of level L1 or L2, not importing,
// not held by a compaction, and labeled with a target. A split already
// adopted has nothing to move.
func PlanAdoption(task *datapb.SplitShardTask, segments []SourceSegment) ([]AdoptionMove, error) {
	if task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskAdopted {
		return nil, nil
	}
	if task.GetState() < datapb.SplitShardTaskState_SplitShardTaskRewriteDone {
		return nil, merr.WrapErrServiceNotReadyMsg("shard split task %d is in state %s, its source is not rewritten yet",
			task.GetTaskId(), task.GetState().String())
	}
	pins := make(map[string]uint64, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		pins[target.GetVchannel()] = TransformPin(task.GetSwitchTimeTick(), target.GetGenesisTimeTick())
	}
	var moves []AdoptionMove
	for _, segment := range segments {
		info := segment.Info
		if info.GetInsertChannel() != task.GetSourceVchannel() || !IsHealthy(info) {
			continue
		}
		var reason string
		switch {
		case info.GetLevel() == datapb.SegmentLevel_L0:
			reason = "is an L0 segment"
		case isUnflushed(info):
			reason = "is not flushed"
		case info.GetIsImporting():
			reason = "is importing"
		case segment.Compacting:
			reason = "is held by a compaction"
		case info.GetSplitTarget() == "":
			reason = "carries no label"
		case !isTarget(task, info.GetSplitTarget()):
			return nil, merr.WrapErrDataIntegrityMsg("segment %d of split source %s is labeled with %q, which is not a target of shard split task %d",
				info.GetID(), task.GetSourceVchannel(), info.GetSplitTarget(), task.GetTaskId())
		}
		if reason != "" {
			return nil, merr.WrapErrServiceNotReadyMsg("shard split task %d cannot be adopted: segment %d of its source %s %s",
				task.GetTaskId(), info.GetID(), task.GetSourceVchannel(), reason)
		}
		moves = append(moves, AdoptionMove{
			SegmentID:      info.GetID(),
			PartitionID:    info.GetPartitionID(),
			Target:         info.GetSplitTarget(),
			TransformStart: pins[info.GetSplitTarget()],
		})
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].SegmentID < moves[j].SegmentID })
	return moves, nil
}
