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

package targets

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func IsOptimize(record *datapb.CompactionTarget) bool {
	return record.GetIntent() == datapb.TargetIntent_INTENT_OPTIMIZE && record.GetOptimize() != nil
}

func isActiveOptimize(record *datapb.CompactionTarget) bool {
	return IsOptimize(record) && record.GetState() == datapb.TargetState_TARGET_STATE_ACTIVE
}

// ActiveOptimize refreshes an unresolved Create or Stop before returning the
// current scope to another admission path.
func (m *CompactionTargetMeta) ActiveOptimize(ctx context.Context, collectionID int64) (*datapb.CompactionTarget, error) {
	m.mu.RLock()
	id := m.activeOptimize[collectionID]
	if pending := m.pendingOptimize[collectionID]; pending != 0 {
		id = pending
	}
	m.mu.RUnlock()
	if id == 0 {
		return nil, nil
	}
	record, err := m.Read(ctx, id)
	if err != nil || isActiveOptimize(record) {
		return record, err
	}
	return nil, nil
}

func (m *CompactionTargetMeta) CheckOptimizeConflict(ctx context.Context, collectionID int64) error {
	record, err := m.ActiveOptimize(ctx, collectionID)
	if err != nil || record == nil {
		return err
	}
	return merr.Wrapf(merr.ErrCompactionPlanConflict, "collection %d already has active OPTIMIZE target %d; this Create was not admitted", collectionID, record.GetTargetID())
}

// OptimizeScopeContains compares the dynamic finite range against current
// segment facts, including the same fallback as a collection REWRITE.
func OptimizeScopeContains(record *datapb.CompactionTarget, segment *meta.SegmentInfo) bool {
	return policy.IsNormalManualCompactionSegment(segment) &&
		segment.GetCollectionID() == record.GetCollectionID() &&
		dataStart(segment) < record.GetExpectedTS()
}

func dataStart(segment *meta.SegmentInfo) uint64 {
	if start := segment.GetEarliestTs(); start != 0 {
		return start
	}
	return meta.SegmentEffectiveTs(segment.SegmentInfo)
}

type OptimizeObservation struct {
	Scope     []*meta.SegmentInfo
	Match     []*meta.SegmentInfo
	Rows      int64
	Satisfied bool
	sizes     map[int64]int64
}

const optimizeTailLimit = 1

func NewOptimizeRecord(
	ctx context.Context, alloc allocator.Allocator, mt *meta.Meta, collection *meta.CollectionInfo,
	topologyQuerier view.CollectionTopologyQuerier, inputSize, minimumSize int64,
) (*datapb.CompactionTarget, error) {
	if minimumSize <= 0 {
		return nil, merr.WrapErrServiceUnavailableMsg("the minimum OPTIMIZE size is unavailable")
	}
	if inputSize < minimumSize {
		return nil, merr.WrapErrParameterInvalidMsg("input_target_size_bytes %d must be at least %d bytes", inputSize, minimumSize)
	}
	record, err := newFiniteRecord(ctx, alloc, collection.ID)
	if err != nil {
		return nil, err
	}
	record.Intent = datapb.TargetIntent_INTENT_OPTIMIZE
	record.TailLimit = optimizeTailLimit
	scope := optimizeScope(ctx, mt, record)
	var insertBytes float64
	for _, segment := range scope {
		size := segment.EnsureStats().GetInsertBinlogSize()
		if size < 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid insert size at segment %d", segment.GetID())
		}
		insertBytes += float64(size)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if topologyQuerier == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("OPTIMIZE topology is initializing")
	}
	topology, err := topologyQuerier.GetCollectionTopology(ctx, collection.ID)
	if err != nil {
		return nil, err
	}
	if topology == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("OPTIMIZE topology is unavailable")
	}
	snapshot := *topology
	snapshot.NumShards = len(collection.VChannelNames)
	actualSize := view.CalculateForceMergeTargetSize(ctx, &snapshot, minimumSize, inputSize, insertBytes)
	record.Optimize = &datapb.CompactionTargetOptimize{
		InputTargetSizeBytes: inputSize, ActualTargetSizeBytes: actualSize,
	}
	observation, err := observeOptimizeScope(ctx, record, scope)
	if err != nil {
		return nil, err
	}
	record.TotalRows = &observation.Rows
	return record, nil
}

func ObserveOptimize(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget) (OptimizeObservation, error) {
	if !IsOptimize(record) || record.GetCollectionID() <= 0 || record.GetExpectedTS() == 0 ||
		record.GetExpectedTS() != record.GetActivatedAtTS() || record.GetTailLimit() != optimizeTailLimit ||
		record.GetOptimize().GetInputTargetSizeBytes() <= 0 || record.GetOptimize().GetActualTargetSizeBytes() <= 0 {
		return OptimizeObservation{}, merr.WrapErrDataIntegrityMsg("invalid OPTIMIZE target %d", record.GetTargetID())
	}
	return observeOptimizeScope(ctx, record, optimizeScope(ctx, mt, record))
}

func optimizeScope(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget) []*meta.SegmentInfo {
	return mt.SelectSegments(ctx, meta.WithCollection(record.GetCollectionID()), meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return OptimizeScopeContains(record, segment)
	}))
}

func observeOptimizeScope(ctx context.Context, record *datapb.CompactionTarget, scope []*meta.SegmentInfo) (OptimizeObservation, error) {
	observation := OptimizeObservation{Scope: scope, Satisfied: true, sizes: make(map[int64]int64, len(scope))}
	tails := make(map[view.CompactionGroupLabel]int32)
	expected := record.GetOptimize().GetActualTargetSizeBytes()
	var firstError error
	for _, segment := range scope {
		if err := ctx.Err(); err != nil {
			return OptimizeObservation{}, err
		}
		rows := segment.GetNumOfRows()
		size := segment.GetResidualSegmentSize()
		if rows < 0 || size < 0 {
			observation.Satisfied = false
			if firstError == nil {
				firstError = merr.WrapErrDataIntegrityMsg("invalid OPTIMIZE observation at segment %d", segment.GetID())
			}
			continue
		}
		observation.sizes[segment.GetID()] = size
		position := view.ClassifyOptimizeSize(size, expected)
		if position == 0 {
			continue
		}
		if position > 0 {
			observation.Satisfied = false
		} else {
			group := view.CompactionGroupLabel{CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(), Channel: segment.GetInsertChannel()}
			tails[group]++
			if tails[group] > record.GetTailLimit() {
				observation.Satisfied = false
			}
		}
		if rows > math.MaxInt64-observation.Rows {
			observation.Satisfied = false
			if firstError == nil {
				firstError = merr.WrapErrDataIntegrityMsg("overflowing OPTIMIZE Match row count at segment %d", segment.GetID())
			}
		} else {
			observation.Rows += rows
		}
		observation.Match = append(observation.Match, segment)
	}
	if err := ctx.Err(); err != nil {
		return OptimizeObservation{}, err
	}
	return observation, firstError
}

// CheckClusteringConflict runs under collection admission and checks all
// clustering plans, including restored work and publication still cleaning.
func CheckClusteringConflict(ctx context.Context, mt *meta.Meta, tasks *task.CompactionTaskMeta, record *datapb.CompactionTarget) error {
	if tasks == nil {
		return nil
	}
	for _, plans := range tasks.GetCompactionTasksByCollection(record.GetCollectionID()) {
		for _, plan := range plans {
			if plan.GetType() != datapb.CompactionType_ClusteringCompaction || plan.GetState() == datapb.CompactionTaskState_cleaned {
				continue
			}
			for _, id := range plan.GetInputSegments() {
				if OptimizeScopeContains(record, mt.GetSegment(ctx, id)) {
					return merr.Wrapf(merr.ErrCompactionPlanConflict, "collection %d has overlapping Clustering plan %d; OPTIMIZE was not admitted", record.GetCollectionID(), plan.GetPlanID())
				}
			}
		}
	}
	return ctx.Err()
}

// optimizeHasInFlightWork checks current input ownership and L0's dynamic
// base selection. Ordinary L0 tasks do not lock their destination bases.
func optimizeHasInFlightWork(mt *meta.Meta, tasks map[int64][]*datapb.CompactionTask, scope []*meta.SegmentInfo) bool {
	for _, segment := range scope {
		if segment.IsCompacting || mt.IsSegmentCompacting(segment.GetID()) {
			return true
		}
	}
	for _, plans := range tasks {
		for _, plan := range plans {
			switch plan.GetState() {
			case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
			default:
				continue
			}
			switch plan.GetType() {
			case datapb.CompactionType_Level0DeleteCompaction, datapb.CompactionType_RestoreLevel0DeleteCompaction:
			default:
				continue
			}
			for _, segment := range scope {
				if plan.GetChannel() == segment.GetInsertChannel() &&
					(plan.GetPartitionID() == common.AllPartitionsID || plan.GetPartitionID() == segment.GetPartitionID()) &&
					(plan.GetType() == datapb.CompactionType_RestoreLevel0DeleteCompaction ||
						meta.SegmentEffectiveTs(segment.SegmentInfo) < plan.GetPos().GetTimestamp()) {
					return true
				}
			}
		}
	}
	return false
}
