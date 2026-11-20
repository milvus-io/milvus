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

package task

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// transformCursorUpdates certifies only a continuous Transform prefix. A task
// can select a subset of L0 files, and task.Pos can even be MaxUint64 (repair),
// so neither that position nor the maximum selected Delete is a certificate.
func (t *L0CompactionTask) transformCursorUpdates(ctx context.Context, outputs []*datapb.CompactionSegment) ([]meta.UpdateOperator, error) {
	task := t.GetTaskProto()
	if task.GetType() == datapb.CompactionType_RestoreLevel0DeleteCompaction {
		return nil, nil // Restore retains the target WAL's cursor.
	}
	m, ok := t.meta.(*meta.Meta)
	if !ok {
		if task.GetMaterializedThrough() != 0 {
			return nil, merr.WrapErrServiceInternalMsg("L0 plan requires materialization metadata")
		}
		return nil, nil
	}
	if task.GetMaterializedThrough() == 0 && (m.StreamingNodeCatalog == nil || len(outputs) == 0) {
		return nil, nil // Legacy owners have no materialization certificate.
	}
	channel := task.GetChannel()
	through := task.GetMaterializedThrough()
	inputs := make(map[int64]struct{}, len(task.GetInputSegments()))
	for _, id := range task.GetInputSegments() {
		inputs[id] = struct{}{}
	}
	if through == 0 {
		// In-flight tasks from an earlier coordinator have no frozen proof.
		base, err := m.StreamingNodeCatalog.GetVChannelBase(ctx, funcutil.ToPhysicalChannel(channel), channel)
		if err != nil {
			return nil, err
		}
		if base == nil || base.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
			return nil, nil
		}
		for id := range inputs {
			if input := m.GetSegment(ctx, id); input != nil {
				through = max(through, meta.SegmentEffectiveDmlTs(input.SegmentInfo))
			}
		}
		through = min(through, base.GetTransformMaterializedTimeTick())
		if through == 0 {
			return nil, nil
		}
	}
	// Retired L0s have already been applied. Every still-live L0 outside this
	// result is a hole, even when another task has reserved it. Its first
	// Delete is excluded from the certified prefix. Unknown positions block
	// advancement; they are never interpreted as an unbounded interval.
	holes := make(map[int64]uint64)
	for _, input := range m.SelectSegments(ctx, meta.WithCollection(task.GetCollectionID()), meta.WithChannel(channel), meta.SegmentFilterFunc(func(s *meta.SegmentInfo) bool {
		return s.GetLevel() == datapb.SegmentLevel_L0 && meta.IsSegmentHealthy(s)
	})) {
		if _, selected := inputs[input.GetID()]; selected {
			continue
		}
		start := meta.SegmentEffectiveTs(input.SegmentInfo)
		if task.GetMaterializedThrough() != 0 {
			partition := input.GetPartitionID()
			if task.GetPartitionID() != common.AllPartitionsID && partition != common.AllPartitionsID && partition != task.GetPartitionID() {
				continue
			}
			start, end := meta.L0MutationTimeRange(input.SegmentInfo)
			if start <= through || end < start || input.GetIsImporting() {
				return nil, merr.WrapErrCompactionPlanConflict(fmt.Sprintf("L0 input %d appeared inside frozen scope through %d", input.GetID(), through))
			}
			continue
		}
		if input.GetIsImporting() {
			start = 0 // Its eventual commit timestamp has not been established.
		}
		limit := start
		if limit > 0 {
			limit-- // A Delete at start still needs replay.
		}
		partition := input.GetPartitionID()
		if old, exists := holes[partition]; !exists || limit < old {
			holes[partition] = limit
		}
	}
	updates := make([]meta.UpdateOperator, 0, len(outputs))
	for _, output := range outputs {
		segment := m.GetSegment(ctx, output.GetSegmentID())
		if segment == nil || !meta.IsSegmentHealthy(segment) || segment.GetTransformStartAfterTimetick() == 0 {
			continue
		}
		covered := through
		if limit, exists := holes[segment.GetPartitionID()]; exists {
			covered = min(covered, limit)
		}
		if limit, exists := holes[common.AllPartitionsID]; exists {
			covered = min(covered, limit)
		}
		if covered > segment.GetTransformStartAfterTimetick() {
			updates = append(updates, meta.AdvanceL0TransformCursor(segment.GetID(), covered))
		}
	}
	return updates, nil
}
