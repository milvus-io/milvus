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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
