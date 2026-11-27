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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// TestSelectFlushedSegment_RespectsCommitTimestamp verifies that import segments
// with a commit_timestamp are excluded from L0 compaction when the trigger
// position is before the commit_timestamp.
func TestSelectFlushedSegment_RespectsCommitTimestamp(t *testing.T) {
	channel := "ch-1"

	// Import segment: start_position.ts=1000, commit_ts=5000.
	// Its effective timestamp is 5000 (controlled by segmentEffectiveTs).
	importSeg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              777,
		CollectionID:    1,
		PartitionID:     10,
		InsertChannel:   channel,
		State:           commonpb.SegmentState_Flushed,
		Level:           datapb.SegmentLevel_L1,
		CommitTimestamp: 5000,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
	}}

	// applyFilters applies a SegmentFilter slice to a candidate list,
	// mirroring what meta.SelectSegments does internally.
	applyFilters := func(candidates []*meta.SegmentInfo, filters ...meta.SegmentFilter) []*meta.SegmentInfo {
		var result []*meta.SegmentInfo
		for _, seg := range candidates {
			pass := true
			for _, f := range filters {
				if !f.Match(seg) {
					pass = false
					break
				}
			}
			if pass {
				result = append(result, seg)
			}
		}
		return result
	}

	makeTask := func(triggerTs uint64) *L0CompactionTask {
		mockAlloc := allocator.NewMockAllocator(t)
		mockMeta := NewMockCompactionMeta(t)
		mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, filters ...meta.SegmentFilter) []*meta.SegmentInfo {
				return applyFilters([]*meta.SegmentInfo{importSeg}, filters...)
			})
		return NewL0CompactionTask(&datapb.CompactionTask{
			PlanID:       1,
			TriggerID:    19530,
			CollectionID: 1,
			PartitionID:  10,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
			Channel:      channel,
			Pos:          &msgpb.MsgPosition{Timestamp: triggerTs},
		}, mockAlloc, mockMeta, nil)
	}

	t.Run("import segment not selected when trigger pos < commit_timestamp", func(t *testing.T) {
		// triggerTs=3000 < commit_ts=5000 → segment must NOT be selected
		task := makeTask(3000)
		selected, _, err := task.SelectFlushedSegment()
		assert.NoError(t, err)
		assert.Empty(t, selected, "import segment with commit_ts=5000 must not be selected at triggerTs=3000")
	})

	t.Run("import segment selected when trigger pos > commit_timestamp", func(t *testing.T) {
		// triggerTs=6000 > commit_ts=5000 → segment must be selected
		task := makeTask(6000)
		selected, _, err := task.SelectFlushedSegment()
		assert.NoError(t, err)
		assert.Len(t, selected, 1, "import segment with commit_ts=5000 must be selected at triggerTs=6000")
		assert.Equal(t, int64(777), selected[0].GetID())
	})
}
