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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// segWithCoverage builds a *SegmentInfo carrying an optional delete_covered_ts
// (covered>0) and optional deltalog TimestampTo values.
func segWithCoverage(covered uint64, deltaTos ...uint64) *SegmentInfo {
	info := &datapb.SegmentInfo{DeleteCoveredTs: covered}
	if len(deltaTos) > 0 {
		binlogs := make([]*datapb.Binlog, 0, len(deltaTos))
		for _, to := range deltaTos {
			binlogs = append(binlogs, &datapb.Binlog{TimestampTo: to})
		}
		info.Deltalogs = []*datapb.FieldBinlog{{Binlogs: binlogs}}
	}
	return NewSegmentInfo(info)
}

func TestInputDeleteCoverageTs(t *testing.T) {
	t.Run("covered_ts_only", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(100))
		assert.True(t, known)
		assert.EqualValues(t, 100, ts)
	})
	t.Run("deltalogs_only_takes_max", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(0, 50, 120, 80))
		assert.True(t, known)
		assert.EqualValues(t, 120, ts)
	})
	t.Run("both_newest_wins", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(100, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 130, ts)
		ts, known = inputDeleteCoverageTs(segWithCoverage(200, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 200, ts)
	})
	t.Run("neither_is_unknown", func(t *testing.T) {
		_, known := inputDeleteCoverageTs(segWithCoverage(0))
		assert.False(t, known)
	})
}

func TestComputeDeleteCoveredTs(t *testing.T) {
	t.Run("min_across_inputs", func(t *testing.T) {
		// the least-covered input (150) bounds the output
		assert.EqualValues(t, 150, computeDeleteCoveredTs([]*SegmentInfo{
			segWithCoverage(0, 300), segWithCoverage(150), segWithCoverage(0, 200),
		}))
	})
	t.Run("any_unknown_input_yields_0", func(t *testing.T) {
		// 0 makes the delegator fall back to start_position (minTs), always safe
		assert.EqualValues(t, 0, computeDeleteCoveredTs([]*SegmentInfo{
			segWithCoverage(0, 300), segWithCoverage(0),
		}))
	})
	t.Run("empty_inputs_yields_0", func(t *testing.T) {
		assert.EqualValues(t, 0, computeDeleteCoveredTs(nil))
	})
	t.Run("single_input", func(t *testing.T) {
		assert.EqualValues(t, 77, computeDeleteCoveredTs([]*SegmentInfo{segWithCoverage(0, 77)}))
	})
}

// completion must copy CompactionTask.delete_covered_ts (computed at plan-build
// from the snapshot) onto the output, and must NOT re-derive it from live input
// metadata — a concurrent L0 compaction may have advanced the input's deltalog
// past the snapshot, which would over-state coverage and drop deletes (#49435).
func testCompletionUsesTaskCoveredTsNotLiveMeta(t *testing.T, compType datapb.CompactionType) {
	const channel = "by-dev-rootcoord-dml_0_v0"
	segments := NewSegmentsInfo()
	// input's LIVE deltalog reaches 900 (drifted), but the task was planned when
	// coverage was only 500.
	segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: channel,
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 2,
		Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{TimestampTo: 900}}}},
	}))
	m := &meta{catalog: &catalogkv.Catalog{MetaKv: NewMetaMemoryKV()}, segments: segments}

	out, _, err := m.CompleteCompactionMutation(context.Background(), &datapb.CompactionTask{
		InputSegments:   []int64{1},
		Type:            compType,
		Channel:         channel,
		DeleteCoveredTs: 500, // value snapshotted at plan-build time
	}, &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 2}},
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	// 500 (the baked snapshot), NOT 900 (drifted live meta)
	assert.EqualValues(t, 500, out[0].GetDeleteCoveredTs())
}

func TestCompleteCompactionMutationUsesTaskCoveredTs(t *testing.T) {
	t.Run("mix", func(t *testing.T) {
		testCompletionUsesTaskCoveredTsNotLiveMeta(t, datapb.CompactionType_MixCompaction)
	})
	t.Run("sort", func(t *testing.T) {
		testCompletionUsesTaskCoveredTsNotLiveMeta(t, datapb.CompactionType_SortCompaction)
	})
	t.Run("clustering", func(t *testing.T) {
		testCompletionUsesTaskCoveredTsNotLiveMeta(t, datapb.CompactionType_ClusteringCompaction)
	})
}
