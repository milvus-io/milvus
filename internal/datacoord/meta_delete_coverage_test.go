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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// segWithCoverage builds a *SegmentInfo carrying an optional
// delete_covered_position (covered>0) and optional deltalog TimestampTo values.
func segWithCoverage(covered uint64, deltaTos ...uint64) *SegmentInfo {
	info := &datapb.SegmentInfo{}
	if covered > 0 {
		info.DeleteCoveredPosition = &msgpb.MsgPosition{Timestamp: covered}
	}
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
	t.Run("covered_position_only", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(100))
		assert.True(t, known)
		assert.EqualValues(t, 100, ts)
	})

	t.Run("deltalogs_only_takes_max_timestamp_to", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(0, 50, 120, 80))
		assert.True(t, known)
		assert.EqualValues(t, 120, ts)
	})

	t.Run("both_present_newest_wins", func(t *testing.T) {
		// deltalog newer than covered position
		ts, known := inputDeleteCoverageTs(segWithCoverage(100, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 130, ts)
		// covered position newer than deltalogs
		ts, known = inputDeleteCoverageTs(segWithCoverage(200, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 200, ts)
	})

	t.Run("neither_present_is_unknown", func(t *testing.T) {
		_, known := inputDeleteCoverageTs(segWithCoverage(0))
		assert.False(t, known)
	})
}

func TestComputeDeleteCoveredPosition(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_v0"

	t.Run("min_coverage_across_inputs", func(t *testing.T) {
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{
			segWithCoverage(0, 300),
			segWithCoverage(150),
			segWithCoverage(0, 200),
		})
		if assert.NotNil(t, p) {
			// the least-covered input (150) bounds the output
			assert.EqualValues(t, 150, p.GetTimestamp())
			assert.Equal(t, channel, p.GetChannelName())
		}
	})

	t.Run("any_unknown_input_yields_nil", func(t *testing.T) {
		// a nil result makes the delegator fall back to start_position (minTs),
		// which is always safe (never loses a delete).
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{
			segWithCoverage(0, 300),
			segWithCoverage(0), // coverage unknown
		})
		assert.Nil(t, p)
	})

	t.Run("empty_inputs_yields_nil", func(t *testing.T) {
		assert.Nil(t, computeDeleteCoveredPosition(channel, nil))
	})

	t.Run("single_input", func(t *testing.T) {
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{segWithCoverage(0, 77)})
		if assert.NotNil(t, p) {
			assert.EqualValues(t, 77, p.GetTimestamp())
		}
	})
}

func TestClusteringDeleteCoveragePropagation(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_v0"

	segments := NewSegmentsInfo()
	segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                    1,
		CollectionID:          100,
		PartitionID:           10,
		InsertChannel:         channel,
		State:                 commonpb.SegmentState_Flushed,
		Level:                 datapb.SegmentLevel_L1,
		NumOfRows:             2,
		DeleteCoveredPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 150},
	}))
	segments.SetSegment(2, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: channel,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		NumOfRows:     2,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{TimestampTo: 200}},
		}},
	}))

	m := &meta{
		catalog:  &catalogkv.Catalog{MetaKv: NewMetaMemoryKV()},
		segments: segments,
	}

	clustered, _, err := m.CompleteCompactionMutation(context.Background(), &datapb.CompactionTask{
		InputSegments: []int64{1, 2},
		Type:          datapb.CompactionType_ClusteringCompaction,
		Channel:       channel,
	}, &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{
			{SegmentID: 3, NumOfRows: 2},
			{SegmentID: 4, NumOfRows: 2},
		},
	})
	require.NoError(t, err)
	require.Len(t, clustered, 2)
	for _, segment := range clustered {
		require.NotNil(t, segment.GetDeleteCoveredPosition())
		assert.EqualValues(t, 150, segment.GetDeleteCoveredPosition().GetTimestamp())
		assert.Equal(t, channel, segment.GetDeleteCoveredPosition().GetChannelName())
	}

	// Clustering outputs are normally processed by sort compaction before they
	// become visible. The coverage must survive that second compaction step.
	sorted, _, err := m.CompleteCompactionMutation(context.Background(), &datapb.CompactionTask{
		InputSegments: []int64{3},
		Type:          datapb.CompactionType_SortCompaction,
		Channel:       channel,
	}, &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{SegmentID: 5, NumOfRows: 2}},
	})
	require.NoError(t, err)
	require.Len(t, sorted, 1)
	require.NotNil(t, sorted[0].GetDeleteCoveredPosition())
	assert.EqualValues(t, 150, sorted[0].GetDeleteCoveredPosition().GetTimestamp())
	assert.Equal(t, channel, sorted[0].GetDeleteCoveredPosition().GetChannelName())
}

func TestClusteringDeleteCoverageUnknownInput(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_v0"

	segments := NewSegmentsInfo()
	segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                    1,
		CollectionID:          100,
		PartitionID:           10,
		InsertChannel:         channel,
		State:                 commonpb.SegmentState_Flushed,
		Level:                 datapb.SegmentLevel_L1,
		NumOfRows:             2,
		DeleteCoveredPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 150},
	}))
	segments.SetSegment(2, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: channel,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		NumOfRows:     2,
	}))

	m := &meta{
		catalog:  &catalogkv.Catalog{MetaKv: NewMetaMemoryKV()},
		segments: segments,
	}

	clustered, _, err := m.CompleteCompactionMutation(context.Background(), &datapb.CompactionTask{
		InputSegments: []int64{1, 2},
		Type:          datapb.CompactionType_ClusteringCompaction,
		Channel:       channel,
	}, &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{SegmentID: 3, NumOfRows: 4}},
	})
	require.NoError(t, err)
	require.Len(t, clustered, 1)
	assert.Nil(t, clustered[0].GetDeleteCoveredPosition())
}
