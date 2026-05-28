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

package compactor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// These tests mock the package-level helper buildTextIndexesForSegment so that
// they can exercise the mixcompaction inline text-index wiring without invoking
// the CGO-backed indexcgowrapper.CreateTextIndex path. Mockey is the only
// viable monkey-patching framework for an unexported package-level function.
func TestMixCompaction_createTextIndex_PopulatesStatsForEnableMatchFields(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const (
		collectionID  = int64(7001)
		partitionID   = int64(7002)
		segmentID     = int64(7003)
		planID        = int64(42)
		textFieldID   = int64(101)
		scalarFieldID = int64(102)
	)

	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  textFieldID,
				Name:     "text_field",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "256"},
					{Key: "enable_match", Value: "true"},
				},
			},
			{
				FieldID:  scalarFieldID,
				Name:     "scalar_field",
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	plan := &datapb.CompactionPlan{
		PlanID: planID,
		Schema: schema,
		Type:   datapb.CompactionType_MixCompaction,
	}

	task := &mixCompactionTask{
		plan:             plan,
		collectionID:     collectionID,
		partitionID:      partitionID,
		compactionParams: compaction.GenParams(),
	}

	mockBuild := mockey.Mock(buildTextIndexesForSegment).
		To(func(ctx context.Context, args buildTextIndexArgs) (map[int64]*datapb.TextIndexStats, error) {
			assert.Equal(t, collectionID, args.collectionID)
			assert.Equal(t, partitionID, args.partitionID)
			assert.Equal(t, segmentID, args.segmentID)
			assert.Equal(t, planID, args.taskID)
			assert.Equal(t, plan, args.plan)
			return map[int64]*datapb.TextIndexStats{
				textFieldID: {
					FieldID:    textFieldID,
					BuildID:    planID,
					Files:      []string{"meta.json_0"},
					LogSize:    1024,
					MemorySize: 1024,
				},
			}, nil
		}).Build()
	defer mockBuild.UnPatch()

	out := &datapb.CompactionSegment{SegmentID: segmentID, NumOfRows: 100}

	got, err := task.createTextIndex(context.Background(), out)
	assert.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Contains(t, got, textFieldID)
	assert.NotContains(t, got, scalarFieldID)
}

func TestMixCompaction_createTextIndex_PropagatesError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	task := &mixCompactionTask{
		plan:             &datapb.CompactionPlan{PlanID: 1, Schema: &schemapb.CollectionSchema{}},
		collectionID:     1,
		partitionID:      2,
		compactionParams: compaction.GenParams(),
	}

	mockBuild := mockey.Mock(buildTextIndexesForSegment).
		Return(nil, assert.AnError).Build()
	defer mockBuild.UnPatch()

	_, err := task.createTextIndex(context.Background(), &datapb.CompactionSegment{SegmentID: 99})
	assert.ErrorIs(t, err, assert.AnError)
}

func TestMixCompaction_inlineTextIndex_AssignsToOutputSegments(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	// Mix of sorted (build), empty (skip), and non-empty-but-unsorted
	// (skip - interim mergeSplit output).
	res := []*datapb.CompactionSegment{
		{SegmentID: 1, NumOfRows: 100, IsSorted: true},
		{SegmentID: 2, NumOfRows: 0, IsSorted: true},
		{SegmentID: 3, NumOfRows: 200, IsSorted: true},
		{SegmentID: 4, NumOfRows: 50},
	}

	statsBySeg := map[int64]map[int64]*datapb.TextIndexStats{
		1: {101: {FieldID: 101, BuildID: 99, Files: []string{"a"}, LogSize: 1, MemorySize: 1}},
		3: {101: {FieldID: 101, BuildID: 99, Files: []string{"b"}, LogSize: 2, MemorySize: 2}},
		4: {101: {FieldID: 101, BuildID: 99, Files: []string{"c"}, LogSize: 3, MemorySize: 3}},
	}

	mockBuild := mockey.Mock(buildTextIndexesForSegment).
		To(func(_ context.Context, args buildTextIndexArgs) (map[int64]*datapb.TextIndexStats, error) {
			return statsBySeg[args.segmentID], nil
		}).Build()
	defer mockBuild.UnPatch()

	task := &mixCompactionTask{
		plan: &datapb.CompactionPlan{
			PlanID: 99,
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}},
			}}},
		},
		collectionID:     1,
		partitionID:      2,
		compactionParams: compaction.GenParams(),
	}

	// Mirrors the guards in mixCompactionTask.Compact(): empty segments and
	// non-sorted outputs (interim mergeSplit results) must not get an inline
	// text index — they will be rebuilt by the next sortcompaction.
	for _, seg := range res {
		if seg.GetNumOfRows() == 0 {
			continue
		}
		if !seg.GetIsSorted() {
			continue
		}
		stats, err := task.createTextIndex(context.Background(), seg)
		assert.NoError(t, err)
		seg.TextStatsLogs = stats
	}

	assert.Len(t, res[0].GetTextStatsLogs(), 1)
	assert.Nil(t, res[1].GetTextStatsLogs(), "empty segments must be skipped")
	assert.Len(t, res[2].GetTextStatsLogs(), 1)
	assert.Nil(t, res[3].GetTextStatsLogs(), "unsorted segments must be skipped")
}

// Direct test of the shared helper for the no-enable_match early-return path —
// the helper iterates the schema, finds nothing to build, and returns an empty
// map without touching CGO.
func TestBuildTextIndexesForSegment_NoEnableMatchFields(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	args := buildTextIndexArgs{
		plan: &datapb.CompactionPlan{
			PlanID: 1,
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "varchar_no_match", DataType: schemapb.DataType_VarChar},
			}},
		},
		compactionParams: compaction.GenParams(),
		collectionID:     1,
		partitionID:      2,
		segmentID:        3,
		taskID:           1,
	}

	got, err := buildTextIndexesForSegment(context.Background(), args)
	assert.NoError(t, err)
	assert.Empty(t, got)
}
