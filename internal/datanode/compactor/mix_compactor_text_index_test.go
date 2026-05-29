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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// textMatchSchema returns a schema whose single VarChar field has enable_match.
func textMatchSchema(fieldID int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  fieldID,
				Name:     "text_field",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "256"},
					{Key: "enable_match", Value: "true"},
				},
			},
		},
	}
}

// capturedBuild holds the last BuildIndexInfo passed to the mocked CreateTextIndex.
type capturedBuild struct {
	info *indexcgopb.BuildIndexInfo
}

// mockCreateTextIndexCGO mocks indexcgowrapper.CreateTextIndex (the CGO leaf used
// by buildTextIndexesForSegment) so the helper body runs without the real
// tantivy/indexcgowrapper stack. It captures the last BuildIndexInfo for
// assertions and returns a cleanup func.
func mockCreateTextIndexCGO(uploaded map[string]int64) (*capturedBuild, func()) {
	cap := &capturedBuild{}
	m := mockey.Mock(indexcgowrapper.CreateTextIndex).To(
		func(ctx context.Context, info *indexcgopb.BuildIndexInfo) (map[string]int64, error) {
			cap.info = info
			return uploaded, nil
		}).Build()
	return cap, func() { m.UnPatch() }
}

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

// TestMixCompaction_Compact_InlineTextIndex drives the real mixCompactionTask.Compact()
// and asserts the inline text-index loop's behavior end to end: sorted outputs get
// TextStatsLogs while empty and unsorted interim outputs are skipped. mergeSplit,
// decompress and the CGO-backed build helper are mocked so the focus stays on the
// loop in Compact().
func TestMixCompaction_Compact_InlineTextIndex(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const fieldID = int64(101)

	mergeOut := []*datapb.CompactionSegment{
		{SegmentID: 1, NumOfRows: 100, IsSorted: true}, // build
		{SegmentID: 2, NumOfRows: 0, IsSorted: true},   // skip: empty
		{SegmentID: 3, NumOfRows: 200, IsSorted: true}, // build
		{SegmentID: 4, NumOfRows: 50},                  // skip: unsorted interim output
	}

	statsFor := func(segID int64) map[int64]*datapb.TextIndexStats {
		return map[int64]*datapb.TextIndexStats{
			fieldID: {FieldID: fieldID, BuildID: segID, Files: []string{"f"}, LogSize: 1, MemorySize: 1},
		}
	}

	buildMock := mockey.Mock(buildTextIndexesForSegment).
		To(func(_ context.Context, args buildTextIndexArgs) (map[int64]*datapb.TextIndexStats, error) {
			return statsFor(args.segmentID), nil
		}).Build()
	defer buildMock.UnPatch()

	mergeMock := mockey.Mock((*mixCompactionTask).mergeSplit).Return(mergeOut, nil).Build()
	defer mergeMock.UnPatch()
	decompressMock := mockey.Mock(binlog.DecompressCompactionBinlogsWithRootPath).Return(nil).Build()
	defer decompressMock.UnPatch()

	params := compaction.GenParams()
	params.UseMergeSort = false // force the mergeSplit path (which we mock)

	plan := &datapb.CompactionPlan{
		PlanID:  999,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: "ch-1",
		MaxSize: 64 * 1024 * 1024,
		Schema:  textMatchSchema(fieldID),
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				CollectionID: 1,
				PartitionID:  2,
				SegmentID:    100,
				FieldBinlogs: []*datapb.FieldBinlog{
					{FieldID: fieldID, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 100, MemorySize: 1024}}},
				},
			},
		},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 20000, End: 30000},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 30000, End: 40000},
	}

	task := NewMixCompactionTask(context.Background(), nil, plan, params, []int64{fieldID})

	result, err := task.Compact()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, datapb.CompactionTaskState_completed, result.GetState())

	byID := make(map[int64]*datapb.CompactionSegment)
	for _, s := range result.GetSegments() {
		byID[s.GetSegmentID()] = s
	}
	require.Len(t, byID, 4)
	assert.Len(t, byID[1].GetTextStatsLogs(), 1, "sorted output gets inline text index")
	assert.Nil(t, byID[2].GetTextStatsLogs(), "empty output skipped")
	assert.Len(t, byID[3].GetTextStatsLogs(), 1, "sorted output gets inline text index")
	assert.Nil(t, byID[4].GetTextStatsLogs(), "unsorted interim output skipped")
}

// TestMixCompaction_Compact_InlineTextIndex_BuildError ensures a failure from the
// inline build aborts Compact().
func TestMixCompaction_Compact_InlineTextIndex_BuildError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	mergeOut := []*datapb.CompactionSegment{{SegmentID: 1, NumOfRows: 100, IsSorted: true}}
	buildMock := mockey.Mock(buildTextIndexesForSegment).Return(nil, assert.AnError).Build()
	defer buildMock.UnPatch()
	mergeMock := mockey.Mock((*mixCompactionTask).mergeSplit).Return(mergeOut, nil).Build()
	defer mergeMock.UnPatch()
	decompressMock := mockey.Mock(binlog.DecompressCompactionBinlogsWithRootPath).Return(nil).Build()
	defer decompressMock.UnPatch()

	params := compaction.GenParams()
	params.UseMergeSort = false

	plan := &datapb.CompactionPlan{
		PlanID:  999,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: "ch-1",
		MaxSize: 64 * 1024 * 1024,
		Schema:  textMatchSchema(101),
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				CollectionID: 1, PartitionID: 2, SegmentID: 100,
				FieldBinlogs: []*datapb.FieldBinlog{
					{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 100, MemorySize: 1024}}},
				},
			},
		},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 20000, End: 30000},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 30000, End: 40000},
	}

	task := NewMixCompactionTask(context.Background(), nil, plan, params, []int64{101})
	_, err := task.Compact()
	assert.ErrorIs(t, err, assert.AnError)
}

// TestBuildTextIndexesForSegment_V1 exercises the real helper body for a storage-V1
// segment: insert files are resolved from binlogs, the index is built (CGO mocked)
// and TextIndexStats are aggregated.
func TestBuildTextIndexesForSegment_V1(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	captured, cleanup := mockCreateTextIndexCGO(map[string]int64{"a.idx": 111, "b.idx": 222})
	defer cleanup()

	args := buildTextIndexArgs{
		plan:             &datapb.CompactionPlan{PlanID: 5, CurrentScalarIndexVersion: 1, Schema: textMatchSchema(101)},
		compactionParams: compaction.GenParams(),
		collectionID:     1,
		partitionID:      2,
		segmentID:        3,
		taskID:           5,
		storageVersion:   storage.StorageV1,
		insertBinlogs:    []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 9}}}},
	}

	got, err := buildTextIndexesForSegment(context.Background(), args)
	require.NoError(t, err)
	require.Len(t, got, 1)
	ts := got[101]
	require.NotNil(t, ts)
	assert.Equal(t, int64(101), ts.GetFieldID())
	assert.Equal(t, int64(5), ts.GetBuildID())
	assert.Equal(t, int64(333), ts.GetLogSize())
	assert.Equal(t, int64(333), ts.GetMemorySize())
	assert.Len(t, ts.GetFiles(), 2)

	// V1: InsertFiles resolved from binlogs, SegmentInsertFiles not set.
	assert.NotEmpty(t, captured.info.GetInsertFiles())
	assert.Nil(t, captured.info.GetSegmentInsertFiles())
}

// TestBuildTextIndexesForSegment_V2 exercises the storage-V2 branch where insert
// files come from SegmentInsertFiles rather than per-field binlog paths.
func TestBuildTextIndexesForSegment_V2(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	captured, cleanup := mockCreateTextIndexCGO(map[string]int64{"a.idx": 10})
	defer cleanup()

	args := buildTextIndexArgs{
		plan:             &datapb.CompactionPlan{PlanID: 6, Schema: textMatchSchema(101)},
		compactionParams: compaction.GenParams(),
		collectionID:     1,
		partitionID:      2,
		segmentID:        3,
		taskID:           6,
		storageVersion:   storage.StorageV2,
		insertBinlogs:    []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 9}}}},
	}

	got, err := buildTextIndexesForSegment(context.Background(), args)
	require.NoError(t, err)
	require.Len(t, got, 1)
	// V2: getInsertFiles returns empty, SegmentInsertFiles populated instead.
	assert.Empty(t, captured.info.GetInsertFiles())
	assert.NotNil(t, captured.info.GetSegmentInsertFiles())
}

// TestBuildTextIndexesForSegment_CreateError ensures CGO build errors propagate.
func TestBuildTextIndexesForSegment_CreateError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	m := mockey.Mock(indexcgowrapper.CreateTextIndex).Return(nil, assert.AnError).Build()
	defer m.UnPatch()

	args := buildTextIndexArgs{
		plan:             &datapb.CompactionPlan{PlanID: 9, Schema: textMatchSchema(101)},
		compactionParams: compaction.GenParams(),
		collectionID:     1,
		partitionID:      2,
		segmentID:        3,
		taskID:           9,
		storageVersion:   storage.StorageV2,
		insertBinlogs:    []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 9}}}},
	}

	_, err := buildTextIndexesForSegment(context.Background(), args)
	assert.ErrorIs(t, err, assert.AnError)
}

// TestBuildTextIndexesForSegment_MissingBinlogError ensures a V1 segment whose
// enable_match field has no binlog produces an error.
func TestBuildTextIndexesForSegment_MissingBinlogError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	args := buildTextIndexArgs{
		plan:             &datapb.CompactionPlan{PlanID: 10, Schema: textMatchSchema(101)},
		compactionParams: compaction.GenParams(),
		collectionID:     1,
		partitionID:      2,
		segmentID:        3,
		taskID:           10,
		storageVersion:   storage.StorageV1,
		insertBinlogs:    nil, // no binlog for field 101
	}

	_, err := buildTextIndexesForSegment(context.Background(), args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field binlog not found")
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
