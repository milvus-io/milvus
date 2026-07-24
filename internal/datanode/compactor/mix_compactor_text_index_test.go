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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// TestMixCompaction_createTextIndex_Delegates verifies the mixCompactionTask
// wrapper forwards the task- and segment-derived identifiers to the shared
// package-level createTextIndex helper (which is CGO-backed and therefore mocked).
func TestMixCompaction_createTextIndex_Delegates(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const (
		collectionID = int64(7001)
		partitionID  = int64(7002)
		segmentID    = int64(7003)
		planID       = int64(42)
		storageVer   = int64(2)
		textFieldID  = int64(101)
	)
	requestContext := []*commonpb.KeyValuePair{{Key: "cipher-context", Value: "opaque"}}

	plan := &datapb.CompactionPlan{
		PlanID:        planID,
		Schema:        textMatchSchema(textFieldID),
		Type:          datapb.CompactionType_MixCompaction,
		PluginContext: requestContext,
	}
	task := &mixCompactionTask{
		plan:             plan,
		collectionID:     collectionID,
		partitionID:      partitionID,
		compactionParams: compaction.GenParams(),
	}

	mockHelper := mockey.Mock(createTextIndex).To(
		func(_ context.Context, _ storage.ChunkManager, gotPlan *datapb.CompactionPlan, _ compaction.Params,
			gotStorageVer, gotColl, gotPart, gotSeg, gotTask int64, _ *datapb.CompactionSegment,
		) (map[int64]*datapb.TextIndexStats, error) {
			assert.Equal(t, plan, gotPlan)
			assert.Equal(t, requestContext, gotPlan.GetPluginContext())
			assert.Equal(t, storageVer, gotStorageVer)
			assert.Equal(t, collectionID, gotColl)
			assert.Equal(t, partitionID, gotPart)
			assert.Equal(t, segmentID, gotSeg)
			assert.Equal(t, planID, gotTask)
			return map[int64]*datapb.TextIndexStats{
				textFieldID: {FieldID: textFieldID, BuildID: planID, Files: []string{"meta.json_0"}, LogSize: 1024, MemorySize: 1024},
			}, nil
		}).Build()
	defer mockHelper.UnPatch()

	out := &datapb.CompactionSegment{SegmentID: segmentID, NumOfRows: 100, StorageVersion: storageVer}
	got, err := task.createTextIndex(context.Background(), out)
	assert.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Contains(t, got, textFieldID)
}

func TestMixCompaction_createTextIndex_PropagatesError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	task := &mixCompactionTask{
		plan:             &datapb.CompactionPlan{PlanID: 1, Schema: &schemapb.CollectionSchema{}},
		collectionID:     1,
		partitionID:      2,
		compactionParams: compaction.GenParams(),
	}

	mockHelper := mockey.Mock(createTextIndex).Return(nil, assert.AnError).Build()
	defer mockHelper.UnPatch()

	_, err := task.createTextIndex(context.Background(), &datapb.CompactionSegment{SegmentID: 99})
	assert.ErrorIs(t, err, assert.AnError)
}

func TestCreateTextIndexPropagatesPluginContext(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const (
		collectionID = int64(7001)
		partitionID  = int64(7002)
		segmentID    = int64(7003)
		fieldID      = int64(101)
	)
	requestContext := []*commonpb.KeyValuePair{{Key: "cipher-context", Value: "opaque"}}
	pluginContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 17,
		CollectionId:     collectionID,
		EncryptionKey:    "unsafe-key",
	}

	parseMock := mockey.Mock(hookutil.GetRequiredCPluginContext).To(
		func(got []*commonpb.KeyValuePair, gotCollectionID int64) (*indexcgopb.StoragePluginContext, error) {
			require.Equal(t, requestContext, got)
			require.Equal(t, collectionID, gotCollectionID)
			return pluginContext, nil
		}).Build()
	defer parseMock.UnPatch()

	var captured *indexcgopb.BuildIndexInfo
	buildMock := mockey.Mock(indexcgowrapper.CreateIndex).To(
		func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
			captured = info
			return fakeTextIndex{}, nil
		}).Build()
	defer buildMock.UnPatch()

	manifest := packed.MarshalManifestPath(t.TempDir(), packed.ManifestEarliest)
	segment := &datapb.CompactionSegment{
		SegmentID:      segmentID,
		StorageVersion: storage.StorageV3,
		Manifest:       manifest,
	}
	params := compaction.GenParams()
	params.StorageConfig = &indexpb.StorageConfig{RootPath: t.TempDir(), StorageType: "local"}
	got, err := createTextIndex(
		context.Background(),
		nil,
		&datapb.CompactionPlan{
			PlanID:        1,
			Schema:        textMatchSchema(fieldID),
			PluginContext: requestContext,
		},
		params,
		storage.StorageV3,
		collectionID,
		partitionID,
		segmentID,
		1,
		segment,
	)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	require.NotNil(t, captured)
	require.Equal(t, pluginContext, captured.GetStoragePluginContext())
	require.Equal(t, manifest, captured.GetManifest())
	require.EqualValues(t, storage.StorageV3, captured.GetStorageVersion())
}

func TestCreateTextIndexRejectsPluginContextBeforeBuild(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	hookutil.InitTestCipher()

	buildCalled := false
	buildMock := mockey.Mock(indexcgowrapper.CreateIndex).To(
		func(context.Context, *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
			buildCalled = true
			return fakeTextIndex{}, nil
		}).Build()
	defer buildMock.UnPatch()

	params := compaction.GenParams()
	params.StorageConfig = &indexpb.StorageConfig{RootPath: t.TempDir(), StorageType: "local"}
	const secret = "never-echo-this-key"
	_, err := createTextIndex(
		context.Background(),
		nil,
		&datapb.CompactionPlan{
			PlanID:        1,
			Schema:        textMatchSchema(101),
			PluginContext: []*commonpb.KeyValuePair{{Key: hookutil.CipherConfigUnsafeEZK, Value: secret}},
		},
		params,
		storage.StorageV2,
		1,
		2,
		3,
		1,
		&datapb.CompactionSegment{SegmentID: 3, StorageVersion: storage.StorageV2},
	)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.NotErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Code(err))
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	require.NotContains(t, err.Error(), secret)
	require.False(t, buildCalled)
}

func TestCreateTextIndexWithoutPluginContext(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	parseMock := mockey.Mock(hookutil.GetRequiredCPluginContext).Return(nil, nil).Build()
	defer parseMock.UnPatch()

	var captured *indexcgopb.BuildIndexInfo
	buildMock := mockey.Mock(indexcgowrapper.CreateIndex).To(
		func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
			captured = info
			return fakeTextIndex{}, nil
		}).Build()
	defer buildMock.UnPatch()

	params := compaction.GenParams()
	params.StorageConfig = &indexpb.StorageConfig{RootPath: t.TempDir(), StorageType: "local"}
	_, err := createTextIndex(
		context.Background(),
		nil,
		&datapb.CompactionPlan{PlanID: 1, Schema: textMatchSchema(101)},
		params,
		storage.StorageV2,
		1,
		2,
		3,
		1,
		&datapb.CompactionSegment{SegmentID: 3, StorageVersion: storage.StorageV2},
	)
	require.NoError(t, err)
	require.NotNil(t, captured)
	require.Nil(t, captured.GetStoragePluginContext())
}

func TestSortCompaction_createTextIndex_ForwardsPlan(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	requestContext := []*commonpb.KeyValuePair{{Key: "cipher-context", Value: "opaque"}}
	plan := &datapb.CompactionPlan{PlanID: 42, PluginContext: requestContext}
	task := &sortCompactionTask{
		plan:             plan,
		compactionParams: compaction.GenParams(),
		storageVersion:   storage.StorageV3,
	}
	segment := &datapb.CompactionSegment{SegmentID: 3, StorageVersion: storage.StorageV3}

	mockHelper := mockey.Mock(createTextIndex).To(
		func(_ context.Context, _ storage.ChunkManager, gotPlan *datapb.CompactionPlan, _ compaction.Params,
			gotStorageVersion, gotCollectionID, gotPartitionID, gotSegmentID, gotTaskID int64,
			gotSegment *datapb.CompactionSegment,
		) (map[int64]*datapb.TextIndexStats, error) {
			require.Equal(t, plan, gotPlan)
			require.Equal(t, requestContext, gotPlan.GetPluginContext())
			require.EqualValues(t, storage.StorageV3, gotStorageVersion)
			require.EqualValues(t, 1, gotCollectionID)
			require.EqualValues(t, 2, gotPartitionID)
			require.EqualValues(t, 3, gotSegmentID)
			require.EqualValues(t, 42, gotTaskID)
			require.Equal(t, segment, gotSegment)
			return map[int64]*datapb.TextIndexStats{}, nil
		}).Build()
	defer mockHelper.UnPatch()

	_, err := task.createTextIndex(context.Background(), 1, 2, 3, 42, segment)
	require.NoError(t, err)
}

// TestMixCompaction_Compact_InlineTextIndex drives the real mixCompactionTask.Compact()
// and asserts the inline text-index loop's behavior end to end: sorted outputs get
// TextStatsLogs, the V3-manifest segment gets its manifest rewritten, while empty and
// unsorted outputs are skipped. mergeSplit/applyLOBCompaction/decompress, the per-task
// createTextIndex wrapper, and the packed manifest helpers are mocked so the focus
// stays on the loop in Compact().
func TestMixCompaction_Compact_InlineTextIndex(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const fieldID = int64(101)

	mergeOut := []*datapb.CompactionSegment{
		{SegmentID: 1, NumOfRows: 100, IsSorted: true},                            // build
		{SegmentID: 2, NumOfRows: 0, IsSorted: true},                              // skip: empty
		{SegmentID: 3, NumOfRows: 200, IsSortedByNamespace: true},                 // build
		{SegmentID: 4, NumOfRows: 50},                                             // skip: unsorted interim output
		{SegmentID: 5, NumOfRows: 300, IsSorted: true, Manifest: "orig-manifest"}, // build + V3 manifest
	}

	statsFor := func(segID int64) map[int64]*datapb.TextIndexStats {
		return map[int64]*datapb.TextIndexStats{
			fieldID: {FieldID: fieldID, BuildID: segID, Files: []string{"f"}, LogSize: 1, MemorySize: 1},
		}
	}

	wrapperMock := mockey.Mock((*mixCompactionTask).createTextIndex).
		To(func(_ *mixCompactionTask, _ context.Context, segment *datapb.CompactionSegment) (map[int64]*datapb.TextIndexStats, error) {
			return statsFor(segment.GetSegmentID()), nil
		}).Build()
	defer wrapperMock.UnPatch()

	mergeMock := mockey.Mock((*mixCompactionTask).mergeSplit).Return(mergeOut, nil).Build()
	defer mergeMock.UnPatch()
	lobMock := mockey.Mock((*mixCompactionTask).applyLOBCompaction).Return(nil).Build()
	defer lobMock.UnPatch()
	decompressMock := mockey.Mock(binlog.DecompressCompactionBinlogsWithRootPath).Return(nil).Build()
	defer decompressMock.UnPatch()

	entriesMock := mockey.Mock(packed.TextIndexStatEntries).
		Return([]packed.StatEntry{{Key: "text_index.101"}}).Build()
	defer entriesMock.UnPatch()
	addManifestCalls := 0
	addMock := mockey.Mock(packed.AddStatsToManifest).
		To(func(manifestPath string, _ *indexpb.StorageConfig, _ []packed.StatEntry) (string, error) {
			addManifestCalls++
			assert.Equal(t, "orig-manifest", manifestPath)
			return "new-manifest", nil
		}).Build()
	defer addMock.UnPatch()

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

	task := NewMixCompactionTask(context.Background(), nil, nil, plan, params, []int64{fieldID})

	result, err := task.Compact()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, datapb.CompactionTaskState_completed, result.GetState())

	byID := make(map[int64]*datapb.CompactionSegment)
	for _, s := range result.GetSegments() {
		byID[s.GetSegmentID()] = s
	}
	require.Len(t, byID, 5)
	assert.Len(t, byID[1].GetTextStatsLogs(), 1, "sorted output gets inline text index")
	assert.Nil(t, byID[2].GetTextStatsLogs(), "empty output skipped")
	assert.Len(t, byID[3].GetTextStatsLogs(), 1, "sorted-by-namespace output gets inline text index")
	assert.Nil(t, byID[4].GetTextStatsLogs(), "unsorted interim output skipped")
	assert.Len(t, byID[5].GetTextStatsLogs(), 1, "V3 sorted output gets inline text index")
	assert.Equal(t, "new-manifest", byID[5].GetManifest(), "V3 manifest rewritten")
	assert.Equal(t, 1, addManifestCalls, "AddStatsToManifest only for the manifest-bearing sorted output")
}

// TestMixCompaction_Compact_InlineTextIndex_BuildError ensures a failure from the
// inline build aborts Compact().
func TestMixCompaction_Compact_InlineTextIndex_BuildError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	mergeOut := []*datapb.CompactionSegment{{SegmentID: 1, NumOfRows: 100, IsSorted: true}}
	wrapperMock := mockey.Mock((*mixCompactionTask).createTextIndex).Return(nil, assert.AnError).Build()
	defer wrapperMock.UnPatch()
	mergeMock := mockey.Mock((*mixCompactionTask).mergeSplit).Return(mergeOut, nil).Build()
	defer mergeMock.UnPatch()
	lobMock := mockey.Mock((*mixCompactionTask).applyLOBCompaction).Return(nil).Build()
	defer lobMock.UnPatch()
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

	task := NewMixCompactionTask(context.Background(), nil, nil, plan, params, []int64{101})
	_, err := task.Compact()
	assert.ErrorIs(t, err, assert.AnError)
}

// TestMixCompaction_Compact_InlineTextIndex_ManifestError ensures a failure from
// packed.AddStatsToManifest (the V3 manifest-update branch) aborts Compact().
func TestMixCompaction_Compact_InlineTextIndex_ManifestError(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	const fieldID = int64(101)
	mergeOut := []*datapb.CompactionSegment{
		{SegmentID: 1, NumOfRows: 100, IsSorted: true, Manifest: "orig-manifest"},
	}

	wrapperMock := mockey.Mock((*mixCompactionTask).createTextIndex).
		To(func(_ *mixCompactionTask, _ context.Context, segment *datapb.CompactionSegment) (map[int64]*datapb.TextIndexStats, error) {
			return map[int64]*datapb.TextIndexStats{
				fieldID: {FieldID: fieldID, BuildID: segment.GetSegmentID(), Files: []string{"f"}, LogSize: 1, MemorySize: 1},
			}, nil
		}).Build()
	defer wrapperMock.UnPatch()
	mergeMock := mockey.Mock((*mixCompactionTask).mergeSplit).Return(mergeOut, nil).Build()
	defer mergeMock.UnPatch()
	lobMock := mockey.Mock((*mixCompactionTask).applyLOBCompaction).Return(nil).Build()
	defer lobMock.UnPatch()
	decompressMock := mockey.Mock(binlog.DecompressCompactionBinlogsWithRootPath).Return(nil).Build()
	defer decompressMock.UnPatch()
	entriesMock := mockey.Mock(packed.TextIndexStatEntries).
		Return([]packed.StatEntry{{Key: "text_index.101"}}).Build()
	defer entriesMock.UnPatch()
	addMock := mockey.Mock(packed.AddStatsToManifest).Return("", assert.AnError).Build()
	defer addMock.UnPatch()

	params := compaction.GenParams()
	params.UseMergeSort = false

	plan := &datapb.CompactionPlan{
		PlanID:  999,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: "ch-1",
		MaxSize: 64 * 1024 * 1024,
		Schema:  textMatchSchema(fieldID),
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				CollectionID: 1, PartitionID: 2, SegmentID: 100,
				FieldBinlogs: []*datapb.FieldBinlog{
					{FieldID: fieldID, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 100, MemorySize: 1024}}},
				},
			},
		},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 20000, End: 30000},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 30000, End: 40000},
	}

	task := NewMixCompactionTask(context.Background(), nil, nil, plan, params, []int64{fieldID})
	_, err := task.Compact()
	assert.ErrorIs(t, err, assert.AnError)
}
