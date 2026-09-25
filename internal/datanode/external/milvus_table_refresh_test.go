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

package external

import (
	"context"
	"io"
	"path"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableL0RefreshUpdatesExistingSegmentManifest() {
	ctx := context.Background()
	partitionID := int64(2000)
	oldManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)
	carriedManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 2)
	carriedTextStats := map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, Version: 2, BuildID: 20},
	}
	carriedJSONStats := map[int64]*datapb.JsonKeyStats{
		102: {FieldID: 102, Version: 2, BuildID: 21, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion},
	}
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   partitionID,
		TaskID:        s.taskID,
		ExternalSpec:  `{"format":"milvus-table"}`,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "json", DataType: schemapb.DataType_JSON},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             1,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      1000,
			ManifestPath:   oldManifest,
			StorageVersion: storage.StorageV3,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {FieldID: 101, Version: 1, BuildID: 10},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {FieldID: 102, Version: 1, BuildID: 11, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion},
			},
		}},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.nextAllocID = req.GetPreAllocatedSegmentIds().GetBegin()
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "source-manifest", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{{
		FragmentID: 201,
		FilePath:   "source-manifest",
		StartRow:   0,
		EndRow:     1000,
		RowCount:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      88,
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 99,
			}},
		}},
	}}

	var gotFragments []packed.Fragment
	mockRefresh := mockey.Mock(packed.RefreshMilvusTableManifest).
		To(func(_ context.Context, oldPath string, columns []string, fragments []packed.Fragment, cfg *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext, outputColumns []string, deltas []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.Equal(oldManifest, oldPath)
			s.Same(req.GetStorageConfig(), cfg)
			s.Empty(outputColumns)
			s.Equal([]packed.DeltaLogEntry{{Path: newFragments[0].Deltalogs[0].Binlogs[0].LogPath, NumEntries: 99}}, deltas)
			gotFragments = fragments
			return packed.MilvusTableRefreshResult{
				ManifestPath: carriedManifest, TextStatsLogs: carriedTextStats, JSONKeyStats: carriedJSONStats,
			}, nil
		}).Build()
	defer mockRefresh.UnPatch()
	mockSourceDeltas := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return(nil, nil).Build()
	defer mockSourceDeltas.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.Empty(task.GetKeptSegmentIDs())
	updated := task.GetUpdatedSegments()
	s.Require().Len(updated, 1)
	s.Equal(int64(1), updated[0].GetID())
	s.Equal(carriedManifest, updated[0].GetManifestPath())
	s.Equal(carriedTextStats, updated[0].GetTextStatsLogs())
	s.Equal(carriedJSONStats, updated[0].GetJsonKeyStats())
	s.Equal(updated, result)
	s.Equal(newFragments, gotFragments)
}

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableSegmentManifestCarriesFunctionArtifacts() {
	ctx := context.Background()
	basePath := "files/insert_log/1000/2000/1"
	oldManifest := packed.MarshalManifestPath(basePath, 1)
	finalManifest := packed.MarshalManifestPath(basePath, 2)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   2000,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 99, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
				{FieldID: 102, Name: "json", DataType: schemapb.DataType_JSON, ExternalField: "json"},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Type:           schemapb.FunctionType_BM25,
					InputFieldIds:  []int64{100},
					OutputFieldIds: []int64{101},
				},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	mockRefresh := mockey.Mock(packed.RefreshMilvusTableManifest).
		To(func(_ context.Context, oldPath string, columns []string, fragments []packed.Fragment, cfg *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext, outputColumns []string, deltas []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.Equal(oldManifest, oldPath)
			s.Same(req.GetStorageConfig(), cfg)
			s.Equal([]string{"101"}, outputColumns)
			s.Len(fragments, 1)
			s.Empty(deltas)
			return packed.MilvusTableRefreshResult{
				ManifestPath:  finalManifest,
				TextStatsLogs: map[int64]*datapb.TextIndexStats{100: {FieldID: 100, Version: 2, BuildID: 20}},
				JSONKeyStats:  map[int64]*datapb.JsonKeyStats{102: {FieldID: 102, Version: 2, BuildID: 21, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}},
			}, nil
		}).Build()
	defer mockRefresh.UnPatch()

	seg := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   s.collectionID,
		PartitionID:    2000,
		ManifestPath:   oldManifest,
		StorageVersion: storage.StorageV3,
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			100: {FieldID: 100, Version: 1, BuildID: 10},
		},
		JsonKeyStats: map[int64]*datapb.JsonKeyStats{
			102: {FieldID: 102, Version: 1, BuildID: 11, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion},
		},
	}
	updated, err := task.refreshMilvusTableSegmentManifest(ctx, seg, []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		RowCount: 1000,
	}}, []string{"101"})
	s.NoError(err)
	s.Equal(seg.GetID(), updated.GetID())
	s.Equal(finalManifest, updated.GetManifestPath())
	s.Equal(storage.StorageV3, updated.GetStorageVersion())
	s.Equal(int64(2), updated.GetTextStatsLogs()[100].GetVersion())
	s.Equal(int64(2), updated.GetJsonKeyStats()[102].GetVersion())
	s.Equal(oldManifest, seg.GetManifestPath(), "source SegmentInfo must remain immutable")
}

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableManifestReusesFilesInOneCommit() {
	paramtable.Init()
	storageConfig := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
	basePath := path.Join(storageConfig.GetRootPath(), "carry_milvus_table_artifacts/segment-1")
	oldDeltaPath := path.Join(basePath, "_delta/old")
	newDeltaPath := path.Join(basePath, "_delta/new")
	newerDeltaPath := path.Join(basePath, "_delta/newer")
	oldTextStatsPath := path.Join(basePath, "_stats/text_index.100/old")
	newTextStatsPath := path.Join(basePath, "_stats/text_index.100/new")
	jsonStatsPath := path.Join(basePath, "_stats/json_stats.102/old")
	for filePath, content := range map[string]string{
		oldDeltaPath:     "old-delta",
		newDeltaPath:     "new-delta",
		newerDeltaPath:   "newer-delta",
		oldTextStatsPath: "old-text-stats",
		newTextStatsPath: "new-text-stats",
		jsonStatsPath:    "json-stats",
	} {
		s.Require().NoError(packed.WriteFile(storageConfig, filePath, []byte(content)))
	}

	// The source manifest represents the segment before the refresh. Its
	// Function output and stats are still valid, but its deltalog is stale.
	outputSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "101",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"101"}),
	}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()
	writer, err := packed.NewFFIPackedWriter(
		basePath,
		outputSchema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
		storageConfig,
		nil,
	)
	s.Require().NoError(err)
	writer.AsNewColumnGroups()
	s.Require().NoError(writer.WriteRecordBatch(record))
	output, err := writer.Close()
	s.Require().NoError(err)
	defer output.Destroy()
	sourceManifest, err := packed.CommitManifestUpdates(basePath, packed.ManifestEarliest, storageConfig, &packed.ManifestUpdates{
		NewFiles: output,
	})
	s.Require().NoError(err)
	sourceManifest, err = packed.AddStatsToManifest(sourceManifest, storageConfig, []packed.StatEntry{
		{Key: "text_index.100", Files: []string{oldTextStatsPath}, Metadata: map[string]string{
			"build": "old", "version": "1", "build_id": "10", "log_size": "100", "memory_size": "200",
		}},
		{Key: "json_stats.102", Files: []string{jsonStatsPath}, Metadata: map[string]string{
			"build": "old", "version": "1", "build_id": "11", "log_size": "101", "memory_size": "201",
			"json_key_stats_data_format": "1",
		}},
	})
	s.Require().NoError(err)
	sourceManifest, err = packed.AddDeltaLogsToManifestOverwrite(sourceManifest, storageConfig, []packed.DeltaLogEntry{
		{Path: oldDeltaPath, NumEntries: 1},
	})
	s.Require().NoError(err)

	// Source text stats must not replace target-local stats. Only source bloom
	// filters are imported; a duplicate bloom key must use the fresh source entry.
	sourceManifest, err = packed.AddStatsToManifest(sourceManifest, storageConfig, []packed.StatEntry{
		{Key: "bloom_filter.99", Files: []string{oldTextStatsPath}, Metadata: map[string]string{"build": "old"}},
	})
	s.Require().NoError(err)
	snapshotSchema := arrow.NewSchema([]arrow.Field{{
		Name: "text", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"}),
	}}, nil)
	snapshotRecord := array.NewRecord(snapshotSchema, []arrow.Array{record.Column(0)}, 1)
	defer snapshotRecord.Release()
	snapshotBase := path.Join(storageConfig.RootPath, "source")
	snapshotWriter, err := packed.NewFFIPackedWriter(snapshotBase, snapshotSchema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}}, storageConfig, nil)
	s.Require().NoError(err)
	s.Require().NoError(snapshotWriter.WriteRecordBatch(snapshotRecord))
	snapshotOutput, err := snapshotWriter.Close()
	s.Require().NoError(err)
	defer snapshotOutput.Destroy()
	snapshotManifest, err := packed.CommitManifestUpdates(snapshotBase, packed.ManifestEarliest, storageConfig,
		&packed.ManifestUpdates{NewFiles: snapshotOutput})
	s.Require().NoError(err)
	snapshotManifest, err = packed.AddStatsToManifest(snapshotManifest, storageConfig, []packed.StatEntry{
		{Key: "bloom_filter.99", Files: []string{newTextStatsPath}, Metadata: map[string]string{"build": "new"}},
		{Key: "text_index.100", Files: []string{newTextStatsPath}, Metadata: map[string]string{"version": "2", "build_id": "20"}},
	})
	s.Require().NoError(err)
	fragments := []packed.Fragment{{FilePath: snapshotManifest, EndRow: 1, RowCount: 1}}
	_, oldVersion, err := packed.UnmarshalManifestPath(sourceManifest)
	s.Require().NoError(err)
	carryResult, err := packed.RefreshMilvusTableManifest(context.Background(), sourceManifest,
		[]string{"text"}, fragments, storageConfig, packed.ExternalSpecContext{},
		[]string{"101"}, []packed.DeltaLogEntry{{Path: newDeltaPath, NumEntries: 2}})
	s.Require().NoError(err)
	finalManifest := carryResult.ManifestPath
	_, finalVersion, err := packed.UnmarshalManifestPath(finalManifest)
	s.Require().NoError(err)
	s.Equal(oldVersion+1, finalVersion, "refresh must commit exactly one manifest version")
	s.Require().Contains(carryResult.TextStatsLogs, int64(100))
	s.Equal(int64(1), carryResult.TextStatsLogs[100].GetVersion())
	s.Equal(int64(10), carryResult.TextStatsLogs[100].GetBuildID())
	s.Require().Contains(carryResult.JSONKeyStats, int64(102))
	s.Equal(int64(1), carryResult.JSONKeyStats[102].GetVersion())
	s.Equal(int64(11), carryResult.JSONKeyStats[102].GetBuildID())
	s.Equal(int64(1), carryResult.JSONKeyStats[102].GetJsonKeyStatsDataFormat())

	hasFunctionOutput, err := packed.ManifestHasColumns(finalManifest, storageConfig, []string{"101"})
	s.Require().NoError(err)
	s.True(hasFunctionOutput)
	sourceOutput, err := packed.ReadFragmentsFromManifest(sourceManifest, storageConfig, []string{"101"})
	s.Require().NoError(err)
	finalOutput, err := packed.ReadFragmentsFromManifest(finalManifest, storageConfig, []string{"101"})
	s.Require().NoError(err)
	s.Equal(sourceOutput, finalOutput)
	reader, err := packed.NewFFIPackedReader(
		finalManifest,
		outputSchema,
		[]string{"101"},
		8192,
		storageConfig,
		nil,
		packed.ExternalReaderContext{},
	)
	s.Require().NoError(err, "carried Function output must retain packed-writer file properties")
	defer reader.Close()
	readRecord, err := reader.ReadNext()
	s.Require().NoError(err)
	s.Equal(int64(42), readRecord.Column(0).(*array.Int64).Value(0))
	readRecord.Release()
	_, err = reader.ReadNext()
	s.ErrorIs(err, io.EOF)

	stats, err := packed.GetManifestStats(finalManifest, storageConfig)
	s.Require().NoError(err)
	s.Equal("old", stats["text_index.100"].Metadata["build"])
	s.Equal("new", stats["bloom_filter.99"].Metadata["build"])
	s.Equal("old", stats["json_stats.102"].Metadata["build"])
	deltaPaths, err := packed.GetDeltaLogPathsFromManifest(finalManifest, storageConfig)
	s.Require().NoError(err)
	s.Require().Len(deltaPaths, 1)
	s.Contains(deltaPaths[0], "_delta/new")
	s.NotContains(deltaPaths[0], "_delta/old")

	// With no Function output columns, retain only stats alongside the refreshed
	// external columns. The old Function output group must not be registered.
	statsOnlyResult, err := packed.RefreshMilvusTableManifest(context.Background(), sourceManifest,
		[]string{"text"}, fragments, storageConfig, packed.ExternalSpecContext{},
		nil, []packed.DeltaLogEntry{{Path: newerDeltaPath, NumEntries: 3}})
	s.Require().NoError(err)
	statsOnlyManifest := statsOnlyResult.ManifestPath
	s.Equal(int64(1), statsOnlyResult.TextStatsLogs[100].GetVersion())
	s.Equal(int64(1), statsOnlyResult.JSONKeyStats[102].GetVersion())
	hasFunctionOutput, err = packed.ManifestHasColumns(statsOnlyManifest, storageConfig, []string{"101"})
	s.Require().NoError(err)
	s.False(hasFunctionOutput)
	stats, err = packed.GetManifestStats(statsOnlyManifest, storageConfig)
	s.Require().NoError(err)
	s.Contains(stats, "text_index.100")
	s.Contains(stats, "json_stats.102")
	deltaPaths, err = packed.GetDeltaLogPathsFromManifest(statsOnlyManifest, storageConfig)
	s.Require().NoError(err)
	s.Require().Len(deltaPaths, 1)
	s.Contains(deltaPaths[0], "_delta/newer")
	s.NotContains(deltaPaths[0], "_delta/old")

	_, err = packed.RefreshMilvusTableManifest(context.Background(), sourceManifest,
		[]string{"text"}, fragments, storageConfig, packed.ExternalSpecContext{}, []string{"999"}, nil)
	s.ErrorIs(err, merr.ErrDataIntegrity)
}

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableSegmentManifestErrors() {
	for _, stage := range []string{"manifest path", "fragments", "deltas", "commit", "canceled after commit"} {
		s.Run(stage, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
				}},
			})
			oldManifest := packed.MarshalManifestPath("files/segment", 1)
			if stage == "manifest path" {
				oldManifest = "invalid"
			}
			seg := &datapb.SegmentInfo{ID: 1, ManifestPath: oldManifest}
			failure := merr.WrapErrStorageMsg("injected %s failure", stage)
			prepare := mockey.Mock((*RefreshExternalCollectionTask).prepareMilvusTableDeltalogFragments).
				To(func(_ *RefreshExternalCollectionTask, fragments []packed.Fragment) ([]packed.Fragment, error) {
					if stage == "fragments" {
						return nil, failure
					}
					return fragments, nil
				}).Build()
			defer prepare.UnPatch()
			deltas := mockey.Mock((*RefreshExternalCollectionTask).prepareMilvusTableL0Deltalogs).
				To(func(_ *RefreshExternalCollectionTask, _ context.Context, _ []packed.Fragment) ([]packed.DeltaLogEntry, error) {
					if stage == "deltas" {
						return nil, failure
					}
					return nil, nil
				}).Build()
			defer deltas.UnPatch()
			commits := 0
			commit := mockey.Mock(packed.RefreshMilvusTableManifest).
				To(func(_ context.Context, _ string, _ []string, _ []packed.Fragment, _ *indexpb.StorageConfig,
					_ packed.ExternalSpecContext, _ []string, _ []packed.DeltaLogEntry,
				) (packed.MilvusTableRefreshResult, error) {
					commits++
					if stage == "commit" {
						return packed.MilvusTableRefreshResult{}, failure
					}
					cancel()
					return packed.MilvusTableRefreshResult{ManifestPath: packed.MarshalManifestPath("files/segment", 2)}, nil
				}).Build()
			defer commit.UnPatch()

			updated, err := task.refreshMilvusTableSegmentManifest(ctx, seg, nil, nil)
			s.Nil(updated)
			switch stage {
			case "manifest path":
				s.ErrorIs(err, merr.ErrDataIntegrity)
			case "canceled after commit":
				s.ErrorIs(err, context.Canceled)
			default:
				s.ErrorIs(err, failure)
			}
			s.Equal(oldManifest, seg.GetManifestPath())
			if stage == "commit" || stage == "canceled after commit" {
				s.Equal(1, commits)
			} else {
				s.Zero(commits, "preparation failure must not reach commit")
			}
		})
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableSegmentManifestPreparesVirtualPKDeletes() {
	ctx := context.Background()
	basePath := path.Join(s.T().TempDir(), "legacy/segment")
	oldManifest := packed.MarshalManifestPath(basePath, 1)
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		StorageConfig: &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()},
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
	})
	task.columns = []string{"text"}
	source := packed.MarshalManifestPath("source/segment", 1)
	mockSource := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return([]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
			{LogPath: "s3://bucket/source/_delta/88", EntriesNum: 2},
		}}}, nil).Build()
	defer mockSource.UnPatch()
	prepared := false
	targetDelta := packed.DeltaLogEntry{Path: path.Join(basePath, "_delta/88"), NumEntries: 2}
	mockTranslate := mockey.Mock((*RefreshExternalCollectionTask).prepareMilvusTableVirtualPKDeltalogs).
		To(func(_ *RefreshExternalCollectionTask, _ context.Context, base string, segmentID int64, fragments []packed.Fragment) ([]packed.DeltaLogEntry, error) {
			s.Equal(basePath, base, "keep the old segment namespace, including legacy paths")
			s.Equal(int64(1), segmentID)
			s.Require().Len(fragments, 1)
			s.Require().Len(fragments[0].Deltalogs, 1)
			s.Equal(int64(88), fragments[0].Deltalogs[0].Binlogs[0].LogID)
			prepared = true
			return []packed.DeltaLogEntry{targetDelta}, nil
		}).Build()
	defer mockTranslate.UnPatch()
	mockCommit := mockey.Mock(packed.RefreshMilvusTableManifest).
		To(func(_ context.Context, old string, columns []string, _ []packed.Fragment, _ *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext, _ []string, deltas []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.True(prepared, "translation must finish before the manifest transaction")
			s.Equal(packed.MilvusTablePrimaryKeyModeVirtual, extfs.MilvusTablePKMode)
			s.Equal(oldManifest, old)
			s.Equal(task.columns, columns)
			s.Equal([]packed.DeltaLogEntry{targetDelta}, deltas)
			return packed.MilvusTableRefreshResult{ManifestPath: packed.MarshalManifestPath(basePath, 2)}, nil
		}).Build()
	defer mockCommit.UnPatch()
	updated, err := task.refreshMilvusTableSegmentManifest(ctx,
		&datapb.SegmentInfo{ID: 1, ManifestPath: oldManifest},
		[]packed.Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, nil)
	s.Require().NoError(err)
	s.Equal(packed.MarshalManifestPath(basePath, 2), updated.ManifestPath)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableL0RefreshAlsoPatchesMissingColumns() {
	ctx := context.Background()
	partitionID := int64(2000)
	oldManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)
	refreshedManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 2)
	finalManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 3)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   partitionID,
		TaskID:        s.taskID,
		ExternalSpec:  `{"format":"milvus-table"}`,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Version: 7,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec"},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             1,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      1000,
			ManifestPath:   oldManifest,
			StorageVersion: storage.StorageV3,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100},
				Binlogs: []*datapb.Binlog{{
					LogID:      7,
					EntriesNum: 1000,
				}},
			}},
		}},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.nextAllocID = req.GetPreAllocatedSegmentIds().GetBegin()
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "source-manifest", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{{
		FragmentID: 201,
		FilePath:   "source-manifest",
		StartRow:   0,
		EndRow:     1000,
		RowCount:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      88,
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 99,
			}},
		}},
	}}

	mockRefresh := mockey.Mock(packed.RefreshMilvusTableManifest).
		To(func(_ context.Context, old string, _ []string, _ []packed.Fragment, _ *indexpb.StorageConfig,
			_ packed.ExternalSpecContext, outputColumns []string, _ []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.Equal(oldManifest, old)
			s.Empty(outputColumns)
			return packed.MilvusTableRefreshResult{ManifestPath: refreshedManifest}, nil
		}).Build()
	defer mockRefresh.UnPatch()
	mockSourceDeltas := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return(nil, nil).Build()
	defer mockSourceDeltas.UnPatch()

	var patchedBaseManifest string
	var patchedFragments []packed.Fragment
	var patchedColumns []string
	mockPatch := mockey.Mock(mockey.GetMethod(task, "patchSegmentForMissingColumns")).
		To(func(ctx context.Context, seg *datapb.SegmentInfo, fragments []packed.Fragment, missingColumns []string) (*datapb.SegmentInfo, error) {
			patchedBaseManifest = seg.GetManifestPath()
			patchedFragments = append([]packed.Fragment(nil), fragments...)
			patchedColumns = append([]string(nil), missingColumns...)
			return &datapb.SegmentInfo{
				ID:             seg.GetID(),
				CollectionID:   seg.GetCollectionID(),
				PartitionID:    seg.GetPartitionID(),
				NumOfRows:      seg.GetNumOfRows(),
				ManifestPath:   finalManifest,
				SchemaVersion:  req.GetSchema().GetVersion(),
				StorageVersion: seg.GetStorageVersion(),
			}, nil
		}).Build()
	defer mockPatch.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.Empty(task.GetKeptSegmentIDs())
	updated := task.GetUpdatedSegments()
	s.Require().Len(updated, 1)
	s.Equal(finalManifest, updated[0].GetManifestPath())
	s.Equal(updated, result)
	s.Equal(refreshedManifest, patchedBaseManifest)
	s.Equal(newFragments, patchedFragments)
	s.Equal([]string{"vec"}, patchedColumns)
}

func (s *RefreshExternalCollectionTaskSuite) TestShouldRefreshMilvusTableDeltalogs_SameL0KeepsSegment() {
	ctx := context.Background()
	basePath := "files/insert_log/1000/2000/1"
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	seg := &datapb.SegmentInfo{
		ID:           1,
		ManifestPath: packed.MarshalManifestPath(basePath, 1),
	}
	currentFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 7,
			}},
		}},
	}}
	newFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      88,
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 7,
			}},
		}},
	}}
	m := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return(nil, nil).Build()
	defer m.UnPatch()

	shouldRefresh, err := task.shouldRefreshMilvusTableDeltalogs(seg, currentFragments, newFragments)
	s.NoError(err)
	s.False(shouldRefresh)
}

func (s *RefreshExternalCollectionTaskSuite) TestShouldRefreshMilvusTableDeltalogs_RealPKSourceManifestDeltasKeepSegment() {
	ctx := context.Background()
	basePath := "files/insert_log/1000/2000/1"
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	seg := &datapb.SegmentInfo{
		ID:           1,
		ManifestPath: packed.MarshalManifestPath(basePath, 1),
	}
	currentFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
					EntriesNum: 7,
				},
				{
					LogPath:    "s3://source-bucket/files/insert_log/1/_delta/102",
					EntriesNum: 9,
				},
			},
		}},
	}}
	newFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      88,
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 7,
			}},
		}},
	}}

	m := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) ([]*datapb.FieldBinlog, error) {
			s.Equal("source-manifest", manifestPath)
			return []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{
					LogPath:    "s3://source-bucket/files/insert_log/1/_delta/102",
					EntriesNum: 9,
				}},
			}}, nil
		}).Build()
	defer m.UnPatch()

	shouldRefresh, err := task.shouldRefreshMilvusTableDeltalogs(seg, currentFragments, newFragments)
	s.NoError(err)
	s.False(shouldRefresh)
}

func (s *RefreshExternalCollectionTaskSuite) TestShouldRefreshMilvusTableDeltalogs_VirtualPKSourceManifestDeltasKeepSegment() {
	ctx := context.Background()
	basePath := "files/insert_log/1000/2000/1"
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		ExternalSource:         "s3://bucket/snapshot/metadata.json",
		ExternalSpec:           `{"format":"milvus-table"}`,
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 200, End: 300},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "source_pk", DataType: schemapb.DataType_Int64},
			},
		},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.nextAllocID = 200

	seg := &datapb.SegmentInfo{
		ID:           1,
		ManifestPath: packed.MarshalManifestPath(basePath, 1),
	}
	currentFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    basePath + "/_delta/102",
				EntriesNum: 7,
			}},
		}},
	}}
	newFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
	}}

	m := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) ([]*datapb.FieldBinlog, error) {
			s.Equal("source-manifest", manifestPath)
			return []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath:    "s3://source-bucket/files/insert_log/1/_delta/102",
					EntriesNum: 7,
				}},
			}}, nil
		}).Build()
	defer m.UnPatch()

	shouldRefresh, err := task.shouldRefreshMilvusTableDeltalogs(seg, currentFragments, newFragments)
	s.NoError(err)
	s.False(shouldRefresh)
	s.Equal(int64(200), task.nextAllocID)
}

func (s *RefreshExternalCollectionTaskSuite) TestShouldRefreshMilvusTableDeltalogs_InvalidIdentityPath() {
	ctx := context.Background()
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	seg := &datapb.SegmentInfo{
		ID:           1,
		ManifestPath: packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1),
	}
	currentFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath: "source/_delta/not-a-number",
			}},
		}},
	}}
	newFragments := []packed.Fragment{{
		FilePath: "source-manifest",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:   88,
				LogPath: "source/_delta/88",
			}},
		}},
	}}
	m := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return(nil, nil).Build()
	defer m.UnPatch()

	shouldRefresh, err := task.shouldRefreshMilvusTableDeltalogs(seg, currentFragments, newFragments)
	s.Error(err)
	s.False(shouldRefresh)
	s.Contains(err.Error(), "must end with a positive numeric log ID")
}

func (s *RefreshExternalCollectionTaskSuite) TestGetMilvusTableSourceManifestDeltalogs_CachesByManifestPath() {
	ctx := context.Background()
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource: "s3://bucket/snapshot/metadata.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	readCount := 0
	m := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) ([]*datapb.FieldBinlog, error) {
			readCount++
			s.Equal("source-manifest", manifestPath)
			return []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
					EntriesNum: 7,
				}},
			}}, nil
		}).Build()
	defer m.UnPatch()

	first, err := task.getMilvusTableSourceManifestDeltalogs("source-manifest")
	s.NoError(err)
	s.Equal(int64(88), first[0].GetBinlogs()[0].GetLogID())
	first[0].GetBinlogs()[0].LogID = 999

	second, err := task.getMilvusTableSourceManifestDeltalogs("source-manifest")
	s.NoError(err)
	s.Equal(1, readCount)
	s.Equal(int64(88), second[0].GetBinlogs()[0].GetLogID())
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_MilvusTableUsesPathDeltalogIDs() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "milvus-table-source",
		ExternalSpec:           `{"format":"milvus-table"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "id"},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"id"}

	var manifestLogID int64
	var capturedEntries []packed.DeltaLogEntry
	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			s.Equal(externalspec.FormatMilvusTable, format)
			manifestLogID = fragments[0].Deltalogs[0].GetBinlogs()[0].GetLogID()
			return "manifest.json", nil
		}).Build()
	defer m1.UnPatch()
	m4 := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			capturedEntries = append([]packed.DeltaLogEntry(nil), deltaLogs...)
			return "manifest-with-delta.json", nil
		}).Build()
	defer m4.UnPatch()
	m5 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"id": 64}, nil).Build()
	defer m5.UnPatch()

	result, err := task.balanceFragmentsToSegments(context.Background(), []packed.Fragment{{
		FragmentID: 1,
		RowCount:   10,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    "source/_delta/100",
				EntriesNum: 3,
			}},
		}},
	}})

	s.NoError(err)
	s.Len(result, 1)
	s.Equal(int64(100), result[0].GetID())
	s.Equal(int64(101), result[0].GetBinlogs()[0].GetBinlogs()[0].GetLogID())
	s.Equal(int64(100), manifestLogID)
	s.Equal([]packed.DeltaLogEntry{{
		Path:       "source/_delta/100",
		NumEntries: 3,
	}}, capturedEntries)
	s.Equal(int64(102), task.nextAllocID)
}

func (s *RefreshExternalCollectionTaskSuite) TestPopulateDeltalogIDsFromPath() {
	binlogs := []*datapb.FieldBinlog{{
		Binlogs: []*datapb.Binlog{
			{LogPath: "source/_delta/10"},
			{LogPath: "source/_delta/10"},
			{LogPath: "source/_delta/99", LogID: 99},
			{LogPath: "s3://bucket/source/_delta/11"},
			{},
		},
	}}

	err := populateDeltalogIDsFromPath(binlogs)
	s.NoError(err)
	s.Equal(int64(10), binlogs[0].GetBinlogs()[0].GetLogID())
	s.Equal(int64(10), binlogs[0].GetBinlogs()[1].GetLogID())
	s.Equal(int64(99), binlogs[0].GetBinlogs()[2].GetLogID())
	s.Equal(int64(11), binlogs[0].GetBinlogs()[3].GetLogID())
	s.Equal(int64(0), binlogs[0].GetBinlogs()[4].GetLogID())
}

func (s *RefreshExternalCollectionTaskSuite) TestPopulateDeltalogIDsFromPath_InvalidPath() {
	binlogs := []*datapb.FieldBinlog{{
		Binlogs: []*datapb.Binlog{{LogPath: "source/_delta/not-a-number"}},
	}}

	err := populateDeltalogIDsFromPath(binlogs)
	s.Error(err)
	s.Contains(err.Error(), "must end with a positive numeric log ID")
}
