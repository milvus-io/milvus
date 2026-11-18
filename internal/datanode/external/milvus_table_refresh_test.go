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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInitializeMilvusTableFieldMappingRejectsFunctionOutput(t *testing.T) {
	task := NewRefreshExternalCollectionTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://bucket/snapshot.json",
			ExternalSpec:   `{"format":"milvus-table"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "id"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
				{FieldID: 102, Name: "extra", DataType: schemapb.DataType_Int64, ExternalField: "extra"},
			},
		},
	})
	task.milvusTableSourceSchema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "extra", DataType: schemapb.DataType_Int64},
	}}

	err := task.initializeMilvusTableFieldMapping()
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "does not support function output fields")
}

func TestMilvusTableRefreshReadsTargetManifestOnce(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cfg := &indexpb.StorageConfig{RootPath: t.TempDir(), StorageType: "local"}
	const source = "source-manifest"
	manifest, err := packed.CommitManifestUpdates(
		packed.MarshalManifestPath(path.Join(cfg.RootPath, "target"), packed.ManifestEarliest), cfg,
		&packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{
			Columns: []string{"101"}, Format: "parquet",
			Files: []packed.ColumnGroupFileEntry{{
				Path: path.Join(cfg.RootPath, "source.parquet"), EndIndex: 3,
				Properties: map[string]string{
					"milvus_table.source_manifest_path":      source,
					"milvus_table.source_row_count":          "3",
					packed.MilvusTableFieldIDMappingProperty: `{"103":"101"}`,
				},
			}},
		}}},
	)
	require.NoError(t, err)

	explore := mockey.Mock(packed.FetchFragmentsFromExternalSourceWithRange).
		Return([]packed.Fragment{{FilePath: source, EndRow: 3, RowCount: 3}}, nil).Build()
	defer explore.UnPatch()
	sourceDeltas := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).Return(nil, nil).Build()
	defer sourceDeltas.UnPatch()
	targetReads := 0
	open := mockey.Mock(packed.GetManifestHandle).
		When(func(manifestPath string, _ *indexpb.StorageConfig, _ ...uint32) bool {
			if manifestPath == manifest {
				targetReads++
			}
			return false // Observe the open while letting the real storage read run.
		}).Return(nil, nil).Build()
	defer open.UnPatch()

	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		StorageConfig:          cfg,
		ExploreManifestPath:    "explore-manifest",
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		CurrentSegments:        []*datapb.SegmentInfo{{ID: 1, ManifestPath: manifest, NumOfRows: 3}},
		Schema:                 &schemapb.CollectionSchema{},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101"}
	task.milvusTableFieldIDMapping = map[string]string{"103": "101"}
	require.NoError(t, task.Execute(ctx))
	require.Equal(t, []int64{1}, task.GetKeptSegmentIDs())
	require.Equal(t, 1, targetReads)
}

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
			Binlogs: []*datapb.FieldBinlog{{
				ChildFields: []int64{100, 101, 102},
				Binlogs:     []*datapb.Binlog{{LogID: 7, EntriesNum: 1000, MemorySize: 10000}},
			}},
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
			extfs packed.ExternalSpecContext, deltas []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.Equal(oldManifest, oldPath)
			s.Same(req.GetStorageConfig(), cfg)
			s.Equal([]packed.DeltaLogEntry{{Path: newFragments[0].Deltalogs[0].Binlogs[0].LogPath, NumEntries: 99}}, deltas)
			gotFragments = fragments
			return packed.MilvusTableRefreshResult{
				ManifestPath: carriedManifest, TextStatsLogs: carriedTextStats, JSONKeyStats: carriedJSONStats,
			}, nil
		}).Build()
	defer mockRefresh.UnPatch()
	mockSourceDeltas := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
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

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableSegmentManifestCarriesStats() {
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
				{FieldID: 102, Name: "json", DataType: schemapb.DataType_JSON, ExternalField: "json"},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	// Exercise result propagation without inspecting stack-backed slices in a
	// reflection-based mock callback; the native artifact test below covers carry.
	mockRefresh := mockey.Mock(packed.RefreshMilvusTableManifest).
		Return(packed.MilvusTableRefreshResult{
			ManifestPath:  finalManifest,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{100: {FieldID: 100, Version: 2, BuildID: 20}},
			JSONKeyStats:  map[int64]*datapb.JsonKeyStats{102: {FieldID: 102, Version: 2, BuildID: 21, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}},
		}, nil).Build()
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
	}}, true)
	s.NoError(err)
	s.Equal(1, mockRefresh.Times())
	s.Equal(seg.GetID(), updated.GetID())
	s.Equal(finalManifest, updated.GetManifestPath())
	s.Equal(storage.StorageV3, updated.GetStorageVersion())
	s.Equal(int64(2), updated.GetTextStatsLogs()[100].GetVersion())
	s.Equal(int64(2), updated.GetJsonKeyStats()[102].GetVersion())
	s.Equal(oldManifest, seg.GetManifestPath(), "source SegmentInfo must remain immutable")
}

func (s *RefreshExternalCollectionTaskSuite) TestRefreshMilvusTableManifestReusesStatsInOneCommit() {
	paramtable.Init()
	storageConfig := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
	basePath := path.Join(storageConfig.GetRootPath(), "carry_milvus_table_artifacts/segment-1")
	oldDeltaPath := path.Join(basePath, "_delta/old")
	newDeltaPath := path.Join(basePath, "_delta/new")
	oldTextStatsPath := path.Join(basePath, "_stats/text_index.100/old")
	newTextStatsPath := path.Join(basePath, "_stats/text_index.100/new")
	jsonStatsPath := path.Join(basePath, "_stats/json_stats.102/old")
	for filePath, content := range map[string]string{
		oldDeltaPath:     "old-delta",
		newDeltaPath:     "new-delta",
		oldTextStatsPath: "old-text-stats",
		newTextStatsPath: "new-text-stats",
		jsonStatsPath:    "json-stats",
	} {
		s.Require().NoError(packed.WriteFile(storageConfig, filePath, []byte(content)))
	}

	colSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "text",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"}),
	}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, colSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()

	snapshotBase := path.Join(storageConfig.RootPath, "source")
	snapshotWriter, err := packed.NewFFIPackedWriter(snapshotBase, colSchema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}}, storageConfig, nil)
	s.Require().NoError(err)
	s.Require().NoError(snapshotWriter.WriteRecordBatch(record))
	snapshotOutput, err := snapshotWriter.Close()
	s.Require().NoError(err)
	defer snapshotOutput.Destroy()
	snapshotManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(snapshotBase, packed.ManifestEarliest), storageConfig,
		&packed.ManifestUpdates{NewFiles: snapshotOutput})
	s.Require().NoError(err)
	snapshotManifest, err = packed.AddStatsToManifest(snapshotManifest, storageConfig, []packed.StatEntry{
		{Key: "bloom_filter.99", Files: []string{newTextStatsPath}, Metadata: map[string]string{"build": "new"}},
		{Key: "text_index.100", Files: []string{newTextStatsPath}, Metadata: map[string]string{"version": "2", "build_id": "20"}},
	})
	s.Require().NoError(err)

	fragments := []packed.Fragment{{FilePath: snapshotManifest, EndRow: 1, RowCount: 1}}
	sourceManifest, err := packed.CreateMilvusTableManifestFromSegmentManifests(
		basePath, []string{"text"}, fragments, storageConfig, packed.ExternalSpecContext{})
	s.Require().NoError(err)
	sourceManifest, err = packed.AddStatsToManifest(sourceManifest, storageConfig, []packed.StatEntry{
		{Key: "text_index.100", Files: []string{oldTextStatsPath}, Metadata: map[string]string{
			"build": "old", "version": "1", "build_id": "10", "log_size": "100", "memory_size": "200",
		}},
		{Key: "json_stats.102", Files: []string{jsonStatsPath}, Metadata: map[string]string{
			"build": "old", "version": "1", "build_id": "11", "log_size": "101", "memory_size": "201",
			"json_key_stats_data_format": "1",
		}},
		{Key: "bloom_filter.99", Files: []string{oldTextStatsPath}, Metadata: map[string]string{"build": "old"}},
	})
	s.Require().NoError(err)
	sourceManifest, err = packed.AddDeltaLogsToManifestOverwrite(sourceManifest, storageConfig, []packed.DeltaLogEntry{
		{Path: oldDeltaPath, NumEntries: 1},
	})
	s.Require().NoError(err)

	_, oldVersion, err := packed.UnmarshalManifestPath(sourceManifest)
	s.Require().NoError(err)
	carryResult, err := packed.RefreshMilvusTableManifest(context.Background(), sourceManifest,
		[]string{"text"}, fragments, storageConfig, packed.ExternalSpecContext{},
		[]packed.DeltaLogEntry{{Path: newDeltaPath, NumEntries: 2}})
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

	reader, err := packed.NewFFIPackedReader(
		finalManifest, colSchema, []string{"text"}, 8192, storageConfig, nil, packed.ExternalReaderContext{},
	)
	s.Require().NoError(err)
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
					_ packed.ExternalSpecContext, _ []packed.DeltaLogEntry,
				) (packed.MilvusTableRefreshResult, error) {
					commits++
					if stage == "commit" {
						return packed.MilvusTableRefreshResult{}, failure
					}
					cancel()
					return packed.MilvusTableRefreshResult{ManifestPath: packed.MarshalManifestPath("files/segment", 2)}, nil
				}).Build()
			defer commit.UnPatch()

			updated, err := task.refreshMilvusTableSegmentManifest(ctx, seg, nil, true)
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
	mockSource := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
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
			extfs packed.ExternalSpecContext, deltas []packed.DeltaLogEntry,
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
		[]packed.Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, true)
	s.Require().NoError(err)
	s.Equal(packed.MarshalManifestPath(basePath, 2), updated.ManifestPath)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableMissingChildFieldsUsesManifest() {
	paramtable.Init()
	readErr := merr.WrapErrServiceUnavailableMsg("manifest read unavailable")
	for _, tc := range []struct {
		name        string
		changed     bool
		hasBinlogs  bool
		readErr     error
		sampleErr   error
		emptySample bool
	}{
		{name: "unchanged segment after restart"},
		{name: "delete refresh after restart", changed: true},
		{name: "delete refresh keeps existing binlogs", changed: true, hasBinlogs: true},
		{name: "read failure does not append columns", readErr: readErr},
		{name: "sample failure publishes no patch", changed: true, sampleErr: readErr},
		{name: "empty sample publishes no patch", changed: true, emptySample: true},
	} {
		s.Run(tc.name, func() {
			ctx := context.Background()
			oldManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)
			newManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 2)
			// Catalog recovery has no fake binlogs, but the pinned manifest still
			// covers the external field. No generic milvus-table group is needed.
			seg := &datapb.SegmentInfo{
				ID: 1, CollectionID: s.collectionID, PartitionID: 2000,
				NumOfRows: 10, ManifestPath: oldManifest, StorageVersion: storage.StorageV3,
			}
			if tc.hasBinlogs {
				seg.Binlogs = buildFakeBinlogs(1, 10, 777, &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{{FieldID: 100}},
				}, externalspec.FormatMilvusTable)
			}
			task := s.newTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
				CollectionID: s.collectionID, CurrentSegments: []*datapb.SegmentInfo{seg},
				ExternalSpec: `{"format":"milvus-table"}`,
				Schema: &schemapb.CollectionSchema{
					Version: 7, ExternalSpec: `{"format":"milvus-table"}`,
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", ExternalField: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
					},
				},
			})
			task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
			fragment := packed.Fragment{FilePath: "source-manifest", StartRow: 0, EndRow: 10, RowCount: 10}
			latest := fragment
			if tc.changed {
				latest.Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
					LogID: 88, LogPath: "s3://source/files/insert_log/1/_delta/88", EntriesNum: 1,
				}}}}
			}
			read := mockey.Mock(packed.ManifestMissingColumns).Return(nil, tc.readErr).Build()
			defer read.UnPatch()
			source := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).Return(nil, nil).Build()
			defer source.UnPatch()
			prepare := mockey.Mock((*RefreshExternalCollectionTask).prepareMilvusTableL0Deltalogs).Return(nil, nil).Build()
			defer prepare.UnPatch()
			refresh := mockey.Mock(packed.RefreshMilvusTableManifest).
				Return(packed.MilvusTableRefreshResult{ManifestPath: newManifest}, nil).Build()
			defer refresh.UnPatch()
			appendColumns := mockey.Mock((*RefreshExternalCollectionTask).patchSegmentForMissingColumns).Return(nil, nil).Build()
			defer appendColumns.UnPatch()
			var sampledManifest string
			sample := mockey.Mock(packed.SampleExternalFieldSizes).
				To(func(manifest string, _ int, _ int64, _, _ string, _ *schemapb.CollectionSchema, _ *indexpb.StorageConfig) (map[string]int64, error) {
					sampledManifest = manifest
					if tc.emptySample {
						return nil, nil
					}
					return map[string]int64{"100": 8}, tc.sampleErr
				}).Build()
			defer sample.UnPatch()

			result, err := task.organizeSegments(ctx, packed.SegmentFragments{1: {fragment}}, []packed.Fragment{latest})
			if tc.hasBinlogs {
				s.Zero(read.Times())
			} else {
				s.Equal(1, read.Times())
			}
			s.Zero(appendColumns.Times(), "missing ChildFields must not create a milvus-table column group")
			s.Equal(oldManifest, seg.GetManifestPath(), "do not mutate dispatched metadata")
			if tc.readErr != nil {
				s.ErrorIs(err, tc.readErr)
				s.Empty(result)
				s.Empty(task.GetUpdatedSegments())
				s.Empty(task.GetKeptSegmentIDs())
				s.Zero(prepare.Times())
				s.Zero(refresh.Times())
				s.Zero(sample.Times())
				return
			}
			if tc.sampleErr != nil || tc.emptySample {
				if tc.sampleErr != nil {
					s.ErrorIs(err, tc.sampleErr)
				} else {
					s.ErrorContains(err, "non-positive average size")
				}
				s.Empty(result)
				s.Empty(task.GetUpdatedSegments())
				s.Empty(task.GetKeptSegmentIDs())
				s.Equal(newManifest, sampledManifest)
				s.Empty(seg.GetBinlogs(), "failed repair must not mutate the input")
				return
			}
			s.Require().NoError(err)
			s.Require().Len(result, 1)
			s.Equal(int64(1), result[0].GetID())
			if tc.changed {
				s.Equal(1, prepare.Times())
				s.Equal(1, refresh.Times())
				s.Empty(task.GetKeptSegmentIDs())
				s.Require().Len(task.GetUpdatedSegments(), 1)
				updated := task.GetUpdatedSegments()[0]
				s.Equal(newManifest, updated.GetManifestPath())
				// These are the fake-binlog and row-count invariants enforced by
				// DataCoord's validateExternalRefreshUpdatedSegment/Patch.
				s.Require().NotEmpty(updated.GetBinlogs())
				s.Equal(updated.GetNumOfRows(), segmentutil.CalcRowCountFromBinLog(updated))
				s.Equal([]int64{100}, updated.GetBinlogs()[0].GetChildFields())
				if tc.hasBinlogs {
					s.Zero(sample.Times(), "valid metadata must not be sampled again")
					s.Equal(seg.GetBinlogs(), updated.GetBinlogs())
				} else {
					s.Equal(1, sample.Times())
					s.Equal(newManifest, sampledManifest)
					s.Equal(int32(7), updated.GetSchemaVersion())
					s.Equal(int64(80), updated.GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
					s.Empty(seg.GetBinlogs(), "successful repair must not mutate the input")
				}
			} else {
				s.Zero(refresh.Times())
				s.Equal([]int64{1}, task.GetKeptSegmentIDs())
				s.Empty(task.GetUpdatedSegments())
				s.Zero(sample.Times(), "a kept segment does not need patch metadata")
			}
		})
	}
}

// A column-only repair must preserve the complete target delete set without
// reopening source PK data or rewriting already translated virtual-PK logs.
func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_VirtualPKColumnRepairPreservesDeletes() {
	paramtable.Init()
	readErr := merr.WrapErrServiceUnavailableMsg("target manifest unavailable")
	for _, tc := range []struct {
		name    string
		changed bool
		empty   bool
		readErr error
	}{
		{name: "unchanged deletes"},
		{name: "empty delete set", empty: true},
		{name: "changed deletes", changed: true},
		{name: "target manifest read failure", readErr: readErr},
	} {
		s.Run(tc.name, func() {
			ctx := context.Background()
			basePath := "files/insert_log/1000/2000/1"
			oldManifest := packed.MarshalManifestPath(basePath, 1)
			newManifest := packed.MarshalManifestPath(basePath, 2)
			schema := &schemapb.CollectionSchema{
				Version: 7, ExternalSpec: `{"format":"milvus-table"}`,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: common.VirtualPKFieldName, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "source_id", ExternalField: "id", DataType: schemapb.DataType_Int64},
					{FieldID: 103, Name: "extra", ExternalField: "extra", Nullable: true, DataType: schemapb.DataType_Int64},
				},
			}
			seg := &datapb.SegmentInfo{
				ID: 1, CollectionID: s.collectionID, PartitionID: 2000,
				NumOfRows: 10, ManifestPath: oldManifest, StorageVersion: storage.StorageV3,
			}
			req := &datapb.RefreshExternalCollectionTaskRequest{
				CollectionID: s.collectionID,
				Schema:       schema, ExternalSpec: schema.ExternalSpec, CurrentSegments: []*datapb.SegmentInfo{seg},
			}
			task := s.newTask(ctx, req)
			task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
			task.columns = []string{"101", "103"}
			targetLogs := []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
				{LogPath: basePath + "/_delta/88", EntriesNum: 7},
				{LogPath: basePath + "/_delta/89", EntriesNum: 0},
			}}}
			sourceLogs := []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
				{LogID: 88, LogPath: "s3://source/segment/_delta/88", EntriesNum: 9},
				{LogID: 89, LogPath: "s3://source/segment/_delta/89", EntriesNum: 2},
			}}}
			want := []packed.DeltaLogEntry{{Path: basePath + "/_delta/88", NumEntries: 7}, {Path: basePath + "/_delta/89"}}
			if tc.empty {
				targetLogs, sourceLogs, want = nil, nil, nil
			}
			if tc.changed {
				sourceLogs[0].Binlogs = append(sourceLogs[0].Binlogs,
					&datapb.Binlog{LogID: 90, LogPath: "s3://source/segment/_delta/90", EntriesNum: 1})
				want = append(want, packed.DeltaLogEntry{Path: basePath + "/_delta/90", NumEntries: 1})
			}
			oldFragment := packed.Fragment{FilePath: "source-manifest", EndRow: 10, RowCount: 10, Deltalogs: targetLogs}
			newFragment := oldFragment
			newFragment.Deltalogs = sourceLogs
			missing := mockey.Mock(packed.ManifestMissingColumns).Return([]string{"103"}, nil).Build()
			defer missing.UnPatch()
			// The source has the nullable column; the target projection needs repair.
			targetReads := 0
			read := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).To(func(manifest string,
				_ *indexpb.StorageConfig, extfs packed.ExternalSpecContext, _ ...uint32,
			) ([]*datapb.FieldBinlog, error) {
				s.Equal(oldManifest, manifest)
				targetReads++
				s.Equal(packed.ExternalSpecContext{}, extfs, "target logs use target storage credentials")
				return targetLogs, tc.readErr
			}).Build()
			defer read.UnPatch()
			sourceRead := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).Return(nil, nil).Build()
			defer sourceRead.UnPatch()
			translate := mockey.Mock((*RefreshExternalCollectionTask).prepareMilvusTableVirtualPKDeltalogs).
				Return(want, nil).Build()
			defer translate.UnPatch()
			var committed []packed.DeltaLogEntry
			refresh := mockey.Mock(packed.RefreshMilvusTableManifest).To(func(_ context.Context, old string,
				_ []string, _ []packed.Fragment, _ *indexpb.StorageConfig, _ packed.ExternalSpecContext,
				logs []packed.DeltaLogEntry,
			) (packed.MilvusTableRefreshResult, error) {
				s.Equal(oldManifest, old)
				committed = append([]packed.DeltaLogEntry(nil), logs...)
				return packed.MilvusTableRefreshResult{ManifestPath: newManifest}, nil
			}).Build()
			defer refresh.UnPatch()
			sample := mockey.Mock(packed.SampleExternalFieldSizes).Return(map[string]int64{"101": 8, "103": 8}, nil).Build()
			defer sample.UnPatch()
			result, err := task.organizeSegments(ctx, packed.SegmentFragments{1: {oldFragment}}, []packed.Fragment{newFragment})
			if tc.readErr != nil {
				s.ErrorIs(err, tc.readErr)
				s.Empty(result)
				s.Empty(task.GetUpdatedSegments())
				s.Equal(1, targetReads)
				s.Zero(refresh.Times(), "never publish an empty delete set after a read failure")
				s.Zero(translate.Times())
				return
			}
			s.Require().NoError(err)
			s.Require().Len(result, 1)
			s.Equal(want, committed, "retain target paths, translated counts, and zero-entry markers")
			s.Equal(newManifest, result[0].GetManifestPath())
			if tc.changed {
				s.Zero(targetReads)
				s.Equal(1, translate.Times())
			} else {
				s.Equal(1, targetReads)
				s.Zero(translate.Times(), "unchanged deletes must not be translated or rewritten")
			}
			s.Equal(1, refresh.Times())
			s.Equal([]int64{100, 101, 103}, result[0].GetBinlogs()[0].GetChildFields())

			// DataCoord retains repaired fake binlogs in memory. Even though the
			// repaired column remains covered, another refresh is a no-op.
			if !tc.changed {
				req.CurrentSegments = result
				next := s.newTask(ctx, req)
				next.parsedSpec = task.parsedSpec
				next.columns = task.columns
				kept, err := next.organizeSegments(ctx, packed.SegmentFragments{1: {oldFragment}}, []packed.Fragment{newFragment})
				s.Require().NoError(err)
				s.Equal(result, kept)
				s.Equal([]int64{1}, next.GetKeptSegmentIDs())
				s.Empty(next.GetUpdatedSegments())
				s.Equal(1, missing.Times())
				s.Equal(1, refresh.Times(), "do not advance the manifest again")
				s.Equal(1, sample.Times())
			}
		})
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_VirtualPKColumnRepairPersistsDeleteReferences() {
	paramtable.Init()
	ctx := context.Background()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
	sourceBase := path.Join(cfg.RootPath, "source")
	targetBase := path.Join(cfg.RootPath, "target")
	groups := []packed.ColumnGroupEntry{{
		Columns: []string{"101"}, Format: "parquet",
		Files: []packed.ColumnGroupFileEntry{{Path: path.Join(sourceBase, "data.parquet"), EndIndex: 10}},
	}}
	// Only manifest metadata is needed. Source delete files deliberately do
	// not exist: unchanged deletes must never be read or translated again.
	sourceGroups := append([]packed.ColumnGroupEntry(nil), groups...)
	sourceGroups[0].Columns = []string{"101", "103"}
	source, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(sourceBase, packed.ManifestEarliest), cfg,
		&packed.ManifestUpdates{ColumnGroups: sourceGroups, DeltaLogs: []packed.DeltaLogEntry{
			{Path: path.Join(sourceBase, "_delta/88"), NumEntries: 9},
			{Path: path.Join(sourceBase, "_delta/89"), NumEntries: 2},
		}})
	s.Require().NoError(err)
	deltaPath := path.Join(targetBase, "_delta/88")
	markerPath := path.Join(targetBase, "_delta/89")
	deltaBytes := []byte("already-translated-delete-data")
	s.Require().NoError(packed.WriteFile(cfg, deltaPath, deltaBytes))
	oldManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(targetBase, packed.ManifestEarliest), cfg,
		&packed.ManifestUpdates{ColumnGroups: groups, DeltaLogs: []packed.DeltaLogEntry{
			{Path: deltaPath, NumEntries: 7}, {Path: markerPath},
		}})
	s.Require().NoError(err)
	oldLogs, err := packed.GetDeltaLogsFromManifestWithExtfs(oldManifest, cfg, packed.ExternalSpecContext{})
	s.Require().NoError(err)
	schema := &schemapb.CollectionSchema{
		Version: 7, ExternalSpec: `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: common.VirtualPKFieldName, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "source_id", ExternalField: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "extra", ExternalField: "extra", Nullable: true, DataType: schemapb.DataType_Int64},
		},
	}
	seg := &datapb.SegmentInfo{
		ID: 1, CollectionID: s.collectionID, PartitionID: 2000,
		NumOfRows: 10, ManifestPath: oldManifest, StorageVersion: storage.StorageV3,
	}
	task := s.newTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		Schema:       schema, ExternalSpec: schema.ExternalSpec, StorageConfig: cfg, CurrentSegments: []*datapb.SegmentInfo{seg},
	})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101", "103"}
	fragment := packed.Fragment{FilePath: source, EndRow: 10, RowCount: 10}
	current := fragment
	current.Deltalogs = oldLogs
	// Sampling requires real row data, which is unrelated to manifest reuse.
	sample := mockey.Mock(packed.SampleExternalFieldSizes).Return(map[string]int64{"101": 8, "103": 8}, nil).Build()
	defer sample.UnPatch()
	result, err := task.organizeSegments(ctx, packed.SegmentFragments{1: {current}}, []packed.Fragment{fragment})
	s.Require().NoError(err)
	s.Require().Len(result, 1)
	newManifest := result[0].GetManifestPath()
	_, oldVersion, err := packed.UnmarshalManifestPath(oldManifest)
	s.Require().NoError(err)
	_, newVersion, err := packed.UnmarshalManifestPath(newManifest)
	s.Require().NoError(err)
	s.Equal(oldVersion+1, newVersion)
	actual, err := packed.GetDeltaLogsFromManifestWithExtfs(newManifest, cfg, packed.ExternalSpecContext{})
	s.Require().NoError(err)
	s.Equal(oldLogs, actual, "persist translated counts and zero-entry identity markers")
	paths, err := packed.GetDeltaLogPathsFromManifest(newManifest, cfg)
	s.Require().NoError(err)
	s.Equal([]string{deltaPath}, paths, "zero-entry markers must not become readable delete files")
	content, err := packed.ReadFile(cfg, deltaPath)
	s.Require().NoError(err)
	s.Equal(deltaBytes, content, "reuse existing delete bytes without rewriting them")
	missing, err := packed.ManifestMissingColumns(newManifest, cfg, []string{"101", "103"})
	s.Require().NoError(err)
	s.Empty(missing, "import a nullable column when real source data exists")
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableL0RefreshAlsoPatchesMissingColumns() {
	ctx := context.Background()
	partitionID := int64(2000)
	oldManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)
	refreshedManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 2)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   partitionID,
		TaskID:        s.taskID,
		ExternalSpec:  `{"format":"milvus-table"}`,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Version: 7, ExternalSpec: `{"format":"milvus-table"}`,
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
	task.columns = typeutil.NewStorageColumnResolver(req.GetSchema()).SourceDataColumnNames()
	task.nextAllocID = req.GetPreAllocatedSegmentIds().GetBegin()
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	missingColumns := mockey.Mock(packed.ManifestMissingColumns).Return([]string{"101"}, nil).Build()
	defer missingColumns.UnPatch()

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
			_ packed.ExternalSpecContext, _ []packed.DeltaLogEntry,
		) (packed.MilvusTableRefreshResult, error) {
			s.Equal(oldManifest, old)
			return packed.MilvusTableRefreshResult{ManifestPath: refreshedManifest}, nil
		}).Build()
	defer mockRefresh.UnPatch()
	mockSourceDeltas := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
		Return(nil, nil).Build()
	defer mockSourceDeltas.UnPatch()

	appendColumns := mockey.Mock(packed.AppendSegmentManifestColumns).
		Return("", merr.WrapErrServiceInternalMsg("generic append cannot import source manifests")).Build()
	defer appendColumns.UnPatch()
	sample := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"100": 8, "101": 32}, nil).Build()
	defer sample.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.Empty(task.GetKeptSegmentIDs())
	updated := task.GetUpdatedSegments()
	s.Require().Len(updated, 1)
	s.Equal(refreshedManifest, updated[0].GetManifestPath())
	s.Equal(updated, result)
	s.Equal(1, mockRefresh.Times(), "deletes and missing columns must be repaired in one commit")
	s.Zero(appendColumns.Times())
	s.Equal(1, sample.Times())
	s.ElementsMatch([]int64{100, 101}, updated[0].GetBinlogs()[0].GetChildFields())
	s.Equal(int64(40000), updated[0].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
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
	m := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
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

	m := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext, _ []string) ([]*datapb.FieldBinlog, error) {
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

	m := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext, _ []string) ([]*datapb.FieldBinlog, error) {
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
	m := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
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
	m := mockey.Mock(packed.GetMilvusTableSourceDeltalogs).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext, _ []string) ([]*datapb.FieldBinlog, error) {
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

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableRepairsMissingPhysicalColumns() {
	paramtable.Init()
	for _, format := range []string{"parquet", "vortex"} {
		for _, scenario := range []struct {
			name        string
			virtualPK   bool
			missingLeaf bool
		}{{name: "real_pk"}, {name: "virtual_pk", virtualPK: true}, {name: "source_missing_leaf", missingLeaf: true}} {
			s.Run(format+"/"+scenario.name, func() {
				ctx := context.Background()
				cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
				fields := []arrow.Field{
					{Name: "100", Type: arrow.PrimitiveTypes.Int64},
					{Name: "1", Type: arrow.PrimitiveTypes.Int64},
					{Name: "101", Type: arrow.BinaryTypes.String},
				}
				if !scenario.missingLeaf {
					fields = append(fields, arrow.Field{Name: "103", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)})
				}
				// A source-only Struct child shares the column group with selected fields.
				fields = append(fields, arrow.Field{Name: "104", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)})
				arrowSchema := arrow.NewSchema(fields, nil)
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
				defer builder.Release()
				builder.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
				builder.Field(1).(*array.Int64Builder).AppendValues([]int64{100, 200}, nil)
				builder.Field(2).(*array.StringBuilder).AppendValues([]string{`{"tag":"a"}`, `{"tag":"b"}`}, nil)
				if !scenario.missingLeaf {
					list := builder.Field(3).(*array.ListBuilder)
					list.Append(true)
					list.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
					list.Append(true)
					list.ValueBuilder().(*array.Int64Builder).Append(3)
				}
				ignored := builder.Field(len(fields) - 1).(*array.ListBuilder)
				ignored.Append(true)
				ignored.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{4, 5}, nil)
				ignored.Append(true)
				ignored.ValueBuilder().(*array.Int64Builder).Append(6)
				record := builder.NewRecord()
				defer record.Release()
				indices := make([]int, len(fields))
				for i := range indices {
					indices[i] = i
				}
				sourceBase := path.Join(cfg.RootPath, "source")
				writer, err := packed.NewFFIPackedWriter(sourceBase, arrowSchema,
					[]storagecommon.ColumnGroup{{Columns: indices, GroupID: storagecommon.DefaultShortColumnGroupID}}, cfg, nil,
					map[string]string{packed.PropertyWriterFormat: format})
				s.Require().NoError(err)
				s.Require().NoError(writer.WriteRecordBatch(record))
				output, err := writer.Close()
				s.Require().NoError(err)
				defer output.Destroy()
				sourceManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(sourceBase, packed.ManifestEarliest), cfg,
					&packed.ManifestUpdates{NewFiles: output})
				s.Require().NoError(err)
				fragments := []packed.Fragment{{FilePath: sourceManifest, EndRow: 2, RowCount: 2}}
				// Deliberately omit dynamic and Struct leaf columns from the target
				// manifest while the source still contains the complete row data.
				targetManifest, err := packed.CreateMilvusTableManifestFromSegmentManifests(path.Join(cfg.RootPath, "target"),
					[]string{"100", "1"}, fragments, cfg, packed.ExternalSpecContext{})
				s.Require().NoError(err)
				schema := &schemapb.CollectionSchema{
					Version: 7, ExternalSpec: `{"format":"milvus-table"}`, EnableDynamicField: true,
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "id", ExternalField: "pk", IsPrimaryKey: !scenario.virtualPK, DataType: schemapb.DataType_Int64},
						{FieldID: 101, Name: common.MetaFieldName, ExternalField: common.MetaFieldName, IsDynamic: true, DataType: schemapb.DataType_JSON},
					},
					StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: "items", Fields: []*schemapb.FieldSchema{{
						FieldID: 103, Name: "items[value]", ExternalField: "items[value]", DataType: schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
					}}}},
				}
				expectedFields := []int64{100, 101, 103}
				if scenario.virtualPK {
					schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
						FieldID: 105, Name: common.VirtualPKFieldName,
						IsPrimaryKey: true, DataType: schemapb.DataType_Int64,
					})
					expectedFields = append(expectedFields, 105)
				}
				seg := &datapb.SegmentInfo{
					ID: 1, CollectionID: s.collectionID, PartitionID: 2000,
					NumOfRows: 2, ManifestPath: targetManifest, StorageVersion: storage.StorageV3,
					Binlogs: []*datapb.FieldBinlog{{ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{EntriesNum: 2, MemorySize: 16}}}},
				}
				task := s.newTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
					CollectionID: s.collectionID, Schema: schema, ExternalSpec: schema.ExternalSpec, StorageConfig: cfg,
					CurrentSegments: []*datapb.SegmentInfo{seg},
				})
				task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
				task.columns = typeutil.NewStorageColumnResolver(schema).SourceDataColumnNames()
				result, err := task.organizeSegments(ctx, packed.SegmentFragments{1: fragments}, fragments)
				s.Equal(targetManifest, seg.GetManifestPath(), "never mutate dispatched metadata")
				if scenario.missingLeaf {
					s.Require().Error(err)
					s.ErrorContains(err, "103")
					s.Empty(result)
					s.Empty(task.GetUpdatedSegments(), "missing source data must not be published with zero memory")
					return
				}
				s.Require().NoError(err)
				s.Require().Len(result, 1)
				s.Require().Len(task.GetUpdatedSegments(), 1)
				s.Empty(task.GetKeptSegmentIDs())
				s.Equal(seg.ID, result[0].GetID(), "repair must preserve virtual PK segment identity")
				s.Equal(int32(7), result[0].GetSchemaVersion())
				s.ElementsMatch(expectedFields, result[0].GetBinlogs()[0].GetChildFields())
				s.Greater(result[0].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize(), int64(16))
				hasColumns, err := packed.ManifestHasColumns(result[0].GetManifestPath(), cfg, []string{"100", "101", "103"})
				s.Require().NoError(err)
				s.True(hasColumns)
				hasIgnored, err := packed.ManifestHasColumns(result[0].GetManifestPath(), cfg, []string{"104"})
				s.Require().NoError(err)
				s.False(hasIgnored, "source-only Struct child must not be published in target manifest")
				// Read repaired columns from the real files, not only manifest metadata.
				reader, err := packed.NewFFIPackedReader(result[0].GetManifestPath(), arrowSchema,
					[]string{"100", "101", "103"}, 8192, cfg, nil, packed.ExternalReaderContext{})
				s.Require().NoError(err)
				defer reader.Close()
				got, err := reader.ReadNext()
				s.Require().NoError(err)
				defer got.Release()
				for i, name := range []string{"100", "101", "103"} {
					s.True(array.Equal(record.Column(arrowSchema.FieldIndices(name)[0]), got.Column(i)), "column %s", name)
				}
				// A second refresh of the repaired manifest must be a no-op.
				task.req.CurrentSegments = result
				again, err := task.organizeSegments(ctx, packed.SegmentFragments{1: fragments}, fragments)
				s.Require().NoError(err)
				s.Require().Len(again, 1)
				s.Equal(result[0].GetManifestPath(), again[0].GetManifestPath())
				s.Empty(task.GetUpdatedSegments())
				s.Equal([]int64{1}, task.GetKeptSegmentIDs())
			})
		}
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestSourceManifestReadRejectsMissingColumn() {
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
	source, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(path.Join(cfg.RootPath, "source"), packed.ManifestEarliest), cfg,
		&packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{
			Columns: []string{"100", "1"}, Format: "parquet",
			Files: []packed.ColumnGroupFileEntry{{Path: path.Join(cfg.RootPath, "data.parquet"), EndIndex: 3}},
		}}})
	s.Require().NoError(err)
	task := NewRefreshExternalCollectionTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{
		StorageConfig: cfg, ExternalSpec: `{"format":"milvus-table"}`,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", ExternalField: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "extra", ExternalField: "extra", Nullable: true, DataType: schemapb.DataType_Int64},
		}},
	})
	for i := 0; i < 2; i++ {
		logs, err := task.getMilvusTableSourceManifestDeltalogs(source)
		s.Require().Error(err, "a failed validation must not enter the task cache")
		s.Nil(logs)
		s.Contains(err.Error(), "field_id=103")
		s.Contains(err.Error(), "new snapshot")
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_KeptSourceMissingColumn() {
	for _, virtualPK := range []bool{false, true} {
		name := "real_pk"
		if virtualPK {
			name = "virtual_pk"
		}
		s.Run(name, func() {
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()}
			source, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(path.Join(cfg.RootPath, "source"), packed.ManifestEarliest), cfg,
				&packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{
					Columns: []string{"100", "1"}, Format: "parquet",
					Files: []packed.ColumnGroupFileEntry{{Path: path.Join(cfg.RootPath, "data.parquet"), EndIndex: 3}},
				}}})
			s.Require().NoError(err)
			fields := []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", ExternalField: "id", IsPrimaryKey: !virtualPK, DataType: schemapb.DataType_Int64},
				{FieldID: 103, Name: "extra", ExternalField: "extra", Nullable: true, DataType: schemapb.DataType_Int64},
			}
			if virtualPK {
				fields = append(fields, &schemapb.FieldSchema{FieldID: 104, Name: common.VirtualPKFieldName, IsPrimaryKey: true, DataType: schemapb.DataType_Int64})
			}
			// Complete target metadata must not hide a missing source column.
			seg := &datapb.SegmentInfo{
				ID: 1, ManifestPath: "unused-target-manifest",
				Binlogs: []*datapb.FieldBinlog{{ChildFields: []int64{100, 103}}},
			}
			task := NewRefreshExternalCollectionTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{
				StorageConfig: cfg, ExternalSpec: `{"format":"milvus-table"}`, Schema: &schemapb.CollectionSchema{Fields: fields},
				CurrentSegments: []*datapb.SegmentInfo{seg},
			})
			task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
			fragment := packed.Fragment{FilePath: source, RowCount: 3, EndRow: 3}
			result, err := task.organizeSegments(context.Background(), packed.SegmentFragments{1: {fragment}}, []packed.Fragment{fragment})
			s.Require().Error(err)
			s.Contains(err.Error(), "field_id=103")
			s.Contains(err.Error(), "new snapshot")
			s.Nil(result)
			s.Empty(task.GetKeptSegmentIDs())
			s.Empty(task.GetUpdatedSegments())
		})
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableProjectedStructColumnRecovery() {
	paramtable.Init()
	dir := s.T().TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	makeManifest := func(name string, columns []string) string {
		m, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(path.Join(dir, name), packed.ManifestEarliest), cfg, &packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{Columns: columns, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: path.Join(dir, "data.parquet"), EndIndex: 10}}}}})
		s.Require().NoError(err)
		return m
	}
	source := makeManifest("source", []string{"1", "100", "102", "103"})
	target := makeManifest("target", []string{"1", "100", "102"})
	schema := &schemapb.CollectionSchema{Version: 2, ExternalSpec: `{"format":"milvus-table"}`, Fields: []*schemapb.FieldSchema{{Name: "id", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "id"}}, StructArrayFields: []*schemapb.StructArrayFieldSchema{{Name: "clips", FieldID: 101, Nullable: true, Fields: []*schemapb.FieldSchema{
		{Name: "clips[base]", ExternalField: "clips[base]", FieldID: 102, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{Name: "clips[note]", ExternalField: "clips[note]", FieldID: 103, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true},
	}}}}
	seg := &datapb.SegmentInfo{ID: 1, CollectionID: s.collectionID, PartitionID: 2000, NumOfRows: 10, SchemaVersion: 2, StorageVersion: storage.StorageV3, ManifestPath: target}
	task := s.newTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{CollectionID: s.collectionID, Schema: schema, CurrentSegments: []*datapb.SegmentInfo{seg}, ExternalSpec: schema.ExternalSpec, StorageConfig: cfg})
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = typeutil.NewStorageColumnResolver(schema).SourceDataColumnNames()
	sample := mockey.Mock(packed.SampleExternalFieldSizes).Return(map[string]int64{"1": 8, "100": 8, "102": 12, "103": 16}, nil).Build()
	defer sample.UnPatch()
	fragment := packed.Fragment{FilePath: source, EndRow: 10, RowCount: 10}
	result, err := task.organizeSegments(context.Background(), packed.SegmentFragments{1: {fragment}}, []packed.Fragment{fragment})
	s.Require().NoError(err)
	s.Require().Len(task.GetUpdatedSegments(), 1, "a column omitted by an older projection is not a historical NULL column")
	s.Require().Len(result, 1)
	s.Equal(int64(1), result[0].GetID())
	fields, err := packed.GetManifestFieldIDs(result[0].GetManifestPath(), cfg)
	s.Require().NoError(err)
	s.Contains(fields, int64(103), "the existing source values must be imported")
	s.Equal(1, sample.Times())
}
