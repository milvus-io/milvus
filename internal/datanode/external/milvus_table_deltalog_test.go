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
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type fakeMilvusTableDeltalogReader struct {
	records  []storage.Record
	nextErr  error
	closeErr error
	next     int
}

func (r *fakeMilvusTableDeltalogReader) Next() (storage.Record, error) {
	if r.nextErr != nil {
		return nil, r.nextErr
	}
	if r.next >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.next]
	r.next++
	return record, nil
}

func (r *fakeMilvusTableDeltalogReader) Close() error {
	return r.closeErr
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestForSegment_MilvusTableVirtualPKMode() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    2000,
		ExternalSource: "s3://bucket/snapshot.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector},
			},
		},
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101"}

	var gotMode packed.MilvusTablePrimaryKeyMode
	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			gotMode = extfs.MilvusTablePKMode
			return "manifest-path", nil
		}).Build()
	defer mockCreate.UnPatch()

	manifestPath, err := task.createManifestForSegment(ctx, 3000, []packed.Fragment{{FragmentID: 1}})
	s.NoError(err)
	s.Equal("manifest-path", manifestPath)
	s.Equal(packed.MilvusTablePrimaryKeyModeVirtual, gotMode)
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestForSegment_MilvusTableVirtualPKTranslatesDeltalogs() {
	ctx := context.Background()
	paramtable.Init()
	dir := s.T().TempDir()
	storageConfig := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "source_id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	metadataPath := filepath.Join(dir, "snapshots/10/metadata/20.json")
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&datapb.SnapshotMetadata{
		Collection: &datapb.CollectionDescription{
			Schema: sourceSchema,
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(packed.WriteFile(storageConfig, metadataPath, metadataBytes))

	sourceManifest := createSourcePKManifest(
		s.T(),
		filepath.Join(dir, "insert_log/10/20/30"),
		storageConfig,
		[]int64{10, 20, 20, 30},
	)
	sourceDeltalogPath := filepath.Join(dir, "insert_log/10/20/30/_delta/9001")
	writeDeltalog(s.T(), storageConfig, sourceDeltalogPath, schemapb.DataType_Int64,
		[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20), storage.NewInt64PrimaryKey(999)},
		[]storage.Timestamp{100, 101})

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    2000,
		ExternalSource: metadataPath,
		ExternalSpec:   `{"format":"milvus-table"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector},
			},
		},
		StorageConfig: storageConfig,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101"}

	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			return "manifest-path", nil
		}).Build()
	defer mockCreate.UnPatch()
	var capturedEntries []packed.DeltaLogEntry
	mockAdd := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			s.Equal("manifest-path", manifestPath)
			capturedEntries = append([]packed.DeltaLogEntry(nil), deltaLogs...)
			return "manifest-with-delta", nil
		}).Build()
	defer mockAdd.UnPatch()

	manifestPath, err := task.createManifestForSegment(ctx, 3000, []packed.Fragment{{
		FragmentID: 1,
		FilePath:   sourceManifest,
		StartRow:   0,
		EndRow:     4,
		RowCount:   4,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceDeltalogPath,
				LogID:      9001,
				EntriesNum: 2,
			}},
		}},
	}})
	s.Require().NoError(err)
	s.Equal("manifest-with-delta", manifestPath)
	s.Equal([]packed.DeltaLogEntry{{
		Path:       filepath.Join(dir, "insert_log/1000/2000/3000/_delta/9001"),
		NumEntries: 2,
	}}, capturedEntries)
	s.Require().Len(capturedEntries, 1)

	pks, tss := readInt64Deltalog(s.T(), storageConfig, capturedEntries[0].Path)
	s.Equal([]int64{testVirtualPK(3000, 1), testVirtualPK(3000, 2)}, pks)
	s.Equal([]int64{100, 100}, tss)
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestForSegment_MilvusTableVirtualPKRejectsUnsupportedDeltalogs() {
	ctx := context.Background()
	paramtable.Init()
	dir := s.T().TempDir()
	storageConfig := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
	metadataPath := filepath.Join(dir, "snapshots/10/metadata/20.json")
	sourceDeltalogPath := filepath.Join(dir, "files/not_delta_log/10/20/30/9002")

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    2000,
		ExternalSource: metadataPath,
		ExternalSpec:   `{"format":"milvus-table"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector},
			},
		},
		StorageConfig: storageConfig,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101"}

	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			return "manifest-path", nil
		}).Build()
	defer mockCreate.UnPatch()

	manifestPath, err := task.createManifestForSegment(ctx, 3000, []packed.Fragment{{
		FragmentID: 1,
		FilePath:   "source-manifest",
		StartRow:   0,
		EndRow:     3,
		RowCount:   3,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceDeltalogPath,
				LogID:      9002,
				EntriesNum: 2,
			}},
		}},
	}})
	s.Empty(manifestPath)
	s.Error(err)
	s.Contains(err.Error(), "only supports StorageV3 source deltalogs under _delta or legacy L0 deltalogs under delta_log")
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestForSegment_MilvusTableVirtualPKReadsSourcePKWithExtfs() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{StorageType: "local"}
	metadataPath := "s3://source-bucket/snapshots/10/metadata/20.json"
	externalSpec := `{"format":"milvus-table","extfs":{"access_key_id":"ak","access_key_value":"sk"}}`
	sourceManifest := `{"base_path":"s3://source-bucket/source/segment/10","ver":1}`
	sourceDeltalogPath := "s3://source-bucket/source/segment/10/_delta/9001"
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "source_id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&datapb.SnapshotMetadata{
		Collection: &datapb.CollectionDescription{Schema: sourceSchema},
	})
	s.Require().NoError(err)

	mockReadMetadata := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(storageConfig *indexpb.StorageConfig, filePath string, extfs packed.ExternalSpecContext) ([]byte, error) {
			s.Equal(s.collectionID, extfs.CollectionID)
			s.Equal(metadataPath, extfs.Source)
			s.Equal(externalSpec, extfs.Spec)
			if filePath != metadataPath {
				s.Failf("unexpected external read", "path=%s", filePath)
				return nil, fmt.Errorf("unexpected external read %s", filePath)
			}
			return metadataBytes, nil
		}).Build()
	defer mockReadMetadata.UnPatch()

	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			return "manifest-path", nil
		}).Build()
	defer mockCreate.UnPatch()

	record, _, _, err := storage.BuildDeleteRecord(
		[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20)},
		[]storage.Timestamp{100},
	)
	s.Require().NoError(err)
	mockDeltalogReader := mockey.Mock(storage.NewDeltalogReader).
		To(func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			s.Equal(schemapb.DataType_Int64, pkType)
			s.Equal([]string{sourceDeltalogPath}, paths)
			return &fakeMilvusTableDeltalogReader{records: []storage.Record{record}}, nil
		}).Build()
	defer mockDeltalogReader.UnPatch()

	readerErr := fmt.Errorf("stop after extfs capture")
	var gotExtfs packed.ExternalSpecContext
	mockReader := mockey.Mock(storage.NewManifestReaderWithExtfs).
		To(func(
			manifest string,
			schema *schemapb.CollectionSchema,
			bufferSize int64,
			storageConfig *indexpb.StorageConfig,
			storagePluginContext *indexcgopb.StoragePluginContext,
			extfs packed.ExternalSpecContext,
		) (*storage.ManifestReader, error) {
			gotExtfs = extfs
			return nil, readerErr
		}).Build()
	defer mockReader.UnPatch()

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    2000,
		ExternalSource: metadataPath,
		ExternalSpec:   externalSpec,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector},
			},
		},
		StorageConfig: storageConfig,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}
	task.columns = []string{"101"}

	_, err = task.createManifestForSegment(ctx, 3000, []packed.Fragment{{
		FragmentID: 1,
		FilePath:   sourceManifest,
		StartRow:   0,
		EndRow:     10,
		RowCount:   10,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceDeltalogPath,
				LogID:      9001,
				EntriesNum: 1,
			}},
		}},
	}})
	s.ErrorIs(err, readerErr)
	s.Equal(packed.ExternalSpecContext{
		CollectionID: s.collectionID,
		Source:       metadataPath,
		Spec:         externalSpec,
	}, gotExtfs)
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestWithFunctions_MilvusTableRealPKAddsL0Deltalogs() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   1000,
		PartitionID:    2000,
		ExternalSource: "s3://source-bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		StorageConfig:  &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://source-bucket/snapshots/100/metadata/200.json",
			ExternalSpec:   `{"format":"milvus-table"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{{
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			}},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: externalspec.FormatMilvusTable}

	mockExec := mockey.Mock(ExecuteFunctionsForSegment).Return("manifest-path", nil).Build()
	defer mockExec.UnPatch()
	mockAdd := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			s.Equal("manifest-path", manifestPath)
			s.Equal([]packed.DeltaLogEntry{{
				Path:       "s3://source-bucket/files/insert_log/1/_delta/88",
				NumEntries: 7,
			}}, deltaLogs)
			return "manifest-with-deltas", nil
		}).Build()
	defer mockAdd.UnPatch()

	manifestPath, err := task.createManifestWithFunctions(ctx, 3000, []packed.Fragment{{
		FragmentID: 1,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      88,
				LogPath:    "s3://source-bucket/files/insert_log/1/_delta/88",
				EntriesNum: 7,
			}},
		}},
	}})
	s.NoError(err)
	s.Equal("manifest-with-deltas", manifestPath)
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_RequiresAllocatedLogID() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:   s.collectionID,
			ExternalSource: "milvus-table-source",
			ExternalSpec:   `{"format":"milvus-table"}`,
			StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	fragments := []packed.Fragment{{
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    "source/_delta/100",
				EntriesNum: 10,
			}},
		}},
	}}

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		context.Background(),
		"external/1000/segments/10/manifest.json",
		fragments,
	)

	s.Empty(manifest)
	s.Error(err)
	s.Contains(err.Error(), "has no allocated log ID")
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_AllowsLegacyL0Deltalogs() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:   s.collectionID,
			ExternalSource: "milvus-table-source",
			ExternalSpec:   `{"format":"milvus-table"}`,
			StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	mockAdd := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			s.Equal("manifest.json", manifestPath)
			s.Equal([]packed.DeltaLogEntry{{
				Path:       "files/delta_log/100",
				NumEntries: 10,
			}}, deltaLogs)
			return "manifest-with-delta.json", nil
		}).Build()
	defer mockAdd.UnPatch()

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		context.Background(),
		"manifest.json",
		[]packed.Fragment{{
			Deltalogs: []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath:    "files/delta_log/100",
					LogID:      100,
					EntriesNum: 10,
				}},
			}},
		}},
	)

	s.NoError(err)
	s.Equal("manifest-with-delta.json", manifest)
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_NoDeltalogs() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:  s.collectionID,
			StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	manifestPath := "external/1000/segments/10/manifest.json"

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		context.Background(),
		manifestPath,
		[]packed.Fragment{{Deltalogs: []*datapb.FieldBinlog{{}}}},
	)

	s.NoError(err)
	s.Equal(manifestPath, manifest)
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_AddsUniqueDeltalogs() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:   s.collectionID,
			ExternalSource: "milvus-table-source",
			ExternalSpec:   `{"format":"milvus-table"}`,
			StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	var capturedEntries []packed.DeltaLogEntry

	m3 := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			s.Equal("manifest.json", manifestPath)
			capturedEntries = append([]packed.DeltaLogEntry(nil), deltaLogs...)
			return "manifest-with-delta.json", nil
		}).Build()
	defer m3.UnPatch()

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		context.Background(),
		"manifest.json",
		[]packed.Fragment{{
			Deltalogs: []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{
					{LogPath: "source/_delta/101", LogID: 101, EntriesNum: 11},
					{LogPath: "source/_delta/101", LogID: 101, EntriesNum: 11},
					{LogPath: ""},
					{LogPath: "source/_delta/102", LogID: 102, EntriesNum: 12},
				},
			}},
		}},
	)

	s.NoError(err)
	s.Equal("manifest-with-delta.json", manifest)
	s.Equal([]packed.DeltaLogEntry{
		{Path: "source/_delta/101", NumEntries: 11},
		{Path: "source/_delta/102", NumEntries: 12},
	}, capturedEntries)
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_ContextCanceledBeforeCommit() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:  s.collectionID,
			StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manifestCalled := false
	m := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			manifestCalled = true
			return "manifest-with-delta.json", nil
		}).Build()
	defer m.UnPatch()

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		ctx,
		"manifest.json",
		[]packed.Fragment{{
			Deltalogs: []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath: "source/_delta/101",
					LogID:   101,
				}},
			}},
		}},
	)

	s.Empty(manifest)
	s.ErrorIs(err, context.Canceled)
	s.False(manifestCalled)
}

func (s *RefreshExternalCollectionTaskSuite) TestAddMilvusTableL0DeltalogsToManifest_ContextCanceledAfterCommit() {
	task := &RefreshExternalCollectionTask{
		req: &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:  s.collectionID,
			StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, deltaLogs []packed.DeltaLogEntry) (string, error) {
			cancel()
			return "manifest-with-delta.json", nil
		}).Build()
	defer m.UnPatch()

	manifest, err := task.addMilvusTableL0DeltalogsToManifest(
		ctx,
		"manifest.json",
		[]packed.Fragment{{
			Deltalogs: []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath: "source/_delta/101",
					LogID:   101,
				}},
			}},
		}},
	)

	s.Empty(manifest)
	s.ErrorIs(err, context.Canceled)
}

func (s *RefreshExternalCollectionTaskSuite) TestAppendMilvusTableSourcePKOffsetsFromRecordKeepsOnlyDeletedPKs() {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "100",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "1",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"1"}),
		},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, pk := range []int64{10, 20, 30} {
		builder.Field(0).(*array.Int64Builder).Append(pk)
		builder.Field(1).(*array.Int64Builder).Append(int64(i + 1))
	}
	arrowRecord := builder.NewRecord()
	record := storage.NewSimpleArrowRecord(arrowRecord, map[storage.FieldID]int{
		100:                   0,
		common.TimeStampField: 1,
	})
	defer record.Release()

	sourcePKOffsets := make(map[string][]milvusTableSourcePKOffset)
	deleteKeys := map[string]struct{}{"i:20": {}}
	err := appendMilvusTableSourcePKOffsetsFromRecord(
		record,
		&schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64},
		packed.Fragment{StartRow: 0, EndRow: 2},
		10,
		0,
		deleteKeys,
		sourcePKOffsets,
	)

	s.NoError(err)
	s.Equal(map[string][]milvusTableSourcePKOffset{
		"i:20": {{
			targetOffset:    11,
			insertTimestamp: 2,
		}},
	}, sourcePKOffsets)
}

func (s *RefreshExternalCollectionTaskSuite) TestAppendMilvusTableSourcePKOffsetsFromRecordErrors() {
	s.Run("bad timestamp column", func() {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int64},
			{Name: "1", Type: arrow.BinaryTypes.String},
		}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int64Builder).Append(20)
		builder.Field(1).(*array.StringBuilder).Append("bad-ts")
		arrowRecord := builder.NewRecord()
		record := storage.NewSimpleArrowRecord(arrowRecord, map[storage.FieldID]int{
			100:                   0,
			common.TimeStampField: 1,
		})
		defer record.Release()

		err := appendMilvusTableSourcePKOffsetsFromRecord(
			record,
			&schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64},
			packed.Fragment{StartRow: 0, EndRow: 1},
			0,
			0,
			map[string]struct{}{"i:20": {}},
			map[string][]milvusTableSourcePKOffset{},
		)

		s.Error(err)
		s.Contains(err.Error(), "timestamp column has unexpected type")
	})

	s.Run("unsupported primary key type", func() {
		record, _, _, err := storage.BuildDeleteRecord(
			[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20)},
			[]storage.Timestamp{100},
		)
		s.Require().NoError(err)
		defer record.Release()

		err = appendMilvusTableSourcePKOffsetsFromRecord(
			record,
			&schemapb.FieldSchema{FieldID: 0, DataType: schemapb.DataType_None},
			packed.Fragment{StartRow: 0, EndRow: 1},
			0,
			0,
			map[string]struct{}{"i:20": {}},
			map[string][]milvusTableSourcePKOffset{},
		)

		s.Error(err)
		s.Contains(err.Error(), "is unsupported")
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestAppendMilvusTableSourceDeleteEventsFromRecord() {
	record, _, _, err := storage.BuildDeleteRecord(
		[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20), storage.NewInt64PrimaryKey(30)},
		[]storage.Timestamp{100, 200},
	)
	s.Require().NoError(err)
	defer record.Release()

	deletedSourcePKKeys := map[string]struct{}{"i:20": {}}
	var events []milvusTableSourceDeleteEvent
	err = appendMilvusTableSourceDeleteEventsFromRecord(
		record,
		schemapb.DataType_Int64,
		deletedSourcePKKeys,
		&events,
	)

	s.NoError(err)
	s.Equal(map[string]struct{}{
		"i:20": {},
		"i:30": {},
	}, deletedSourcePKKeys)
	s.Equal([]milvusTableSourceDeleteEvent{
		{sourcePKKey: "i:20", deleteTimestamp: 100},
		{sourcePKKey: "i:30", deleteTimestamp: 200},
	}, events)
}

func (s *RefreshExternalCollectionTaskSuite) TestAppendMilvusTableSourceDeleteEventsFromRecordBadTimestamp() {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "1", Type: arrow.BinaryTypes.String},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(20)
	builder.Field(1).(*array.StringBuilder).Append("bad-ts")
	arrowRecord := builder.NewRecord()
	record := storage.NewSimpleArrowRecord(arrowRecord, map[storage.FieldID]int{
		0:                     0,
		common.TimeStampField: 1,
	})
	defer record.Release()

	var events []milvusTableSourceDeleteEvent
	err := appendMilvusTableSourceDeleteEventsFromRecord(
		record,
		schemapb.DataType_Int64,
		map[string]struct{}{},
		&events,
	)

	s.Error(err)
	s.Contains(err.Error(), "deltalog timestamp column has unexpected type")
	s.Empty(events)
}

func (s *RefreshExternalCollectionTaskSuite) TestLoadMilvusTableSourceDeltalogDeletesEmptyAndMissingLogID() {
	task := &RefreshExternalCollectionTask{}

	deletes, keys, err := task.loadMilvusTableSourceDeltalogDeletes(
		context.Background(),
		nil,
		schemapb.DataType_Int64,
	)
	s.NoError(err)
	s.Empty(deletes)
	s.Empty(keys)

	deletes, keys, err = task.loadMilvusTableSourceDeltalogDeletes(
		context.Background(),
		[]milvusTableDeltalogRef{{sourcePath: "source/_delta/0"}},
		schemapb.DataType_Int64,
	)
	s.Error(err)
	s.Nil(deletes)
	s.Nil(keys)
	s.Contains(err.Error(), "has no allocated log ID")
}

func (s *RefreshExternalCollectionTaskSuite) TestGetMilvusTableSourcePKFieldCachesSnapshotMetadata() {
	ctx := context.Background()
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		ExternalSource: "s3://source-bucket/snapshots/10/metadata/20.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	})
	sourcePKField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "source_id",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	readCalls := 0
	mockMetadata := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
		To(func(
			externalSource string,
			externalSpec string,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (*datapb.SnapshotMetadata, error) {
			readCalls++
			return &datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{
					Schema: &schemapb.CollectionSchema{
						Fields: []*schemapb.FieldSchema{sourcePKField},
					},
				},
			}, nil
		}).Build()
	defer mockMetadata.UnPatch()

	firstField, err := task.getMilvusTableSourcePKField()
	s.Require().NoError(err)
	secondField, err := task.getMilvusTableSourcePKField()
	s.Require().NoError(err)

	s.True(firstField == secondField)
	s.Equal(sourcePKField, firstField)
	s.Equal(1, readCalls)
}

func (s *RefreshExternalCollectionTaskSuite) TestLoadMilvusTableSourceDeltalogDeletesReaderBranches() {
	task := NewRefreshExternalCollectionTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		ExternalSource: "s3://source-bucket/snapshots/10/metadata/20.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	})
	ref := milvusTableDeltalogRef{sourcePath: "source/_delta/1", logID: 1, numEntries: 2}

	s.Run("success", func() {
		record, _, _, err := storage.BuildDeleteRecord(
			[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20), storage.NewInt64PrimaryKey(20)},
			[]storage.Timestamp{100, 101},
		)
		s.Require().NoError(err)
		reader := &fakeMilvusTableDeltalogReader{records: []storage.Record{record}}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			To(func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
				s.Equal(schemapb.DataType_Int64, pkType)
				s.Equal([]string{ref.sourcePath}, paths)
				return reader, nil
			}).Build()
		defer mockReader.UnPatch()

		deletes, keys, err := task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_Int64,
		)

		s.NoError(err)
		s.Equal(map[string]struct{}{"i:20": {}}, keys)
		s.Require().Len(deletes, 1)
		s.Equal(ref, deletes[0].ref)
		s.Equal([]milvusTableSourceDeleteEvent{
			{sourcePKKey: "i:20", deleteTimestamp: 100},
			{sourcePKKey: "i:20", deleteTimestamp: 101},
		}, deletes[0].events)
	})

	s.Run("legacy_l0_success", func() {
		legacyRef := milvusTableDeltalogRef{sourcePath: "files/delta_log/1/2/3/10", logID: 10, numEntries: 2}
		reader := &fakeMilvusTableDeltalogReader{}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			To(func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
				s.Equal(schemapb.DataType_Int64, pkType)
				s.Equal([]string{legacyRef.sourcePath}, paths)
				return reader, nil
			}).Build()
		defer mockReader.UnPatch()

		deletes, keys, err := task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{legacyRef},
			schemapb.DataType_Int64,
		)

		s.NoError(err)
		s.Empty(keys)
		s.Require().Len(deletes, 1)
		s.Equal(legacyRef, deletes[0].ref)
		s.Empty(deletes[0].events)
	})

	s.Run("open error", func() {
		expectedErr := fmt.Errorf("open failed")
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			Return(nil, expectedErr).Build()
		defer mockReader.UnPatch()

		_, _, err := task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_Int64,
		)

		s.ErrorIs(err, expectedErr)
	})

	s.Run("next error", func() {
		expectedErr := fmt.Errorf("next failed")
		reader := &fakeMilvusTableDeltalogReader{nextErr: expectedErr}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			Return(reader, nil).Build()
		defer mockReader.UnPatch()

		_, _, err := task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_Int64,
		)

		s.ErrorIs(err, expectedErr)
	})

	s.Run("append error closes reader", func() {
		record, _, _, err := storage.BuildDeleteRecord(
			[]storage.PrimaryKey{storage.NewInt64PrimaryKey(20)},
			[]storage.Timestamp{100},
		)
		s.Require().NoError(err)
		reader := &fakeMilvusTableDeltalogReader{records: []storage.Record{record}}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			Return(reader, nil).Build()
		defer mockReader.UnPatch()

		_, _, err = task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_None,
		)

		s.Error(err)
		s.Contains(err.Error(), "is unsupported")
	})

	s.Run("close error", func() {
		expectedErr := fmt.Errorf("close failed")
		reader := &fakeMilvusTableDeltalogReader{closeErr: expectedErr}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			Return(reader, nil).Build()
		defer mockReader.UnPatch()

		_, _, err := task.loadMilvusTableSourceDeltalogDeletes(
			context.Background(),
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_Int64,
		)

		s.ErrorIs(err, expectedErr)
	})

	s.Run("context canceled", func() {
		reader := &fakeMilvusTableDeltalogReader{}
		mockReader := mockey.Mock(storage.NewDeltalogReader).
			Return(reader, nil).Build()
		defer mockReader.UnPatch()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := task.loadMilvusTableSourceDeltalogDeletes(
			ctx,
			[]milvusTableDeltalogRef{ref},
			schemapb.DataType_Int64,
		)

		s.ErrorIs(err, context.Canceled)
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestBuildMilvusTableVirtualPKDeletesFiltersOffsets() {
	pks, timestamps := buildMilvusTableVirtualPKDeletes(
		3000,
		map[string][]milvusTableSourcePKOffset{
			"i:20": {
				{targetOffset: 1, insertTimestamp: 99},
				{targetOffset: 2, insertTimestamp: 100},
			},
			"i:30": {
				{targetOffset: 3, insertTimestamp: 200},
			},
		},
		[]milvusTableSourceDeleteEvent{
			{sourcePKKey: "i:unknown", deleteTimestamp: 100},
			{sourcePKKey: "i:20", deleteTimestamp: 100},
			{sourcePKKey: "i:30", deleteTimestamp: 100},
		},
	)

	s.Require().Len(pks, 1)
	s.Equal(testVirtualPK(3000, 1), pks[0].GetValue())
	s.Equal([]storage.Timestamp{100}, timestamps)
}

func createSourcePKManifest(t *testing.T, basePath string, storageConfig *indexpb.StorageConfig, pks []int64) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "100",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "1",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"1"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{{
		Columns: []int{0, 1},
		GroupID: storagecommon.DefaultShortColumnGroupID,
	}}
	writer, err := packed.NewFFIPackedWriter(basePath, schema, columnGroups, storageConfig, nil)
	if err != nil {
		t.Fatalf("create source manifest writer: %v", err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, pk := range pks {
		builder.Field(0).(*array.Int64Builder).Append(pk)
		builder.Field(1).(*array.Int64Builder).Append(int64(i + 1))
	}
	record := builder.NewRecord()
	defer record.Release()
	if err := writer.WriteRecordBatch(record); err != nil {
		t.Fatalf("write source manifest record: %v", err)
	}
	output, err := writer.Close()
	if err != nil {
		t.Fatalf("close source manifest writer: %v", err)
	}
	defer output.Destroy()

	manifestPath, err := packed.CommitManifestUpdates(basePath, packed.ManifestEarliest, storageConfig, &packed.ManifestUpdates{
		NewFiles: output,
	})
	if err != nil {
		t.Fatalf("commit source manifest: %v", err)
	}
	return manifestPath
}

func writeDeltalog(
	t *testing.T,
	storageConfig *indexpb.StorageConfig,
	path string,
	pkType schemapb.DataType,
	pks []storage.PrimaryKey,
	tss []storage.Timestamp,
) {
	t.Helper()
	writer, err := storage.NewDeltalogWriter(
		context.Background(),
		10,
		20,
		30,
		9001,
		pkType,
		path,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
	)
	if err != nil {
		t.Fatalf("create deltalog writer: %v", err)
	}
	record, _, _, err := storage.BuildDeleteRecord(pks, tss)
	if err != nil {
		t.Fatalf("build delete record: %v", err)
	}
	defer record.Release()
	if err := writer.Write(record); err != nil {
		t.Fatalf("write deltalog: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close deltalog writer: %v", err)
	}
}

func readInt64Deltalog(t *testing.T, storageConfig *indexpb.StorageConfig, path string) ([]int64, []int64) {
	t.Helper()
	reader, err := storage.NewDeltalogReader(
		schemapb.DataType_Int64,
		[]string{path},
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
	)
	if err != nil {
		t.Fatalf("create deltalog reader: %v", err)
	}
	defer reader.Close()
	var pks []int64
	var tss []int64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read deltalog: %v", err)
		}
		func() {
			defer record.Release()
			pkColumn := record.Column(0).(*array.Int64)
			tsColumn := record.Column(common.TimeStampField).(*array.Int64)
			for i := 0; i < record.Len(); i++ {
				pks = append(pks, pkColumn.Value(i))
				tss = append(tss, tsColumn.Value(i))
			}
		}()
	}
	return pks, tss
}

func testVirtualPK(segmentID int64, offset int64) int64 {
	return ((segmentID & 0xFFFFFFFF) << 32) | (offset & 0xFFFFFFFF)
}
