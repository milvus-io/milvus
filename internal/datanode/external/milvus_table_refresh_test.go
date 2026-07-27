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

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_MilvusTableL0RefreshUpdatesExistingSegmentManifest() {
	ctx := context.Background()
	partitionID := int64(2000)
	oldManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)
	newManifest := packed.MarshalManifestPath("files/insert_log/1000/2000/1", 2)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   partitionID,
		TaskID:        s.taskID,
		ExternalSpec:  `{"format":"milvus-table"}`,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             1,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      1000,
			ManifestPath:   oldManifest,
			StorageVersion: storage.StorageV3,
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

	var gotSegmentID int64
	var gotFragments []packed.Fragment
	mockCreate := mockey.Mock(mockey.GetMethod(task, "createManifestForSegment")).
		To(func(ctx context.Context, segmentID int64, fragments []packed.Fragment) (string, error) {
			gotSegmentID = segmentID
			gotFragments = fragments
			return newManifest, nil
		}).Build()
	defer mockCreate.UnPatch()
	mockSourceDeltas := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return(nil, nil).Build()
	defer mockSourceDeltas.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.Empty(task.GetKeptSegmentIDs())
	updated := task.GetUpdatedSegments()
	s.Require().Len(updated, 1)
	s.Equal(int64(1), updated[0].GetID())
	s.Equal(newManifest, updated[0].GetManifestPath())
	s.Equal(updated, result)
	s.Equal(int64(1), gotSegmentID)
	s.Equal(newFragments, gotFragments)
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

	mockCreate := mockey.Mock(mockey.GetMethod(task, "createManifestForSegment")).
		Return(refreshedManifest, nil).Build()
	defer mockCreate.UnPatch()
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
