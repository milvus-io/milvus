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

//go:build test

package bulkimport

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newCapturingImportMeta builds an importMeta whose catalog records every proto
// handed to SaveImportJob, so tests can assert what was actually persisted.
func NewCapturingImportMeta(t *testing.T, saved *[]*datapb.ImportJob) ImportMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, job *datapb.ImportJob) error {
			*saved = append(*saved, job)
			return nil
		}).Maybe()
	alloc := allocator.NewMockAllocator(t)
	importMeta, err := NewImportMeta(context.Background(), catalog, alloc, nil)
	require.NoError(t, err)
	return importMeta
}

func PatchSnapshotImportInstance(t *testing.T) {
	t.Helper()
	patch := mockey.Mock(snapshotstorage.InstanceConfigFromParamtable).To(func(*paramtable.ComponentParam) *objectstorage.Config {
		return &objectstorage.Config{Address: "localhost:9000", BucketName: "source", CloudProvider: "aws"}
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func SnapshotImportTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "target",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "1024"},
				},
			},
		},
	}
}

func SnapshotImportTestData(layout datapb.SnapshotLayout) *snapshotstorage.SnapshotData {
	sourceSchema := proto.Clone(SnapshotImportTestSchema()).(*schemapb.CollectionSchema)
	sourceSchema.Name = "source"
	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1},
		Collection: &datapb.CollectionDescription{
			Schema:     sourceSchema,
			Partitions: map[string]int64{"source_partition": 10},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      20,
				PartitionId:    10,
				SegmentLevel:   datapb.SegmentLevel_L1,
				StorageVersion: milvusstorage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath("root/files/segment/20", 8),
			},
			{
				SegmentId:      10,
				PartitionId:    10,
				SegmentLevel:   datapb.SegmentLevel_L1,
				StorageVersion: milvusstorage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath("root/files/segment/10", 7),
			},
		},
		Layout: layout,
	}
}

func SnapshotImportTestOptions() importutilv2.Options {
	return importutilv2.Options{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot},
	}
}

func SnapshotImportTestEZK(ezID int64) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"ez_id":%d}`, ezID)))
}
