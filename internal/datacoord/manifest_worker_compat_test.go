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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the real worker result through the complete-manifest receiver retained
// for the 3.0 protocol, including catalog publication and an idempotent result replay.
func TestLegacyManifestWorkerResultAdoption(t *testing.T) {
	ctx := context.Background()
	pt := paramtable.Get()
	for key, value := range map[string]string{
		pt.CommonCfg.StorageType.Key: "local",
		pt.CommonCfg.UseLoonFFI.Key:  "false",
		pt.LocalStorageCfg.Path.Key:  t.TempDir(),
	} {
		old, err := paramtable.GetBaseTable().Load(key)
		require.NoError(t, err)
		require.NoError(t, pt.Save(key, value))
		t.Cleanup(func() { _ = pt.Save(key, old) })
	}
	initcore.InitStorageV2FileSystem(pt)
	t.Cleanup(initcore.CleanArrowFileSystem)
	params := compaction.GenParams()
	params.StorageVersion = storage.StorageV3
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	io := mock_util.NewMockBinlogIO(t)
	io.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	alloc := compactor.NewCompactionAllocator(allocator.NewLocalAllocator(100, 200), allocator.NewLocalAllocator(1000, 2000))
	writer, err := compactor.NewMultiSegmentWriter(ctx, io, alloc, 64*1024*1024,
		schema, params, 100, 10, 1, "compat-channel", 1024,
		storage.WithStorageConfig(params.StorageConfig), storage.WithVersion(storage.StorageV3))
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		require.NoError(t, writer.WriteValue(&storage.Value{
			PK: storage.NewInt64PrimaryKey(i), Timestamp: 1,
			Value: map[int64]any{common.RowIDField: i, common.TimeStampField: int64(1), 100: i},
		}))
	}
	require.NoError(t, writer.Close())
	require.Len(t, writer.GetCompactionSegments(), 1)
	source := writer.GetCompactionSegments()[0]
	targetSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	targetSchema.Version = 2
	targetSchema.Fields = append(targetSchema.Fields, &schemapb.FieldSchema{
		FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	jsonParams, err := compaction.GenerateJSONParams(targetSchema)
	require.NoError(t, err)
	// No capability field: this is the legacy request contract.
	plan := &datapb.CompactionPlan{
		PlanID: 7001, Type: datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema: targetSchema, TotalRows: 3, MaxSize: 64 * 1024 * 1024, JsonParams: jsonParams,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 300, End: 400},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 3000, End: 4000},
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID: source.GetSegmentID(), CollectionID: 1, PartitionID: 10,
			InsertChannel: "compat-channel", StorageVersion: storage.StorageV3,
			Manifest: source.GetManifest(), FieldBinlogs: source.GetInsertLogs(),
		}},
	}
	payload, err := proto.Marshal(plan)
	require.NoError(t, err)
	receivedPlan := &datapb.CompactionPlan{}
	require.NoError(t, proto.Unmarshal(payload, receivedPlan))
	require.False(t, receivedPlan.GetEnableManifestDelta())
	worker := compactor.NewBumpSchemaVersionCompactionTask(ctx,
		storage.NewLocalChunkManager(objectstorage.RootPath(params.StorageConfig.GetRootPath())), receivedPlan, params)
	result, err := worker.Compact()
	require.NoError(t, err)
	require.Len(t, result.GetSegments(), 1)
	require.Nil(t, result.GetSegments()[0].GetManifestDelta())
	require.NotEmpty(t, result.GetSegments()[0].GetManifest())

	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: source.GetSegmentID(), CollectionID: 1, PartitionID: 10,
		State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: source.GetManifest(), SchemaVersion: 1, NumOfRows: 3,
	})))
	task := newBumpSchemaVersionTask(context.TODO(), &datapb.CompactionTask{
		PlanID: plan.GetPlanID(), Type: plan.GetType(), CollectionID: 1, PartitionID: 10,
		Schema: targetSchema, InputSegments: []int64{source.GetSegmentID()},
	}, nil, mt, nil)
	require.NoError(t, task.saveSegmentMeta(result))
	published := proto.Clone(mt.GetSegment(ctx, source.GetSegmentID()).SegmentInfo)
	require.Equal(t, int32(2), mt.GetSegment(ctx, source.GetSegmentID()).GetSchemaVersion())
	fields, err := packed.GetManifestFieldIDs(mt.GetSegment(ctx, source.GetSegmentID()).GetManifestPath(), params.StorageConfig)
	require.NoError(t, err)
	require.Contains(t, fields, int64(100))
	require.Contains(t, fields, int64(101))
	require.NoError(t, task.saveSegmentMeta(result))
	require.True(t, proto.Equal(published, mt.GetSegment(ctx, source.GetSegmentID()).SegmentInfo),
		"replaying a complete manifest must not double-count materialization stats")
}
