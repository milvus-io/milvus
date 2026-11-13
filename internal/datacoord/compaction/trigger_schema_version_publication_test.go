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

package compaction

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	logallocator "github.com/milvus-io/milvus/internal/allocator"
	workercompaction "github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestStandingSchemaVersionPublishesRealV3Results(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	for key, value := range map[string]string{
		Params.CommonCfg.StorageType.Key:                               "local",
		Params.CommonCfg.UseLoonFFI.Key:                                "false",
		Params.LocalStorageCfg.Path.Key:                                root,
		Params.DataNodeCfg.StorageFormat.Key:                           "parquet",
		Params.DataCoordCfg.EnableAutoCompaction.Key:                   "true",
		Params.DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key:     "true",
		Params.DataCoordCfg.StorageVersionCompactionEnabled.Key:        "false",
		Params.DataCoordCfg.StorageFormatCompactionEnabled.Key:         "false",
		Params.DataCoordCfg.EnableSortCompaction.Key:                   "false",
		Params.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key: "1",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	require.NoError(t, initcore.InitStorageV2FileSystem(Params))
	t.Cleanup(initcore.CleanArrowFileSystem)
	chunks := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	config := embed.NewConfig()
	config.Dir = t.TempDir()
	config.LogLevel = "error"
	loopback := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	config.ListenClientUrls = []url.URL{loopback}
	config.ListenPeerUrls = []url.URL{loopback}
	config.AdvertisePeerUrls = []url.URL{loopback}
	config.InitialCluster = config.InitialClusterFromName(config.Name)
	kv, err := etcdkv.NewEmbededEtcdKV(config, "test")
	require.NoError(t, err)
	t.Cleanup(kv.Close)
	catalog := kvdatacoord.NewCatalog(kv, "", "")
	mt, err := meta.NewMeta(ctx, catalog, chunks, meta.NewMetaStoreWithCollections(catalog), broker.NewMockBroker(t), meta.NoDomainMetas)
	require.NoError(t, err)
	t.Cleanup(mt.CloseManifestExecutors)
	manager, err := targets.RecoverCompactionTargetMeta(ctx, mt.Catalog)
	require.NoError(t, err)
	tasks, err := ctask.NewCompactionTaskMeta(ctx, mt.Catalog)
	require.NoError(t, err)
	alloc := meta.NewMockAllocator(t)
	versions := index.NewTestVersionManager()
	handler := meta.NewMockHandlerWithMeta(mt)
	inspector := NewCompactionInspector(mt, tasks, nil, nil, nil, manager, nil, alloc, handler, nil, nil, versions)
	require.NoError(t, inspector.InitAdmission())
	triggers := NewCompactionTriggerManager(alloc, handler, inspector, mt, nil, tasks, nil, manager, versions)

	const (
		collectionID = 1
		partitionID  = 10
		inputID      = 1000
		originalTS   = 5
	)
	base := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "obsolete", DataType: schemapb.DataType_Int64},
	}}
	setSchema := func(schema *schemapb.CollectionSchema, ttl string) {
		mt.AddCollection(&meta.CollectionInfo{
			ID: collectionID, Schema: schema, Properties: map[string]string{common.CollectionTTLConfigKey: ttl},
		})
	}
	const year = "31536000"
	setSchema(base, year)
	oldTS := tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	newTS := tsoutil.ComposeTSByTime(time.Now().Add(-5 * time.Minute))
	record, err := storage.ValueSerializer([]*storage.Value{
		{Value: map[storage.FieldID]any{common.RowIDField: int64(1), common.TimeStampField: int64(oldTS), 100: int64(42), 101: int64(7)}},
		{Value: map[storage.FieldID]any{common.RowIDField: int64(2), common.TimeStampField: int64(newTS), 100: int64(43), 101: int64(8)}},
	}, base)
	require.NoError(t, err)
	defer record.Release()
	storageConfig := meta.CreateStorageConfig()
	writer, err := storage.NewBinlogRecordWriter(ctx, collectionID, partitionID, inputID, base,
		logallocator.NewLocalAllocator(9000, 10_000), 1<<20, 2,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(storageConfig), storage.WithWriterFormat("parquet"),
		storage.WithUploader(chunks.MultiWrite))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()
	require.NotEmpty(t, manifest)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: inputID, CollectionID: collectionID, PartitionID: partitionID, InsertChannel: "ch",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2, LastLevel: datapb.SegmentLevel_L1,
		StorageVersion: storage.StorageV3, NumOfRows: 2, ManifestPath: manifest, CreateTs: originalTS,
		IsSorted: true, IsSortedByNamespace: true, SchemaVersion: 1,
		Stats:         storage.BuildStatsFromFieldBinlogs(storage.SortFieldBinlogs(logs), nil, nil, nil),
		StartPosition: &msgpb.MsgPosition{Timestamp: oldTS}, DmlPosition: &msgpb.MsgPosition{Timestamp: newTS},
	})))

	queue := func(input int64, schemaVersion int32) *ctask.BumpSchemaVersionTask {
		t.Helper()
		triggers.handleTicker(ctx, TargetTicker)
		queued, err := inspector.queueTasks.Dequeue()
		require.NoError(t, err)
		require.Zero(t, inspector.queueTasks.Len())
		job, ok := queued.(*ctask.BumpSchemaVersionTask)
		require.True(t, ok)
		stored, err := mt.Catalog.ListCompactionTask(ctx)
		require.NoError(t, err)
		require.Contains(t, stored, job.GetTaskProto())
		require.Equal(t, []int64{input}, job.GetTaskProto().GetInputSegments())
		require.Equal(t, schemaVersion, job.GetTaskProto().GetSchema().GetVersion())
		require.Greater(t, job.GetTaskProto().GetCreateTs(), uint64(originalTS))
		require.Zero(t, job.GetTaskProto().GetOutputSegmentCountHint())
		return job
	}
	compact := func(job *ctask.BumpSchemaVersionTask) *datapb.CompactionSegment {
		t.Helper()
		plan, err := job.BuildCompactionRequest()
		require.NoError(t, err)
		require.True(t, proto.Equal(job.GetTaskProto().GetSchema(), plan.GetSchema()))
		params, err := workercompaction.ParseParamsFromJSON(plan.GetJsonParams())
		require.NoError(t, err)
		require.EqualValues(t, storage.StorageV2, params.StorageVersion)
		worker := compactor.NewBumpSchemaVersionCompactionTask(ctx, chunks, plan, params)
		result, err := worker.Compact()
		require.NoError(t, err)
		require.Len(t, result.GetSegments(), 1)
		payload, err := proto.Marshal(result)
		require.NoError(t, err)
		decoded := &datapb.CompactionPlanResult{}
		require.NoError(t, proto.Unmarshal(payload, decoded))
		require.NoError(t, job.SaveSegmentMeta(decoded))
		require.True(t, job.Clean())
		return result.GetSegments()[0]
	}
	getPublished := func(id int64, schemaVersion int32) *meta.SegmentInfo {
		t.Helper()
		segment := mt.GetSegment(ctx, id)
		require.NotNil(t, segment)
		require.Equal(t, schemaVersion, segment.GetSchemaVersion())
		require.EqualValues(t, storage.StorageV3, segment.GetStorageVersion())
		require.Equal(t, datapb.SegmentLevel_L2, segment.GetLevel())
		require.Equal(t, datapb.SegmentLevel_L1, segment.GetLastLevel())
		require.True(t, segment.GetIsSorted())
		require.True(t, segment.GetIsSortedByNamespace())
		return segment
	}

	versionOnly := proto.CloneOf(base)
	versionOnly.Version = 2
	setSchema(versionOnly, "1")
	confirm := queue(inputID, 2)
	addColumn := proto.CloneOf(versionOnly)
	addColumn.Version = 3
	addColumn.Fields = append(addColumn.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
	setSchema(addColumn, "1")
	result := compact(confirm)
	require.EqualValues(t, inputID, result.GetSegmentID())
	require.Nil(t, result.GetManifestDelta())
	confirmed := getPublished(inputID, 2)
	require.Equal(t, manifest, confirmed.GetManifestPath())
	require.EqualValues(t, originalTS, confirmed.GetCreateTs())
	require.EqualValues(t, 2, confirmed.GetNumOfRows())

	result = compact(queue(inputID, 3))
	require.EqualValues(t, inputID, result.GetSegmentID())
	require.NotNil(t, result.GetManifestDelta())
	appended := getPublished(inputID, 3)
	require.EqualValues(t, originalTS, appended.GetCreateTs())
	require.EqualValues(t, 2, appended.GetNumOfRows())
	fields, err := packed.GetManifestFieldIDs(appended.GetManifestPath(), storageConfig)
	require.NoError(t, err)
	require.Contains(t, fields, int64(102))
	require.Contains(t, fields, int64(101))

	setSchema(addColumn, "1200")
	triggers.handleTicker(ctx, TargetTicker)
	require.Zero(t, inspector.queueTasks.Len(), "a same-version TTL change does not schedule a schema bump")
	dropColumn := proto.CloneOf(addColumn)
	dropColumn.Version = 4
	dropColumn.Fields = append(dropColumn.Fields[:3], dropColumn.Fields[4:]...)
	setSchema(dropColumn, "1200")
	rewrite := queue(inputID, 4)
	result = compact(rewrite)
	require.Equal(t, rewrite.GetTaskProto().GetPreAllocatedSegmentIDs().GetBegin(), result.GetSegmentID())
	require.NotEqualValues(t, inputID, result.GetSegmentID())
	rewritten := getPublished(result.GetSegmentID(), 4)
	require.Equal(t, rewrite.GetTaskProto().GetCreateTs(), rewritten.GetCreateTs())
	require.Equal(t, []int64{inputID}, rewritten.GetCompactionFrom())
	require.EqualValues(t, 1, rewritten.GetNumOfRows())
	fields, err = packed.GetManifestFieldIDs(rewritten.GetManifestPath(), storageConfig)
	require.NoError(t, err)
	require.Contains(t, fields, int64(102))
	require.NotContains(t, fields, int64(101))
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, inputID).GetState())

	last := proto.CloneOf(dropColumn)
	last.Version = 5
	last.Fields = last.Fields[:3]
	setSchema(last, "1")
	empty := queue(rewritten.GetID(), 5)
	result = compact(empty)
	require.Equal(t, empty.GetTaskProto().GetPreAllocatedSegmentIDs().GetBegin(), result.GetSegmentID())
	require.Zero(t, result.GetNumOfRows())
	filtered := getPublished(result.GetSegmentID(), 5)
	require.Equal(t, commonpb.SegmentState_Dropped, filtered.GetState())
	require.Equal(t, []int64{rewritten.GetID()}, filtered.GetCompactionFrom())
	require.Equal(t, empty.GetTaskProto().GetCreateTs(), filtered.GetCreateTs())
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, rewritten.GetID()).GetState())

	stored, err := mt.Catalog.ListSegments(ctx, collectionID)
	require.NoError(t, err)
	var persisted *datapb.SegmentInfo
	for _, segment := range stored {
		if segment.GetID() == filtered.GetID() {
			persisted = segment
		}
	}
	require.NotNil(t, persisted)
	require.Equal(t, commonpb.SegmentState_Dropped, persisted.GetState())
	require.EqualValues(t, 5, persisted.GetSchemaVersion())
	rows, err := targets.ObserveSchemaVersion(ctx, mt)
	require.NoError(t, err)
	require.Zero(t, rows)
}
