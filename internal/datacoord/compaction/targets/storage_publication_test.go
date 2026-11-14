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

package targets

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	logallocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestStorageStandingReconcilesRealMixOutput(t *testing.T) {
	for _, test := range []struct {
		name          string
		beforeVersion int64
		beforeFormat  string
		afterVersion  int64
		afterFormat   string
	}{
		{name: "upgrade old V1", beforeVersion: storage.StorageV1, afterVersion: storage.StorageV3, afterFormat: "parquet"},
		{name: "rewrite old V3 format", beforeVersion: storage.StorageV3, beforeFormat: "parquet", afterVersion: storage.StorageV3, afterFormat: "vortex"},
		{name: "downgrade V3", beforeVersion: storage.StorageV3, beforeFormat: "vortex", afterVersion: storage.StorageV2, afterFormat: "parquet"},
	} {
		t.Run(test.name, func(t *testing.T) {
			enableCompactionTargetReconciler(t)
			params := paramtable.Get()
			setStorageParam := func(key, value string) {
				t.Helper()
				require.NoError(t, params.Save(key, value))
				t.Cleanup(func() { params.Reset(key) })
			}
			root := t.TempDir()
			setStorageParam(params.CommonCfg.StorageType.Key, "local")
			setStorageParam(params.LocalStorageCfg.Path.Key, root)
			setStorageParam(params.CommonCfg.UseLoonFFI.Key, strconv.FormatBool(test.afterVersion == storage.StorageV3))
			setStorageParam(params.DataNodeCfg.StorageFormat.Key, test.afterFormat)
			setStorageParam(params.DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
			setStorageParam(params.DataCoordCfg.StorageFormatCompactionEnabled.Key, "true")
			setStorageParam(params.DataCoordCfg.EnableAutoCompaction.Key, "true")
			setStorageParam(params.DataCoordCfg.EnableSortCompaction.Key, "false")
			setStorageParam(params.DataCoordCfg.IndexBasedCompaction.Key, "false")
			require.NoError(t, initcore.InitStorageV2FileSystem(params))

			ctx := t.Context()
			mt, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			chunks := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			schema := &schemapb.CollectionSchema{
				Name: "storage_target",
				Fields: []*schemapb.FieldSchema{
					{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			}
			mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: schema})
			segment := writeStorageCompactionInput(t, chunks, schema, test.beforeVersion, test.beforeFormat)
			require.NoError(t, mt.AddSegment(ctx, segment))

			manager := NewLoadedCompactionTargetMeta(t, ctx)
			alloc := meta.NewMockAllocator(t)
			versionManager := index.NewMockVersionManager(t)
			absent := index.IndexVersionCondition{Mode: index.IndexVersionAbsent}
			versionManager.EXPECT().ResolveIndexVersionConditions(mock.Anything).Return(absent, absent).Maybe()
			versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("3.0.0")).Maybe()
			versionManager.EXPECT().ResolveScalarIndexVersion().Return(0).Once()
			handler := meta.NewMockHandlerWithMeta(mt)
			reconciler := NewCompactionTargetReconciler(mt, &index.IndexMeta{}, manager, handler, alloc, func(context.Context, int64) (bool, error) {
				return true, nil
			})
			reconciler.SetIndexVersionManager(versionManager)
			events, err := reconciler.Reconcile(ctx)
			require.NoError(t, err)
			plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
			require.Len(t, plans, 1)
			task := plans[0]
			require.Equal(t, datapb.CompactionType_MixCompaction, task.GetType())
			require.Equal(t, []int64{segment.GetID()}, task.GetInputSegments())

			task.PlanID = 3000
			task.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 5000, End: 5010}
			taskMeta, err := ctask.NewCompactionTaskMeta(ctx, mt.Catalog)
			require.NoError(t, err)
			mixTask := ctask.NewMixCompactionTask(task, alloc, mt, taskMeta, versionManager)
			plan, err := mixTask.BuildCompactionRequest()
			require.NoError(t, err)
			workerParams, err := compaction.ParseParamsFromJSON(plan.GetJsonParams())
			require.NoError(t, err)
			worker := compactor.NewMixCompactionTask(ctx, io.NewBinlogIO(chunks), chunks, plan, workerParams, []int64{100})
			result, err := worker.Compact()
			require.NoError(t, err)
			// DataCoord receives a decoded worker RPC, including non-nil empty
			// messages where the worker's in-memory result has nil elements.
			payload, err := proto.Marshal(result)
			require.NoError(t, err)
			var decoded datapb.CompactionPlanResult
			require.NoError(t, proto.Unmarshal(payload, &decoded))
			require.NoError(t, mixTask.SaveSegmentMeta(&decoded))
			require.Len(t, mixTask.GetTaskProto().GetResultSegments(), 1)
			output := mt.GetHealthySegment(ctx, mixTask.GetTaskProto().GetResultSegments()[0])
			require.NotNil(t, output)
			require.Equal(t, test.afterVersion, output.GetStorageVersion())
			require.Equal(t, datapb.SegmentLevel_L1, output.GetLevel())
			require.Equal(t, segment.GetNumOfRows(), output.GetNumOfRows())
			require.Equal(t, []int64{segment.GetID()}, output.GetCompactionFrom())
			if test.afterVersion == storage.StorageV3 {
				formats, err := mt.GetManifestInsertFormats(ctx, output, meta.CreateStorageConfig())
				require.NoError(t, err)
				require.Equal(t, []string{test.afterFormat}, formats)
			} else {
				require.Empty(t, output.GetManifestPath())
				require.NotEmpty(t, output.GetBinlogs())
			}

			events, err = reconciler.Reconcile(ctx)
			require.NoError(t, err)
			require.Empty(t, events[policy.TriggerTypeSingle])
			version := storageTestRecord(t, manager, datapb.TargetIntent_INTENT_STORAGE_VERSION)
			rows, err := ObserveStorage(ctx, mt, handler, version)
			require.NoError(t, err)
			require.Zero(t, rows)
			format := storageTestRecord(t, manager, datapb.TargetIntent_INTENT_STORAGE_FORMAT)
			if test.afterVersion == storage.StorageV3 {
				rows, err := ObserveStorage(ctx, mt, handler, format)
				require.NoError(t, err)
				require.Zero(t, rows)
			} else {
				require.False(t, format.GetEnabled())
				require.Empty(t, format.GetProperties())
			}
		})
	}
}

func storageTestRecord(t *testing.T, targetMeta *CompactionTargetMeta, intent datapb.TargetIntent) *datapb.CompactionTarget {
	t.Helper()
	var found *datapb.CompactionTarget
	for _, record := range targetMeta.GetCompactionTargets() {
		if record.GetIntent() == intent {
			require.Nil(t, found)
			found = record
		}
	}
	require.NotNil(t, found)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, found.GetState())
	require.Zero(t, found.GetCollectionID())
	return found
}

func writeStorageCompactionInput(t *testing.T, chunks storage.ChunkManager, schema *schemapb.CollectionSchema, version int64, format string) *meta.SegmentInfo {
	t.Helper()
	ctx := t.Context()
	ts := tsoutil.ComposeTSByTime(time.Now())
	record, err := storage.ValueSerializer([]*storage.Value{{Value: map[storage.FieldID]any{
		common.RowIDField: int64(1), common.TimeStampField: int64(ts), 100: int64(42),
	}}}, schema)
	require.NoError(t, err)
	defer record.Release()
	writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, 1000, schema, logallocator.NewLocalAllocator(9000, 10_000), 1<<20, 1,
		storage.WithVersion(version), storage.WithStorageConfig(meta.CreateStorageConfig()), storage.WithWriterFormat(format),
		storage.WithUploader(chunks.MultiWrite))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, stats, _, manifest, _ := writer.GetLogs()
	input := &datapb.SegmentInfo{
		ID: 1000, CollectionID: 1, PartitionID: 10, InsertChannel: "ch",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2,
		StorageVersion: version, NumOfRows: 1, ManifestPath: manifest,
		Binlogs: storage.SortFieldBinlogs(logs), StartPosition: &msgpb.MsgPosition{Timestamp: ts}, DmlPosition: &msgpb.MsgPosition{Timestamp: ts},
	}
	if version == storage.StorageV3 {
		// Modern catalog records contain no legacy arrays for manifest-backed data.
		input.Stats = storage.BuildStatsFromFieldBinlogs(input.Binlogs, nil, nil, nil)
		input.Binlogs = nil
	} else {
		input.Statslogs = []*datapb.FieldBinlog{stats}
		require.NoError(t, binlog.CompressBinLogs(input.Binlogs, input.Statslogs))
	}
	return meta.NewSegmentInfo(input)
}
