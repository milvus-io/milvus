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
	stdio "io"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestStandingCleanupPublishesMixRows(t *testing.T) {
	const pkFieldID, ttlFieldID = int64(100), int64(101)
	for _, tc := range []struct {
		name                       string
		level                      datapb.SegmentLevel
		ttlSeconds, toleranceHours int
		ages                       []time.Duration
		deletePKs                  []int64
		ttlField                   bool
		want                       []int64
	}{
		{name: "Delete Cleanup shares L2 rewrite with strict TTL", level: datapb.SegmentLevel_L2,
			ttlSeconds: 3600, toleranceHours: 0, ages: []time.Duration{2 * time.Hour, time.Minute, time.Minute, time.Minute, time.Minute},
			deletePKs: []int64{2, 2}, want: []int64{3, 4, 5}},
		{name: "ordinary TTL publishes an empty frontier with strict disabled", level: datapb.SegmentLevel_L1,
			ttlSeconds: 3600, toleranceHours: -1, ages: []time.Duration{2 * time.Hour, 2 * time.Hour, 2 * time.Hour},
			want: nil},
		{name: "entity TTL filters published expiration values", level: datapb.SegmentLevel_L1,
			toleranceHours: -1, ages: []time.Duration{time.Minute, time.Minute, time.Minute, time.Minute, time.Minute},
			ttlField: true, want: []int64{2, 3, 4, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableCompactionTargetReconciler(t)
			root := t.TempDir()
			for key, value := range map[string]string{
				Params.CommonCfg.StorageType.Key:                          "local",
				Params.LocalStorageCfg.Path.Key:                           root,
				Params.CommonCfg.UseLoonFFI.Key:                           "true",
				Params.DataNodeCfg.StorageFormat.Key:                      "parquet",
				Params.DataCoordCfg.EnableAutoCompaction.Key:              "true",
				Params.DataCoordCfg.EnableSortCompaction.Key:              "false",
				Params.DataCoordCfg.IndexBasedCompaction.Key:              "false",
				Params.DataCoordCfg.StorageVersionCompactionEnabled.Key:   "false",
				Params.DataCoordCfg.StorageFormatCompactionEnabled.Key:    "false",
				Params.DataCoordCfg.SingleCompactionRatioThreshold.Key:    "0.2",
				Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key:    "200",
				Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.Key:   "16777216",
				Params.DataCoordCfg.SingleCompactionExpiredLogMaxSize.Key: "10485760",
				Params.DataCoordCfg.CompactionExpiryTolerance.Key:         strconv.Itoa(tc.toleranceHours),
			} {
				setStorageTargetParam(t, key, value)
			}
			require.NoError(t, initcore.InitStorageV2FileSystem(Params))
			ctx := t.Context()
			mt, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			t.Cleanup(mt.CloseManifestExecutors)
			chunks := storage.NewLocalChunkManager(objectstorage.RootPath(root))

			schema := &schemapb.CollectionSchema{
				Name: "standing_cleanup",
				Fields: []*schemapb.FieldSchema{
					{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
					{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			}
			if tc.ttlField {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: ttlFieldID, Name: "expires_at", DataType: schemapb.DataType_Timestamptz})
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "expires_at"}}
			}
			properties := map[string]string{common.CollectionTTLConfigKey: strconv.Itoa(tc.ttlSeconds)}
			mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: schema, Properties: properties})

			now := time.Now()
			values := make([]*storage.Value, 0, len(tc.ages))
			for i, age := range tc.ages {
				pk := int64(i + 1)
				ts := int64(tsoutil.ComposeTSByTime(now.Add(-age)))
				row := map[storage.FieldID]any{common.RowIDField: pk, common.TimeStampField: ts, pkFieldID: pk}
				if tc.ttlField {
					expiry := now.Add(time.Hour)
					if i == 0 {
						expiry = now.Add(-time.Hour)
					}
					row[ttlFieldID] = expiry.UnixMicro()
				}
				values = append(values, &storage.Value{Value: row})
			}
			record, err := storage.ValueSerializer(values, schema)
			require.NoError(t, err)
			defer record.Release()
			const segmentID = 1000
			writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, segmentID, schema, logallocator.NewLocalAllocator(9000, 10_000), 1<<20,
				int64(len(values)), storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(meta.CreateStorageConfig()),
				storage.WithUploader(chunks.MultiWrite))
			require.NoError(t, err)
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())
			logs, statsLog, _, _, expiration := writer.GetLogs()
			inserts := storage.SortFieldBinlogs(logs)
			stats := []*datapb.FieldBinlog{statsLog}
			require.NoError(t, binlog.CompressBinLogs(inserts, stats))
			segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID: segmentID, CollectionID: 1, PartitionID: 10, InsertChannel: "ch",
				State: commonpb.SegmentState_Flushed, Level: tc.level, IsSorted: tc.level != datapb.SegmentLevel_L2,
				StorageVersion: storage.StorageV2, NumOfRows: int64(len(values)),
				Binlogs: inserts, Statslogs: stats, ExpirQuantiles: expiration,
			})
			require.NoError(t, mt.AddSegment(ctx, segment))
			if len(tc.deletePKs) != 0 {
				applyStandingCleanupDeletes(t, mt, chunks, root, segment, tc.deletePKs)
			}

			manager := NewLoadedCompactionTargetMeta(t, ctx)
			alloc := meta.NewMockAllocator(t)
			versions := index.NewIndexEngineVersionManager()
			r := NewCompactionTargetReconciler(mt, &index.IndexMeta{}, manager, meta.NewMockHandlerWithMeta(mt), alloc)
			events, err := r.Reconcile(ctx)
			require.NoError(t, err)
			plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
			require.Len(t, plans, 1)
			task := plans[0]
			require.Equal(t, []int64{segmentID}, task.GetInputSegments())
			require.Equal(t, int64(time.Duration(tc.ttlSeconds)*time.Second), task.GetCollectionTtl())
			for _, family := range []datapb.TargetIntent{datapb.TargetIntent_INTENT_DELETE_CLEANUP, datapb.TargetIntent_INTENT_TTL} {
				rows, err := ObserveCleanup(ctx, mt, storageTestRecord(t, manager, family))
				require.NoError(t, err)
				if family == datapb.TargetIntent_INTENT_TTL || len(tc.deletePKs) > 0 {
					require.Equal(t, int64(len(values)), rows)
				} else {
					require.Zero(t, rows)
				}
			}

			// A collection TTL update after planning must not change the
			// already selected task's row filter.
			mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: schema,
				Properties: map[string]string{common.CollectionTTLConfigKey: "86400"}})
			task.PlanID = 3000
			task.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 5000, End: 5010}
			taskMeta, err := ctask.NewCompactionTaskMeta(ctx, mt.Catalog)
			require.NoError(t, err)
			mix := ctask.NewMixCompactionTask(task, alloc, mt, taskMeta, versions)
			plan, err := mix.BuildCompactionRequest()
			require.NoError(t, err)
			params, err := compaction.ParseParamsFromJSON(plan.GetJsonParams())
			require.NoError(t, err)
			worker := compactor.NewMixCompactionTask(ctx, io.NewBinlogIO(chunks), chunks, plan, params, []int64{pkFieldID})
			result, err := worker.Compact()
			require.NoError(t, err)
			payload, err := proto.Marshal(result)
			require.NoError(t, err)
			var decoded datapb.CompactionPlanResult
			require.NoError(t, proto.Unmarshal(payload, &decoded))
			require.NoError(t, mix.SaveSegmentMeta(&decoded))
			require.Nil(t, mt.GetHealthySegment(ctx, segmentID))

			var got []int64
			for _, id := range mix.GetTaskProto().GetResultSegments() {
				output := mt.GetHealthySegment(ctx, id)
				if output == nil {
					continue
				}
				require.Equal(t, datapb.SegmentLevel_L1, output.GetLevel())
				require.Equal(t, []int64{segmentID}, output.GetCompactionFrom())
				require.NotEmpty(t, output.GetManifestPath())
				require.Zero(t, output.EnsureStats().GetDeleteNumRows())
				reader, err := storage.NewManifestRecordReader(ctx, output.GetManifestPath(), schema,
					storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(meta.CreateStorageConfig()))
				require.NoError(t, err)
				for {
					record, err := reader.Next()
					if err == stdio.EOF {
						break
					}
					require.NoError(t, err)
					column := record.Column(pkFieldID).(*array.Int64)
					got = append(got, column.Int64Values()...)
				}
				require.NoError(t, reader.Close())
			}
			require.ElementsMatch(t, tc.want, got)

			events, err = r.Reconcile(ctx)
			require.NoError(t, err)
			require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
			for _, family := range []datapb.TargetIntent{datapb.TargetIntent_INTENT_DELETE_CLEANUP, datapb.TargetIntent_INTENT_TTL} {
				rows, err := ObserveCleanup(ctx, mt, storageTestRecord(t, manager, family))
				require.NoError(t, err)
				require.Zero(t, rows)
			}
		})
	}
}

func applyStandingCleanupDeletes(t *testing.T, mt *meta.Meta, chunks storage.ChunkManager, root string, segment *meta.SegmentInfo, ids []int64) {
	t.Helper()
	ctx := t.Context()
	const logID int64 = 9100
	path := metautil.BuildDeltaLogPath(root, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), logID)
	writer, err := storage.NewDeltalogWriter(ctx, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), logID,
		schemapb.DataType_Int64, path, storage.WithVersion(storage.StorageV1), storage.WithUploader(chunks.MultiWrite))
	require.NoError(t, err)
	pks := make([]storage.PrimaryKey, 0, len(ids))
	timestamps := make([]uint64, 0, len(ids))
	for _, pk := range ids {
		pks = append(pks, storage.NewInt64PrimaryKey(pk))
		timestamps = append(timestamps, tsoutil.ComposeTSByTime(time.Now()))
	}
	record, from, to, err := storage.BuildDeleteRecord(pks, timestamps)
	require.NoError(t, err)
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	delta := &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{
		LogID: logID, EntriesNum: int64(len(ids)), TimestampFrom: from, TimestampTo: to, MemorySize: int64(writer.GetWrittenUncompressed()),
	}}}
	require.NoError(t, mt.UpdateSegmentsInfo(ctx, meta.AddL0DeltalogsOperator(segment.GetID(), []*datapb.FieldBinlog{delta})))
}
