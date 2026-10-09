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

package recovery

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Only the catalog and coordinator endpoints are replaced. Both the legacy
// packed file and the replayed tail use real StorageV2 writers and readers.
func TestLegacySchemaMigrationRestartsAndWritesStorageV2Field121(t *testing.T) {
	ctx := context.Background()
	catalog := &legacyMigrationCatalog{}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	rootPath := t.TempDir()
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(rootPath)
	pt := paramtable.Get()
	pt.Init(paramtable.NewBaseTable())
	settings := map[string]string{
		pt.CommonCfg.StorageType.Key: "local",
		pt.CommonCfg.UseLoonFFI.Key:  "false",
		pt.MinioCfg.RootPath.Key:     rootPath,
		pt.LocalStorageCfg.Path.Key:  rootPath,
	}
	for key, value := range settings {
		require.NoError(t, pt.Save(key, value))
	}
	t.Cleanup(func() {
		for key := range settings {
			_ = pt.Reset(key)
		}
		initcore.CleanArrowFileSystem()
	})
	storageConfig := &indexpb.StorageConfig{StorageType: "local", RootPath: rootPath}
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	channelMeta := legacySchemaMigrationVChannel()
	for _, version := range channelMeta.CollectionInfo.Schemas {
		version.Schema.Name = "legacy_schema_native_io"
		version.Schema.Fields = append([]*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		}, version.Schema.Fields...)
		for _, field := range version.Schema.Fields {
			if field.FieldID >= 121 {
				field.Nullable = true
			}
		}
	}
	encodingSchema := channelMeta.CollectionInfo.Schemas[1].Schema
	oldPath := filepath.Join(rootPath, "insert_log", "10", "20", "1", "0", "1")
	require.NoError(t, os.MkdirAll(filepath.Dir(oldPath), 0o755))
	oldGroup := storagecommon.ColumnGroup{
		GroupID: 0, Fields: []int64{0, 1, 100, 120, 121}, Columns: []int{0, 1, 2, 3, 4}, Format: "parquet",
	}
	oldWriter, err := storage.NewPackedRecordWriter("", []string{oldPath}, encodingSchema,
		packed.DefaultWriteBufferSize, packed.DefaultMultiPartUploadSize,
		[]storagecommon.ColumnGroup{oldGroup}, storageConfig, nil)
	require.NoError(t, err)
	arrowSchema, err := storage.ConvertToArrowSchema(encodingSchema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	for index, value := range []int64{1, 90, 1, 10, 7} {
		builder.Field(index).(*array.Int64Builder).Append(value)
	}
	record := builder.NewRecord()
	builder.Release()
	oldRecord := storage.NewSimpleArrowRecord(record, map[int64]int{0: 0, 1: 1, 100: 2, 120: 3, 121: 4})
	err = oldWriter.Write(oldRecord)
	oldRecord.Release()
	require.NoError(t, err)
	require.NoError(t, oldWriter.Close())
	require.FileExists(t, oldPath)

	// A 2.6 DataCoord-only segment has an encoding schema newer than V0, but
	// schema_version was absent from its protobuf and therefore reads as zero.
	durable := legacySchemaMigrationSegment(80, 0)
	durable.NumOfRows = 1
	durable.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 0, ChildFields: oldGroup.Fields, Format: "parquet",
		Binlogs: []*datapb.Binlog{{
			LogID: 1, LogPath: oldPath, EntriesNum: 1, TimestampFrom: 90, TimestampTo: 90,
			LogSize:         int64(oldWriter.GetColumnGroupWrittenCompressed(0)),
			MemorySize:      int64(oldWriter.GetColumnGroupWrittenUncompressed(0)),
			FieldNullCounts: map[int64]int64{121: 0},
		}},
	}}
	seek := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 100}
	checkpointPatch := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).Return(seek, []int64{1}, nil).Build()
	defer checkpointPatch.UnPatch()
	segmentPatch := mockey.Mock((*recoveryStorageImpl).getLegacySegmentInfo).Return(durable, nil).Build()
	defer segmentPatch.UnPatch()
	var encodedSegment []byte
	saves := 0
	catalogPatch := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
		saves++
		if len(snapshot.SegmentAssignments) == 0 {
			require.EqualValues(t, utility.RecoveryMagicStreamingInitialized, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
			return nil
		}
		require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
		var err error
		encodedSegment, err = proto.Marshal(snapshot.SegmentAssignments[1])
		return err
	}).Build()
	defer catalogPatch.UnPatch()
	store := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(90), TimeTick: 900, Magic: utility.RecoveryMagicStreamingInitialized,
	})
	assignments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	migrated, err := store.migrateLegacyRecoveryInfo(ctx, map[string]*streamingpb.VChannelMeta{"v1": channelMeta}, assignments)
	require.NoError(t, err)
	require.True(t, migrated)
	require.Equal(t, 2, saves)
	var recovered streamingpb.SegmentAssignmentMeta
	require.NoError(t, proto.Unmarshal(encodedSegment, &recovered))
	require.EqualValues(t, 1, recovered.GetSchemaVersion())
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED, recovered.GetState())
	require.Zero(t, durable.GetSchemaVersion(), "the DataCoord response is not rewritten")
	// Reopening the new format must use this persisted version, even though
	// the latest collection schema is V2 and carries another added field.
	restart := newTestRecoveryStorage(t, store.checkpoint.Clone())
	migrated, err = restart.migrateLegacyRecoveryInfo(ctx, map[string]*streamingpb.VChannelMeta{"v1": channelMeta},
		map[int64]*streamingpb.SegmentAssignmentMeta{1: &recovered})
	require.NoError(t, err)
	require.False(t, migrated)
	require.Equal(t, 2, saves)
	scheduler := &legacySchemaNativeScheduler{}
	lifecycle := &legacySchemaNativeLifecycle{}
	module, err := vchannel.NewModule(vchannel.ModuleConfig{
		PChannel: "p1", VChannel: "v1", VChannelMeta: proto.Clone(channelMeta).(*streamingpb.VChannelMeta),
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1: &recovered},
		Runtime:  moduleapi.Runtime{Scheduler: scheduler}, SegmentLifecycle: lifecycle,
		SegmentPackWriter: segment.NewBulkPackWriter(chunkManager, allocator.NewLocalAllocator(100, math.MaxInt64), storageConfig),
	})
	require.NoError(t, err)
	tail := message.NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 10, Partitions: []*messagespb.PartitionSegmentAssignment{{
				PartitionId: 20, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 1},
			}},
		}).WithBody(&msgpb.InsertRequest{
		Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 2,
		RowIDs: []int64{2, 3}, Timestamps: []uint64{999, 999},
		FieldsData: []*schemapb.FieldData{
			legacySchemaNativeLongField(100, []int64{2, 3}, nil),
			legacySchemaNativeLongField(120, []int64{20, 30}, nil),
			legacySchemaNativeLongField(121, []int64{8, 0}, []bool{true, false}),
		},
	}).MustBuildMutable().WithTimeTick(120).
		WithLastConfirmed(rmq.NewRmqID(119)).IntoImmutableMessage(rmq.NewRmqID(120))
	legacySchemaNativeObserve(t, module, tail)
	flush := message.NewFlushMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.FlushMessageHeader{CollectionId: 10, SegmentId: 1}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(150).
		WithLastConfirmed(rmq.NewRmqID(149)).IntoImmutableMessage(rmq.NewRmqID(150))
	legacySchemaNativeObserve(t, module, flush)
	for index := 0; index < len(scheduler.tasks); index++ {
		require.Less(t, index, 16, "the replay must complete without retrying")
		require.NoError(t, scheduler.tasks[index].Execute(ctx))
	}
	require.NotNil(t, lifecycle.committed)
	require.EqualValues(t, 1, lifecycle.committed.GetSchemaVersion())
	require.EqualValues(t, 3, lifecycle.committed.GetStat().GetModifiedRows())
	var binlogs []*datapb.Binlog
	var tailBinlog *datapb.Binlog
	for _, batch := range lifecycle.committed.GetPersistedStorage().GetBinlogs() {
		for _, group := range batch.GetFieldBinlog() {
			require.EqualValues(t, 0, group.GetFieldID())
			require.Equal(t, oldGroup.Fields, group.GetChildFields())
			for _, binlog := range group.GetBinlogs() {
				require.FileExists(t, binlog.GetLogPath())
				binlogs = append(binlogs, binlog)
				if binlog.GetLogPath() != oldPath {
					tailBinlog = binlog
				}
			}
		}
	}
	require.Len(t, binlogs, 2, "both the old packed file and the replayed tail must remain readable")
	require.NotNil(t, tailBinlog)
	require.Contains(t, tailBinlog.GetFieldNullCounts(), int64(121))
	require.EqualValues(t, 1, tailBinlog.GetFieldNullCounts()[121])
	reader, err := storage.NewBinlogRecordReader(ctx, []*datapb.FieldBinlog{{
		FieldID: 0, ChildFields: oldGroup.Fields, Format: "parquet", Binlogs: binlogs,
	}}, encodingSchema, storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(storageConfig))
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	var values []int64
	var valid []bool
	var timeTicks []int64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		column := record.Column(121).(*array.Int64)
		timestamps := record.Column(common.TimeStampField).(*array.Int64)
		for row := 0; row < column.Len(); row++ {
			values = append(values, column.Value(row))
			valid = append(valid, column.IsValid(row))
			timeTicks = append(timeTicks, timestamps.Value(row))
		}
	}
	require.Equal(t, []bool{true, true, false}, valid)
	require.Equal(t, []int64{7, 8}, values[:2])
	require.Equal(t, []int64{90, 120, 120}, timeTicks)
}

func legacySchemaNativeLongField(id int64, data []int64, valid []bool) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: id, Type: schemapb.DataType_Int64, ValidData: valid,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
		}},
	}
}

func legacySchemaNativeObserve(t *testing.T, module *vchannel.VChannelRecoveryModule, raw message.ImmutableMessage) {
	t.Helper()
	owner := message.NewOwnedImmutableMessage(raw, nil)
	dispatch := owner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), dispatch))
	dispatch.Release()
	owner.Release()
}

type legacySchemaNativeScheduler struct{ tasks []nodescheduler.Task }

func (s *legacySchemaNativeScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return legacySchemaNativeTaskHandle{}
}

type legacySchemaNativeTaskHandle struct{}

func (legacySchemaNativeTaskHandle) Cancel()                    {}
func (legacySchemaNativeTaskHandle) Wait(context.Context) error { return nil }

type legacySchemaNativeLifecycle struct {
	committed *streamingpb.SegmentAssignmentMeta
}

func (*legacySchemaNativeLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return nil
}

func (*legacySchemaNativeLifecycle) PersistGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta, *msgpb.MsgPosition, *msgpb.MsgPosition) error {
	return nil
}

func (l *legacySchemaNativeLifecycle) CommitL1Segment(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	l.committed = proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
	return &viewpb.DataVersion{StreamingVersion: 1}, nil
}
