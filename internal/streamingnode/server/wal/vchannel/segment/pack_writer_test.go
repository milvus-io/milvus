package segment

import (
	"context"
	"math"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestCurrentSplitForGrowingPackFillsNewSplitFormats(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() {
		_ = params.Reset(params.DataNodeCfg.StorageFormat.Key)
	})

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	meta := &streamingpb.SegmentAssignmentMeta{StorageVersion: storage.StorageV3}

	columnGroups, err := currentSplitForGrowingPack(schema, nil, meta)
	require.NoError(t, err)

	require.NotEmpty(t, columnGroups)
	for _, columnGroup := range columnGroups {
		assert.Equal(t, "parquet", columnGroup.Format)
	}
}

func TestManifestPathForGrowingPackUsesPrimaryStorageRoot(t *testing.T) {
	params := paramtable.Get()
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	}
	localRoot := t.TempDir()

	for _, testCase := range []struct {
		name        string
		storageType string
		minioRoot   string
		wantRoot    string
	}{
		{name: "local", storageType: "local", minioRoot: "files", wantRoot: localRoot},
		{name: "remote", storageType: "remote", minioRoot: "files", wantRoot: "files"},
		{name: "remote_bucket_root", storageType: "remote", minioRoot: "/", wantRoot: "."},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, params.Save(params.CommonCfg.StorageType.Key, testCase.storageType))
			// minio.rootPath stays set in both cases: under local storage it
			// must not leak into the key.
			require.NoError(t, params.Save(params.MinioCfg.RootPath.Key, testCase.minioRoot))
			require.NoError(t, params.Save(params.LocalStorageCfg.Path.Key, localRoot))
			t.Cleanup(func() {
				_ = params.Reset(params.CommonCfg.StorageType.Key)
				_ = params.Reset(params.MinioCfg.RootPath.Key)
				_ = params.Reset(params.LocalStorageCfg.Path.Key)
			})

			base, version, err := packed.UnmarshalManifestPath(manifestPathForGrowingPack(meta))
			require.NoError(t, err)
			assert.Equal(t, path.Join(testCase.wantRoot, "insert_log", "1", "2", "3"), base)
			assert.Equal(t, packed.ManifestEarliest, version)
		})
	}
}

func TestManifestPathForGrowingPackPreservesPersistedPath(t *testing.T) {
	for _, manifestPath := range []string{
		packed.MarshalManifestPath("files/insert_log/1/2/3", 7),
		packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 9),
	} {
		meta := &streamingpb.SegmentAssignmentMeta{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      3,
			StorageVersion: storage.StorageV3,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				ManifestPath: manifestPath,
			},
		}
		assert.Equal(t, manifestPath, manifestPathForGrowingPack(meta))
	}
}

func TestManifestPathForGrowingPackSkipsNonV3(t *testing.T) {
	for _, storageVersion := range []int64{storage.StorageV1, storage.StorageV2} {
		assert.Empty(t, manifestPathForGrowingPack(&streamingpb.SegmentAssignmentMeta{
			CollectionId:   1,
			PartitionId:    2,
			SegmentId:      3,
			StorageVersion: storageVersion,
		}))
	}
}

func TestCurrentSplitFromPersistedStorageRestoresFormat(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	persisted := &streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{{
			FieldBinlog: []*datapb.FieldBinlog{
				{FieldID: 100, ChildFields: []int64{100}, Format: "parquet"},
				{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
			},
		}},
	}

	columnGroups, err := currentSplitFromPersistedStorage(schema, persisted)
	require.NoError(t, err)

	require.Len(t, columnGroups, 2)
	assert.Equal(t, int64(101), columnGroups[1].GroupID)
	assert.Equal(t, []int64{101}, columnGroups[1].Fields)
	assert.Equal(t, []int{1}, columnGroups[1].Columns)
	assert.Equal(t, "vortex", columnGroups[1].Format)
}

func TestCurrentSplitFromPersistedStoragePreservesFormat(t *testing.T) {
	schema := testGrowingPackSchema()
	persistedStorage := &streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{
			{
				FieldBinlog: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
					{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
				},
			},
		},
	}

	currentSplit, err := currentSplitFromPersistedStorage(schema, persistedStorage)
	require.NoError(t, err)
	require.Len(t, currentSplit, 2)
	require.Equal(t, int64(0), currentSplit[0].GroupID)
	require.Equal(t, []int64{100, 0, 1}, currentSplit[0].Fields)
	require.Equal(t, []int{2, 0, 1}, currentSplit[0].Columns)
	require.Equal(t, "parquet", currentSplit[0].Format)
}

func TestCurrentSplitForNewGrowingPackFillsFormats(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	}

	currentSplit, err := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.NotEmpty(t, currentSplit)
	wantFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	for _, columnGroup := range currentSplit {
		require.Equal(t, wantFormat, columnGroup.Format)
	}
}

func TestCurrentSplitForGrowingPackPreservesPersistedManifestFormat(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "manifest-v2",
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "vortex"},
						{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
					},
				},
			},
		},
	}

	currentSplit, err := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.Len(t, currentSplit, 2)
	require.Equal(t, "vortex", currentSplit[0].Format)
}

func TestCurrentSplitForGrowingPackDoesNotGuessPersistedV3Format(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "manifest-v2",
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}},
						{FieldID: 101, ChildFields: []int64{101}},
					},
				},
			},
		},
	}

	currentSplit, err := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.Len(t, currentSplit, 2)
	require.Empty(t, currentSplit[0].Format)
}

func TestCurrentSplitFromPersistedStorageRejectsIncompatibleSchema(t *testing.T) {
	schema := testGrowingPackSchema()
	validGroups := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
		{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
	}
	for _, testCase := range []struct {
		name    string
		schema  *schemapb.CollectionSchema
		batches [][]*datapb.FieldBinlog
		message string
	}{
		{
			name:   "persisted field missing from encoding schema",
			schema: schema,
			batches: [][]*datapb.FieldBinlog{{
				{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
				{FieldID: 101, ChildFields: []int64{101, 121}, Format: "vortex"},
			}},
			message: "field 121 absent from schema",
		},
		{
			name:   "incompatible field in later binlog batch",
			schema: schema,
			batches: [][]*datapb.FieldBinlog{validGroups, {
				{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
				{FieldID: 101, ChildFields: []int64{121}, Format: "vortex"},
			}},
			message: "field 121 absent from schema",
		},
		{
			name:   "field duplicated across column groups",
			schema: schema,
			batches: [][]*datapb.FieldBinlog{{
				{FieldID: 0, ChildFields: []int64{100, 0, 1, 101}, Format: "parquet"},
				{FieldID: 101, ChildFields: []int64{101}, Format: "vortex"},
			}},
			message: "field 101 appears in multiple persisted column groups",
		},
		{
			name:   "mixed grouped and legacy field binlogs",
			schema: schema,
			batches: [][]*datapb.FieldBinlog{{
				{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
				{FieldID: 101},
			}},
			message: "persisted column group 101 has no child fields",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			persisted := &streamingpb.L1SegmentPersistedStorage{}
			for _, fields := range testCase.batches {
				persisted.Binlogs = append(persisted.Binlogs, &streamingpb.L1SegmentBinLogs{FieldBinlog: fields})
			}
			groups, err := currentSplitFromPersistedStorage(testCase.schema, persisted)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Contains(t, err.Error(), testCase.message)
			require.Nil(t, groups, "invalid field IDs must never default to column zero")
		})
	}
}

func TestCurrentSplitForGrowingPackHandlesLegacyFieldBinlogs(t *testing.T) {
	for _, version := range []int64{storage.StorageV1, storage.StorageV2, storage.StorageV3} {
		meta := &streamingpb.SegmentAssignmentMeta{
			StorageVersion: version,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FieldBinlog: []*datapb.FieldBinlog{
					{FieldID: 0}, {FieldID: 1}, {FieldID: 100}, {FieldID: 101},
				}}},
			},
		}
		groups, err := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
		require.NoError(t, err)
		if version == storage.StorageV1 {
			require.Empty(t, groups, "StorageV1 writes per-field binlogs without column groups")
			continue
		}
		require.NotEmpty(t, groups, "legacy binlogs without ChildFields must get a fresh split")
		var fields []int64
		for _, group := range groups {
			fields = append(fields, group.Fields...)
			require.NotEmpty(t, group.Format)
		}
		require.ElementsMatch(t, []int64{0, 1, 100, 101}, fields)
	}
}

func TestGrowingColumnGroupsRejectIncorrectColumnMapping(t *testing.T) {
	err := validateGrowingColumnGroups(testGrowingPackSchema(), []storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{100, 0, 1}, Columns: []int{2, 0, 1}},
		{GroupID: 101, Fields: []int64{101}, Columns: []int{0}},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Contains(t, err.Error(), "field 101 maps to column 0 instead of 3")
}

func TestFlushInsertBufferRejectsIncompatibleColumnGroupsBeforeWrite(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		vchannel     = "v1"
		timetick     = uint64(10)
	)
	schema := crashTestSchema()
	insert := buildCrashTestInsertMessage(t, vchannel, collectionID, partitionID, segmentID, timetick, timetick)
	pack := crashTestPack(collectionID, partitionID, segmentID, vchannel, timetick, schema, insert)
	pack.Meta.StorageVersion = storage.StorageV2
	pack.Meta.PersistedStorage.Binlogs = []*streamingpb.L1SegmentBinLogs{{FieldBinlog: []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
		{FieldID: 101, ChildFields: []int64{101, 121}, Format: "parquet"},
	}}}
	writeCalls := 0
	writer := &growingBulkPackWriter{writeFn: func(context.Context, *growingBulkWriteRequest) (*growingBulkWriteResult, error) {
		writeCalls++
		return &growingBulkWriteResult{}, nil
	}}

	result, err := writer.FlushInsertBuffer(context.Background(), pack)

	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.False(t, retry.IsRecoverable(err), "incompatible persisted groups must not retry object writes")
	require.Nil(t, result)
	require.Zero(t, writeCalls, "schema validation must happen before any storage IO")
}

func TestFlushInsertBufferBuildsStorageV2ColumnGroups(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		vchannel     = "v1"
		timetick     = uint64(10)
	)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        1,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     []int64{1},
			Timestamps: []uint64{timetick},
			NumRows:    1,
			FieldsData: []*schemapb.FieldData{
				newTestLongFieldData(0, 1),
				newTestLongFieldData(1, int64(timetick)),
				newTestLongFieldData(100, 1),
			},
		}).
		BuildMutable()
	require.NoError(t, err)
	insert := mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))

	writer := &growingBulkPackWriter{
		writeFn: func(_ context.Context, req *growingBulkWriteRequest) (*growingBulkWriteResult, error) {
			require.Equal(t, storage.StorageV2, req.storageVersion)
			require.NotEmpty(t, req.currentSplit)
			for _, columnGroup := range req.currentSplit {
				require.NotEmpty(t, columnGroup.Fields)
				require.NotEmpty(t, columnGroup.Format)
			}
			return &growingBulkWriteResult{
				statistics: &datapb.Statistics{InsertBinlogSize: 123},
			}, nil
		},
	}
	result, err := writer.FlushInsertBuffer(context.Background(), &flushPack{
		Meta: &streamingpb.SegmentAssignmentMeta{
			CollectionId:     collectionID,
			PartitionId:      partitionID,
			SegmentId:        segmentID,
			Vchannel:         vchannel,
			StorageVersion:   storage.StorageV2,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
		SegmentID:    segmentID,
		VChannel:     vchannel,
		FromTimeTick: timetick,
		ToTimeTick:   timetick,
		Schema:       schema,
		Rows:         1,
		Inserts:      []message.ImmutableMessage{insert},
	})
	require.NoError(t, err)
	require.NotNil(t, result.PersistedStorage.GetStatistics())
	assert.Equal(t, int64(123), result.PersistedStorage.GetStatistics().GetInsertBinlogSize())
}

func newTestLongFieldData(fieldID int64, values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}

func testGrowingPackSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, DataType: schemapb.DataType_Int64},
			{FieldID: 1, DataType: schemapb.DataType_Int64},
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		},
	}
}

func TestFlushInsertBufferMaterializesMissingBM25Output(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "consumer_fallback", Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: "max_length", Value: "1024"}, {Key: "enable_analyzer", Value: "true"},
			}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name: "bm25", Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"text"}, OutputFieldNames: []string{"sparse"},
			InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
		}},
	}
	body := &msgpb.InsertRequest{
		Version: msgpb.InsertDataVersion_ColumnBased,
		RowIDs:  []int64{1, 2}, Timestamps: []uint64{10, 10}, NumRows: 2,
		FieldsData: []*schemapb.FieldData{
			newTestLongFieldData(0, 1, 2),
			newTestLongFieldData(1, 10, 10),
			newTestLongFieldData(100, 1, 2),
			{
				FieldId: 101, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"search engine", "distributed database"}}},
				}},
			},
		},
	}
	packFor := func() *flushPack {
		msg := message.NewInsertMessageBuilderV1().WithVChannel("consumer_fallback").
			WithHeader(&messagespb.InsertMessageHeader{
				CollectionId: 991059427,
				Partitions: []*messagespb.PartitionSegmentAssignment{{
					PartitionId: 2, Rows: 2,
					SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 3},
				}},
			}).
			WithBody(body).MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().
			IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		pack := crashTestPack(991059427, 2, 3, "consumer_fallback", 10, schema, msg)
		pack.Rows = 2
		return pack
	}

	t.Run("missing managed runner", func(t *testing.T) {
		// Drop releases managed WAL runners before the final growing flush.
		// With no managed entry for this collection, use a pack-local runner.
		writer := &growingBulkPackWriter{writeFn: func(_ context.Context, req *growingBulkWriteRequest) (*growingBulkWriteResult, error) {
			require.Len(t, req.insertData, 1)
			require.Equal(t, 2, req.insertData[0].Data[102].RowNum())
			return &growingBulkWriteResult{}, nil
		}}
		_, err := writer.FlushInsertBuffer(context.Background(), packFor())
		require.NoError(t, err)
		_, stats, err := buildGrowingInsertData(schema, packFor())
		require.NoError(t, err)
		require.NotNil(t, stats[102])
	})
	t.Run("persist missing output with StorageV3", func(t *testing.T) {
		storageConfig, cm := setupV3TestEnv(t)
		writer := NewBulkPackWriter(cm, allocator.NewLocalAllocator(1, math.MaxInt64), storageConfig)
		result, err := writer.FlushInsertBuffer(context.Background(), packFor())
		require.NoError(t, err)
		stats, err := packed.GetManifestStats(result.PersistedStorage.GetManifestPath(), storageConfig)
		require.NoError(t, err)
		require.NotEmpty(t, stats["bm25.102"].Paths)
	})
	t.Run("transient materialization failure", func(t *testing.T) {
		mockey.PatchConvey("retry instead of poisoning WAL handles", t, func() {
			materializeErr := merr.WrapErrServiceInternalMsg("remote analyzer temporarily unavailable")
			mockey.Mock((*function.FunctionRunnerLocalStore).FillEmbeddingData).Return(materializeErr).Build()
			writer := &growingBulkPackWriter{}
			_, err := writer.FlushInsertBuffer(context.Background(), packFor())
			require.ErrorIs(t, err, materializeErr)
			require.True(t, retry.IsRecoverable(err))
		})
	})
	t.Run("preserve existing output", func(t *testing.T) {
		store := function.NewFunctionRunnerLocalStore()
		defer store.Close()
		require.NoError(t, store.FillEmbeddingData(991059427, schema, body))
		expected := body.FieldsData[len(body.FieldsData)-1].GetVectors().GetSparseFloatVector().Contents
		mockey.PatchConvey("no function execution for materialized WAL", t, func() {
			patch := mockey.Mock((*function.FunctionRunnerLocalStore).FillEmbeddingData).
				Return(merr.WrapErrServiceInternalMsg("must not execute again")).Build()
			data, _, err := buildGrowingInsertData(schema, packFor())
			require.NoError(t, err)
			require.Equal(t, expected, data[0].Data[102].(*storage.SparseFloatVectorFieldData).Contents)
			require.Zero(t, patch.Times())
		})
	})
}
