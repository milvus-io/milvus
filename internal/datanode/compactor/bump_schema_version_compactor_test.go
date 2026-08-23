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

package compactor

import (
	"context"
	sio "io"
	"math"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	mock_storage "github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBumpSchemaVersionCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(BumpSchemaVersionCompactionTaskSuite))
}

type fakeTextIndex struct{}

func (fakeTextIndex) Build(*indexcgowrapper.Dataset) error                        { return nil }
func (fakeTextIndex) Serialize() ([]*indexcgowrapper.Blob, error)                 { return nil, nil }
func (fakeTextIndex) GetIndexFileInfo() ([]*indexcgowrapper.IndexFileInfo, error) { return nil, nil }
func (fakeTextIndex) Load([]*indexcgowrapper.Blob) error                          { return nil }
func (fakeTextIndex) Delete() error                                               { return nil }
func (fakeTextIndex) CleanLocalData() error                                       { return nil }
func (fakeTextIndex) UpLoad() (*cgopb.IndexStats, error) {
	return &cgopb.IndexStats{SerializedIndexInfos: []*cgopb.SerializedIndexFileInfo{{FileName: "text-index", FileSize: 42}}}, nil
}

type fakeBinlogRecordWriter struct {
	fieldBinlogs     map[storage.FieldID]*datapb.FieldBinlog
	statsLog         *datapb.FieldBinlog
	bm25StatsLog     map[storage.FieldID]*datapb.FieldBinlog
	manifest         string
	expirQuantiles   []int64
	rowNum           int64
	statsBlobSize    int64
	schema           *schemapb.CollectionSchema
	writtenTimestamp []int64
}

type cleanupTrackingBatchWriter struct {
	closeCount int
	abortCount int
	writeRows  []int
	closeErr   error
}

type emptyRecordReader struct{}

func (*emptyRecordReader) Next() (storage.Record, error) { return nil, sio.EOF }
func (*emptyRecordReader) Close() error                  { return nil }

func (w *cleanupTrackingBatchWriter) Write(record storage.Record) error {
	w.writeRows = append(w.writeRows, record.Len())
	return nil
}

func (w *cleanupTrackingBatchWriter) GetWrittenUncompressed() uint64 {
	return 0
}
func (w *cleanupTrackingBatchWriter) AsNewColumnGroups() {}
func (w *cleanupTrackingBatchWriter) Abort()             { w.abortCount++ }
func (w *cleanupTrackingBatchWriter) Close() (packed.WriterOutput, error) {
	w.closeCount++
	return nil, w.closeErr
}

func (w *fakeBinlogRecordWriter) Write(r storage.Record) error {
	w.rowNum += int64(r.Len())
	if tsArray, ok := r.Column(common.TimeStampField).(*array.Int64); ok {
		for i := 0; i < tsArray.Len(); i++ {
			w.writtenTimestamp = append(w.writtenTimestamp, tsArray.Value(i))
		}
	}
	return nil
}

func (w *fakeBinlogRecordWriter) GetWrittenUncompressed() uint64 { return 0 }
func (w *fakeBinlogRecordWriter) Close() error                   { return nil }
func (w *fakeBinlogRecordWriter) GetLogs() (map[storage.FieldID]*datapb.FieldBinlog, *datapb.FieldBinlog, map[storage.FieldID]*datapb.FieldBinlog, string, []int64) {
	return w.fieldBinlogs, w.statsLog, w.bm25StatsLog, w.manifest, w.expirQuantiles
}
func (w *fakeBinlogRecordWriter) GetRowNum() int64                   { return w.rowNum }
func (w *fakeBinlogRecordWriter) GetStatsBlobSize() int64            { return w.statsBlobSize }
func (w *fakeBinlogRecordWriter) FlushChunk() error                  { return nil }
func (w *fakeBinlogRecordWriter) GetBufferUncompressed() uint64      { return 0 }
func (w *fakeBinlogRecordWriter) Schema() *schemapb.CollectionSchema { return w.schema }

type BumpSchemaVersionCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO

	meta           *etcdpb.CollectionMeta
	multiSegWriter *MultiSegmentWriter

	task *bumpSchemaVersionCompactionTask
}

func (s *BumpSchemaVersionCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *BumpSchemaVersionCompactionTaskSuite) setupTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())

	s.meta = genTestCollectionMetaWithBM25()

	params, err := compaction.GenerateJSONParams(s.meta.GetSchema())
	if err != nil {
		panic(err)
	}

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:        1,
			PartitionID:         1,
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
			InsertChannel:       "test_channel",
			StorageVersion:      storage.StorageV3,
			Manifest:            "manifest",
		}},
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Id:               100,
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			InputFieldIds:    []int64{101},
			OutputFieldNames: []string{"sparse"},
			OutputFieldIds:   []int64{102},
		}},
		TotalRows: 3,
	}

	cm, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	if err != nil {
		panic(err)
	}
	compactionParams := compaction.GenParams()
	compactionParams.StorageVersion = storage.StorageV3
	s.task = NewBumpSchemaVersionCompactionTask(context.Background(), cm, plan, compactionParams)
}

func genTestCollectionMetaWithMinHash() *etcdpb.CollectionMeta {
	return &etcdpb.CollectionMeta{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Name: "schema_bump_minhash_test",
			Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{
					FieldID:  101,
					Name:     "text",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxLengthKey, Value: "128"},
					},
				},
				{
					FieldID:  102,
					Name:     "minhash",
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "32"},
					},
					IsFunctionOutput: true,
				},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "minhash_func",
					Id:               1000,
					Type:             schemapb.FunctionType_MinHash,
					InputFieldNames:  []string{"text"},
					InputFieldIds:    []int64{101},
					OutputFieldNames: []string{"minhash"},
					OutputFieldIds:   []int64{102},
				},
			},
		},
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) setupMinHashTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMetaWithMinHash()

	params, err := compaction.GenerateJSONParams(s.meta.GetSchema())
	s.Require().NoError(err)

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      100,
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV3,
			Manifest:       "manifest",
		}},
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		TotalRows:              3,
	}

	cm, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	s.Require().NoError(err)
	compactionParams := compaction.GenParams()
	compactionParams.StorageVersion = storage.StorageV3
	s.task = NewBumpSchemaVersionCompactionTask(context.Background(), cm, plan, compactionParams)
}

func (s *BumpSchemaVersionCompactionTaskSuite) prepareMinHashBumpSchemaVersionCompaction() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.initSegBufferForSchemaBump(segID)
	s.finishBumpSchemaVersionSegment()
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionMaterializesMinHashOutput() {
	s.setupMinHashTest()
	s.prepareMinHashBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
	s.Require().Len(result.GetSegments(), 1)

	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEmpty(segment.GetManifest())
	s.Empty(segment.GetBm25Logs())

	const minHashOutputFieldID = int64(102)
	found := false
	for _, fl := range segment.GetInsertLogs() {
		if fl.GetFieldID() == minHashOutputFieldID {
			found = true
			s.Require().Len(fl.GetBinlogs(), 1)
			s.EqualValues(3, fl.GetBinlogs()[0].GetEntriesNum())
			s.Greater(fl.GetBinlogs()[0].GetMemorySize(), int64(0))
		}
	}
	s.True(found, "MinHash output field should be materialized into insert logs")

	// The in-place receiver reads neither of these; they were echoes of the
	// plan, whose arrays are empty for a recovered StorageV3 segment.
	s.Empty(segment.GetDeltalogs())
	s.Empty(segment.GetField2StatslogPaths())

	// Stats is an INCREMENT: it describes only the columns this run wrote, so
	// it carries no delete or timestamp aggregates for DataCoord to adopt.
	stats := segment.GetStats()
	s.Require().NotNil(stats)
	s.Greater(stats.GetInsertBinlogSize(), int64(0))
	s.EqualValues(1, stats.GetInsertBinlogCount())
	s.EqualValues(0, stats.GetDeleteNumRows())
	s.EqualValues(0, stats.GetDeltaBinlogSize())
	s.EqualValues(0, stats.GetTimestampFrom())
	s.EqualValues(0, stats.GetTimestampTo())
	s.Contains(stats.GetNullCounts(), minHashOutputFieldID)
}

func (s *BumpSchemaVersionCompactionTaskSuite) SetupTest() {
	// Align with sort_compaction_test fixture: use a per-test temp dir as the local
	// storage root and disable Loon FFI. Without UseLoonFFI=false, the loon local
	// filesystem double-prepends the configured root_path onto already-absolute log
	// paths produced by MultiSegmentWriter, causing the reader to look for files at
	// /rootPath/rootPath/insert_log/... and fail to open them.
	paramtable.Get().Save("common.storage.enablev2", "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, s.T().TempDir())
	initcore.InitStorageV2FileSystem(paramtable.Get())
	s.setupTest()
}

func (s *BumpSchemaVersionCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	paramtable.Get().Reset("common.storage.enablev2")
	initcore.CleanArrowFileSystem()
}

func (s *BumpSchemaVersionCompactionTaskSuite) prepareBumpSchemaVersionCompaction() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create multi segment writer with varchar field (input) but without sparse vector field (output)
	// This simulates a segment that needs schema bump
	s.initSegBufferForSchemaBump(segID)
	s.finishBumpSchemaVersionSegment()
}

func (s *BumpSchemaVersionCompactionTaskSuite) prepareBumpSchemaVersionCompactionWithDroppedField() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.initSegBufferWithDroppedFieldForSchemaBump(segID)
	s.finishBumpSchemaVersionSegment()
}

func (s *BumpSchemaVersionCompactionTaskSuite) finishBumpSchemaVersionSegment() {
	// Close the writer to finalize segments
	err := s.multiSegWriter.Close()
	s.Require().NoError(err)

	// Get compaction segments
	segments := s.multiSegWriter.GetCompactionSegments()
	s.Require().Equal(1, len(segments), "should create exactly one segment")

	segment := segments[0]
	// Use the actual segmentID allocated by MultiSegmentWriter
	actualSegID := segment.GetSegmentID()
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      actualSegID,
			FieldBinlogs:   segment.GetInsertLogs(),
			InsertChannel:  "test_channel",
			StorageVersion: segment.GetStorageVersion(),
			Manifest:       segment.GetManifest(),
		},
	}

	mlog.Info(context.TODO(), "created segment with MultiSegmentWriter",
		mlog.FieldSegmentID(actualSegID),
		mlog.Int("numRows", int(segment.GetNumOfRows())),
		mlog.Int("insertLogsCount", len(segment.GetInsertLogs())))
}

func (s *BumpSchemaVersionCompactionTaskSuite) initSegBufferForSchemaBump(segID int64) {
	s.initSegBufferForSchemaBumpWithFields(segID, schemaBumpBaseFields(), nil)
}

func (s *BumpSchemaVersionCompactionTaskSuite) initSegBufferWithDroppedFieldForSchemaBump(segID int64) {
	fields := append(schemaBumpBaseFields(), &schemapb.FieldSchema{
		FieldID:  103,
		Name:     "dropped",
		DataType: schemapb.DataType_Int64,
	})
	s.initSegBufferForSchemaBumpWithFields(segID, fields, func(i int, value map[int64]interface{}) {
		value[103] = int64(i)
	})
}

func schemaBumpBaseFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{
		{
			FieldID:  common.RowIDField,
			Name:     "row_id",
			DataType: schemapb.DataType_Int64,
		},
		{
			FieldID:  common.TimeStampField,
			Name:     "Timestamp",
			DataType: schemapb.DataType_Int64,
		},
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
				{
					Key:   common.MaxLengthKey,
					Value: "128",
				},
			},
		},
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) initSegBufferForSchemaBumpWithFields(segID int64, fields []*schemapb.FieldSchema, fillValue func(int, map[int64]interface{})) {
	schema := &schemapb.CollectionSchema{Fields: fields}

	// Create allocator for MultiSegmentWriter
	// Use segID as the starting point for segmentID allocation to ensure consistent paths
	segIDAlloc := allocator.NewLocalAllocator(segID, math.MaxInt64)
	logIDAlloc := allocator.NewLocalAllocator(9530, 19530)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)

	// Create MultiSegmentWriter with StorageV3
	multiSegWriter, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		compAlloc,
		64*1024*1024, // 64MB segment size
		schema,
		s.task.compactionParams,
		1000, // maxRows
		PartitionID,
		CollectionID,
		"test_channel",
		compactionBatchSize,
		storage.WithStorageConfig(s.task.compactionParams.StorageConfig),
		storage.WithVersion(storage.StorageV3),
	)
	s.Require().NoError(err)

	// Write test data with varchar field
	for i := 0; i < 3; i++ {
		value := map[int64]interface{}{
			common.RowIDField:     segID + int64(i),
			common.TimeStampField: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			100:                   segID + int64(i),
			101:                   "test string " + string(rune('0'+i)),
		}
		if fillValue != nil {
			fillValue(i, value)
		}
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(segID + int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     value,
		}
		err = multiSegWriter.WriteValue(&v)
		s.Require().NoError(err)
	}

	s.multiSegWriter = multiSegWriter
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionSuccess() {
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	// SegmentID should match the one allocated by MultiSegmentWriter
	s.NotZero(segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEmpty(segment.GetInsertLogs())
	// BM25 stats are embedded in the V3 manifest atomically with column groups.
	s.Empty(segment.GetBm25Logs())
	s.NotEmpty(segment.GetManifest())

	// The materialized output field (ID=102) must carry a non-zero LogID in its insert binlog.
	const materializedOutputFieldID = int64(102)
	for _, fl := range segment.GetInsertLogs() {
		if fl.GetFieldID() == materializedOutputFieldID {
			s.Require().NotEmpty(fl.GetBinlogs(), "materialized output field must have at least one binlog entry")
			s.NotZero(fl.GetBinlogs()[0].GetLogID(),
				"V3 schema bump insert binlog (fieldID=%d) must have non-zero LogID", materializedOutputFieldID)
		}
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsOrdinaryNullableField() {
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added_nullable",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEqual(segment.GetBaseManifest(), segment.GetManifest())
	s.Require().Len(segment.GetInsertLogs(), 1)
	s.EqualValues(ordinaryFieldID, segment.GetInsertLogs()[0].GetFieldID())
	s.EqualValues(3, segment.GetInsertLogs()[0].GetBinlogs()[0].GetEntriesNum())
	s.EqualValues(3, segment.GetStats().GetNullCounts()[ordinaryFieldID])

	fields, err := packed.GetManifestFieldIDs(segment.GetManifest(), s.task.compactionParams.StorageConfig)
	s.Require().NoError(err)
	s.Contains(fields, ordinaryFieldID)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsOrdinaryDefaultField() {
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added_default",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{
			LongData: 42,
		}},
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.Require().Len(segment.GetInsertLogs(), 1)
	s.EqualValues(ordinaryFieldID, segment.GetInsertLogs()[0].GetFieldID())
	// #52781 (merged): the reader default-fills absent default-valued fields, so
	// the added column materializes its declared default (42), not NULL.
	s.EqualValues(0, segment.GetStats().GetNullCounts()[ordinaryFieldID])

	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(s.task.plan.GetSchema(), ordinaryFieldID)},
	}, storage.WithCollectionID(1), storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	column, ok := record.Column(ordinaryFieldID).(*array.Int64)
	s.Require().True(ok)
	s.Equal(3, column.Len())
	for i := 0; i < column.Len(); i++ {
		s.False(column.IsNull(i))
		s.EqualValues(42, column.Value(i))
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsTTLDefaultWithoutFiltering() {
	const ttlFieldID = int64(103)
	expireAt := s.task.currentTime.Add(-time.Hour).UnixMicro()
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ttlFieldID,
		Name:     "expire_at",
		DataType: schemapb.DataType_Timestamptz,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{
			TimestamptzData: expireAt,
		}},
	})
	s.task.plan.Schema.Properties = append(s.task.plan.Schema.Properties, &commonpb.KeyValuePair{
		Key: common.CollectionTTLFieldKey, Value: "expire_at",
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows(), "an added TTL field must not filter rows during additive reconciliation")
	// #52781 (merged): the reader default-fills the added TTL field with its
	// declared default; additive reconciliation must NOT apply TTL filtering, so
	// all rows survive even though that default is already expired.
	s.EqualValues(0, segment.GetStats().GetNullCounts()[ttlFieldID])

	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(s.task.plan.GetSchema(), ttlFieldID)},
	}, storage.WithCollectionID(1), storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	column := record.Column(ttlFieldID).(*array.Int64)
	for i := 0; i < column.Len(); i++ {
		s.False(column.IsNull(i))
		s.EqualValues(expireAt, column.Value(i))
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsNullableTTLWithoutFiltering() {
	const ttlFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID: ttlFieldID, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true,
	})
	s.task.plan.Schema.Properties = append(s.task.plan.Schema.Properties, &commonpb.KeyValuePair{
		Key: common.CollectionTTLFieldKey, Value: "expire_at",
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows(), "an added nullable TTL field must not filter rows during additive reconciliation")
	s.EqualValues(3, segment.GetStats().GetNullCounts()[ttlFieldID])

	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(s.task.plan.GetSchema(), ttlFieldID)},
	}, storage.WithCollectionID(1), storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	column := record.Column(ttlFieldID).(*array.Int64)
	s.Equal(3, column.Len())
	s.Equal(3, column.NullN())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsNullableStructChildren() {
	const structFieldID = int64(200)
	children := []*schemapb.FieldSchema{
		{
			FieldID:     201,
			Name:        "profile[tags]",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			Nullable:    true,
			TypeParams: []*commonpb.KeyValuePair{{
				Key: common.MaxCapacityKey, Value: "16",
			}},
		},
		{
			FieldID:     202,
			Name:        "profile[scores]",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			Nullable:    true,
			TypeParams: []*commonpb.KeyValuePair{{
				Key: common.MaxCapacityKey, Value: "16",
			}},
		},
	}
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{
		FieldID:  structFieldID,
		Name:     "profile",
		Fields:   children,
		Nullable: true,
	}}
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows())
	s.Require().Len(segment.GetInsertLogs(), 2)
	s.ElementsMatch([]int64{201, 202}, lo.Map(segment.GetInsertLogs(), func(fb *datapb.FieldBinlog, _ int) int64 {
		return fb.GetFieldID()
	}))

	manifestFields, err := packed.GetManifestFieldIDs(segment.GetManifest(), s.task.compactionParams.StorageConfig)
	s.Require().NoError(err)
	s.NotContains(manifestFields, structFieldID)
	for _, child := range children {
		s.Contains(manifestFields, child.GetFieldID())
		s.EqualValues(3, segment.GetStats().GetNullCounts()[child.GetFieldID()])
	}

	readSchema := &schemapb.CollectionSchema{StructArrayFields: s.task.plan.Schema.GetStructArrayFields()}
	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), readSchema,
		storage.WithCollectionID(1),
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(s.task.compactionParams.StorageConfig),
	)
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	for _, child := range children {
		column := record.Column(child.GetFieldID())
		s.Require().NotNil(column)
		s.Equal(3, column.Len())
		s.Equal(3, column.NullN())
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPrefillsMissingFunctionInput() {
	const addedInputFieldID = int64(103)
	outputField := typeutil.GetField(s.task.plan.GetSchema(), 102)
	s.task.plan.Schema.Fields = []*schemapb.FieldSchema{
		s.task.plan.Schema.Fields[0],
		s.task.plan.Schema.Fields[1],
		s.task.plan.Schema.Fields[2],
		s.task.plan.Schema.Fields[3],
		{
			FieldID:  addedInputFieldID,
			Name:     "added_input",
			DataType: schemapb.DataType_VarChar,
			Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{
				Key: common.MaxLengthKey, Value: "128",
			}},
		},
		outputField,
	}
	s.task.plan.Schema.Functions[0].InputFieldIds = []int64{addedInputFieldID}
	s.task.plan.Schema.Functions[0].InputFieldNames = []string{"added_input"}
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.Require().Len(segment.GetInsertLogs(), 2)
	s.ElementsMatch([]int64{102, addedInputFieldID}, lo.Map(segment.GetInsertLogs(), func(fb *datapb.FieldBinlog, _ int) int64 {
		return fb.GetFieldID()
	}))
	s.EqualValues(3, segment.GetStats().GetNullCounts()[addedInputFieldID])
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPrefillsMissingMultiAnalyzerByField() {
	const byFieldID = int64(103)
	textField := typeutil.GetField(s.task.plan.GetSchema(), 101)
	textField.TypeParams = append(textField.GetTypeParams(),
		&commonpb.KeyValuePair{Key: common.EnableAnalyzerKey, Value: "true"},
		&commonpb.KeyValuePair{Key: "multi_analyzer_params", Value: `{"by_field":"lang","analyzers":{"default":{"type":"standard"}}}`},
	)
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID: byFieldID, Name: "lang", DataType: schemapb.DataType_VarChar, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "32"}},
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.ElementsMatch([]int64{102, byFieldID}, lo.Map(segment.GetInsertLogs(), func(fb *datapb.FieldBinlog, _ int) int64 {
		return fb.GetFieldID()
	}))
	s.EqualValues(3, segment.GetStats().GetNullCounts()[byFieldID])
	s.EqualValues(3, segment.GetNumOfRows())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationSkipsPresentFunctionOutput() {
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID: ordinaryFieldID, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	physicalFields := append(schemaBumpBaseFields(), typeutil.GetField(s.task.plan.GetSchema(), 102))
	s.initSegBufferForSchemaBumpWithFields(100, physicalFields, func(_ int, value map[int64]interface{}) {
		value[102] = typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1})
	})
	s.finishBumpSchemaVersionSegment()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.Require().Len(segment.GetInsertLogs(), 1)
	s.EqualValues(ordinaryFieldID, segment.GetInsertLogs()[0].GetFieldID())
	s.EqualValues(3, segment.GetStats().GetNullCounts()[ordinaryFieldID])
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationPersistsNullableTextAsBinary() {
	const textFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  textFieldID,
		Name:     "added_text",
		DataType: schemapb.DataType_Text,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{
			Key: common.MaxLengthKey, Value: "65535",
		}},
	})
	s.prepareBumpSchemaVersionCompaction()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.Require().Len(segment.GetInsertLogs(), 1)
	s.EqualValues(textFieldID, segment.GetInsertLogs()[0].GetFieldID())
	s.EqualValues(3, segment.GetStats().GetNullCounts()[textFieldID])
	s.Empty(segment.GetTextStatsLogs())

	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		typeutil.GetField(s.task.plan.GetSchema(), textFieldID),
	}}
	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), readSchema,
		storage.WithCollectionID(1),
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(s.task.compactionParams.StorageConfig),
	)
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	textColumn, ok := record.Column(textFieldID).(*array.Binary)
	s.Require().True(ok)
	s.Equal(3, textColumn.Len())
	s.Equal(3, textColumn.NullN())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationRejectsRowCountMismatch() {
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added_nullable",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	s.prepareBumpSchemaVersionCompaction()
	s.task.plan.TotalRows = 4

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "read 3 rows, expected 4")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReconciliationAbortsWriterOnRowCountMismatch() {
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added_nullable",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	s.prepareBumpSchemaVersionCompaction()
	s.task.plan.TotalRows = 4

	writer := &cleanupTrackingBatchWriter{}
	writerPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).setupWriter).Return(
		&bumpSchemaVersionWriterResult{
			writer: writer,
			columnGroups: []storagecommon.ColumnGroup{{
				GroupID: ordinaryFieldID,
				Columns: []int{0},
				Fields:  []int64{ordinaryFieldID},
			}},
			storageVersion: storage.StorageV3,
		}, nil,
	).Build()
	defer writerPatch.UnPatch()

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.Equal(1, writer.abortCount)
	s.Zero(writer.closeCount)
}

// buildTextLOBTask builds a minimal schema-bump task whose schema optionally has a
// DataType_Text field (the LOB-carrying input of a BM25 function). Only plan +
// compactionParams are needed to exercise the LOB wiring (the cgo manifest helpers
// are mocked in the tests below), so no chunk manager is required.
func (s *BumpSchemaVersionCompactionTaskSuite) buildTextLOBTask(withTextField bool) *bumpSchemaVersionCompactionTask {
	fields := []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}
	schema := &schemapb.CollectionSchema{Name: "text_lob_bump", Fields: fields}
	if withTextField {
		schema.Fields = append(schema.Fields,
			&schemapb.FieldSchema{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			&schemapb.FieldSchema{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		)
		schema.Functions = []*schemapb.FunctionSchema{{
			Name: "bm25", Id: 100, Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"text"}, InputFieldIds: []int64{101},
			OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102},
		}}
	}
	plan := &datapb.CompactionPlan{
		PlanID:    999,
		Type:      datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:    schema,
		TotalRows: 3,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID: 1, PartitionID: 1, SegmentID: 100,
			StorageVersion: storage.StorageV3, Manifest: "manifest",
		}},
	}
	params := compaction.GenParams()
	params.StorageVersion = storage.StorageV3
	return &bumpSchemaVersionCompactionTask{
		ctx:              context.Background(),
		plan:             plan,
		compactionParams: params,
	}
}

// TestInitLOBCompactionContextTextFieldReuseAll: a schema-bump on a segment with a
// TEXT field forces REUSE_ALL for that field (never REWRITE_ALL), so its existing
// LOB data is carried by reference.
func (s *BumpSchemaVersionCompactionTaskSuite) TestInitLOBCompactionContextTextFieldReuseAll() {
	task := s.buildTextLOBTask(true)
	collectPatch := mockey.Mock(compaction.CollectLobFilesFromManifests).Return(
		map[int64][]packed.LobFileInfo{100: {{FieldID: 101, TotalRows: 3, ValidRows: 3}}}, nil,
	).Build()
	defer collectPatch.UnPatch()

	err := task.initLOBCompactionContext(context.Background())
	s.NoError(err)
	s.Require().NotNil(task.lobContext)
	s.True(task.lobContext.HasReuseAllFields(), "TEXT field must be forced REUSE_ALL for schema bump")
	s.True(task.lobContext.IsReuseAll(101))
}

// TestInitLOBCompactionContextInlineTextReuseAll: even with NO LOB files (inline
// TEXT < threshold), the forced strategy still produces a REUSE_ALL decision, so
// the writer is told WithTextRefsAsBinary (HasReuseAllFields true) and the inline
// bytes are preserved.
func (s *BumpSchemaVersionCompactionTaskSuite) TestInitLOBCompactionContextInlineTextReuseAll() {
	task := s.buildTextLOBTask(true)
	collectPatch := mockey.Mock(compaction.CollectLobFilesFromManifests).Return(
		map[int64][]packed.LobFileInfo{}, nil, // no LOB files -> inline TEXT
	).Build()
	defer collectPatch.UnPatch()

	err := task.initLOBCompactionContext(context.Background())
	s.NoError(err)
	s.Require().NotNil(task.lobContext)
	s.True(task.lobContext.HasReuseAllFields(), "inline TEXT must still force REUSE_ALL so WithTextRefsAsBinary is applied")
}

// TestInitLOBCompactionContextNoTextFieldNoop: schemas without a TEXT field skip
// the LOB path entirely (lobContext stays nil).
func (s *BumpSchemaVersionCompactionTaskSuite) TestInitLOBCompactionContextNoTextFieldNoop() {
	task := s.buildTextLOBTask(false)
	err := task.initLOBCompactionContext(context.Background())
	s.NoError(err)
	s.Nil(task.lobContext, "no TEXT field -> lobContext stays nil (no-op)")
}

// TestApplyLOBCompactionMergesRefsIntoManifest: the output manifest is updated with
// the merged LOB references (REUSE_ALL).
func (s *BumpSchemaVersionCompactionTaskSuite) TestApplyLOBCompactionMergesRefsIntoManifest() {
	task := s.buildTextLOBTask(true)
	collectPatch := mockey.Mock(compaction.CollectLobFilesFromManifests).Return(
		map[int64][]packed.LobFileInfo{100: {{FieldID: 101, TotalRows: 3, ValidRows: 3}}}, nil,
	).Build()
	defer collectPatch.UnPatch()
	s.Require().NoError(task.initLOBCompactionContext(context.Background()))
	s.Require().True(task.lobContext.HasReuseAllFields())

	// Capture the chained manifest value inside the closure at call time rather than
	// storing the argument map and reading it back after applyLOBCompaction returns:
	// holding the map reference and indexing it later trips the runtime "concurrent
	// map read and map write" detector under -race.
	var gotChainedManifest string
	applyPatch := mockey.Mock(compaction.ApplyLobCompactionToManifests).To(
		func(_ *compaction.LOBCompactionContext, outputManifests map[int64]string, _ *indexpb.StorageConfig) (map[int64]string, error) {
			gotChainedManifest = outputManifests[200]
			return map[int64]string{200: "merged-manifest"}, nil
		}).Build()
	defer applyPatch.UnPatch()

	out := &datapb.CompactionSegment{SegmentID: 200, Manifest: "orig-manifest"}
	err := task.applyLOBCompaction(context.Background(), out)
	s.NoError(err)
	s.Equal("merged-manifest", out.GetManifest(), "output manifest must be updated with merged LOB refs")
	s.Equal("orig-manifest", gotChainedManifest, "merge must chain on the segment's current manifest")
}

// TestApplyLOBCompactionNilContextNoop: no LOB context (e.g. no TEXT fields) -> the
// output manifest is left unchanged.
func (s *BumpSchemaVersionCompactionTaskSuite) TestApplyLOBCompactionNilContextNoop() {
	task := s.buildTextLOBTask(true) // no init -> lobContext nil
	out := &datapb.CompactionSegment{SegmentID: 200, Manifest: "orig-manifest"}
	err := task.applyLOBCompaction(context.Background(), out)
	s.NoError(err)
	s.Equal("orig-manifest", out.GetManifest())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteDropsExpirQuantilesWhenTTLFieldRemoved() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	s.task.plan.GetSegmentBinlogs()[0].IsSorted = true
	s.task.plan.GetSegmentBinlogs()[0].IsSortedByNamespace = true
	s.task.plan.GetSegmentBinlogs()[0].ExpirQuantiles = []int64{100, 200, 300}

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	segment := result.GetSegments()[0]
	inputSegment := s.task.plan.GetSegmentBinlogs()[0]
	inputSegmentID := inputSegment.GetSegmentID()
	newSegmentID := s.task.plan.GetPreAllocatedSegmentIDs().GetBegin()
	s.NotEqual(inputSegmentID, segment.GetSegmentID())
	s.Equal(newSegmentID, segment.GetSegmentID())
	s.True(segment.GetIsSorted())
	s.True(segment.GetIsSortedByNamespace())
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEmpty(segment.GetInsertLogs())
	s.Empty(segment.GetBm25Logs())
	s.NotEmpty(segment.GetManifest())
	s.Empty(segment.GetExpirQuantiles())
	manifestBasePath, _, err := packed.UnmarshalManifestPath(segment.GetManifest())
	s.NoError(err)
	expectedBasePath := path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), common.SegmentInsertLogPath, metautil.JoinIDPath(inputSegment.GetCollectionID(), inputSegment.GetPartitionID(), newSegmentID))
	s.Equal(expectedBasePath, manifestBasePath)

	for _, fieldBinlog := range segment.GetInsertLogs() {
		s.NotEqualValues(103, fieldBinlog.GetFieldID())
		s.NotContains(fieldBinlog.GetChildFields(), int64(103))
		for _, binlog := range fieldBinlog.GetBinlogs() {
			s.Empty(binlog.GetLogPath())
			s.NotZero(binlog.GetLogID())
		}
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteNormalizesCommitTimestampBeforeWrite() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	s.task.plan.GetSegmentBinlogs()[0].CommitTimestamp = 5000
	newSegmentID := s.task.plan.GetPreAllocatedSegmentIDs().GetBegin()
	writer := &fakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			100: {FieldID: 100, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 3}}},
			101: {FieldID: 101, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{LogID: 2, EntriesNum: 3}}},
			102: {FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogID: 3, EntriesNum: 3}}},
		},
		manifest: packed.MarshalManifestPath(path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), common.SegmentInsertLogPath, metautil.JoinIDPath(1, 1, newSegmentID)), 1),
		schema:   s.task.plan.GetSchema(),
	}
	s.Require().NotEqual([]int64{5000, 5000, 5000}, writer.writtenTimestamp)

	newWriterPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer newWriterPatch.UnPatch()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)
	s.Equal([]int64{5000, 5000, 5000}, writer.writtenTimestamp)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteAddsWriterStatsToManifest() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	newSegmentID := s.task.plan.GetPreAllocatedSegmentIDs().GetBegin()
	manifestPath := packed.MarshalManifestPath(path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), common.SegmentInsertLogPath, metautil.JoinIDPath(1, 1, newSegmentID)), 1)
	writer := &fakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			100: {FieldID: 100, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 3}}},
			101: {FieldID: 101, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{LogID: 2, EntriesNum: 3}}},
			102: {FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogID: 3, EntriesNum: 3}}},
		},
		statsLog: &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), "_stats/bloom_filter.100/9530"), MemorySize: 64}}},
		bm25StatsLog: map[storage.FieldID]*datapb.FieldBinlog{
			102: {FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), "_stats/bm25.102/9531"), MemorySize: 128}}},
		},
		manifest: manifestPath,
		schema:   s.task.plan.GetSchema(),
	}

	newWriterPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer newWriterPatch.UnPatch()
	var statKeys []string
	addStatsPatch := mockey.Mock(packed.AddStatsToManifest).To(func(manifestPath string, _ *indexpb.StorageConfig, stats []packed.StatEntry) (string, error) {
		for _, stat := range stats {
			statKeys = append(statKeys, stat.Key)
		}
		return manifestPath + "-with-writer-stats", nil
	}).Build()
	defer addStatsPatch.UnPatch()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	segment := result.GetSegments()[0]
	s.Empty(segment.GetField2StatslogPaths())
	s.Empty(segment.GetBm25Logs())
	s.Contains(segment.GetManifest(), "with-writer-stats")
	s.ElementsMatch([]string{"bloom_filter.100", "bm25.102"}, statKeys)
}

// V3 writers leave statsLog/bm25StatsLog nil because stats are embedded
// in the manifest, not in FieldBinlog arrays. The result Statistics must
// still report a non-zero StatsBinlogSize sourced from the writer's
// tracked counter (GetStatsBlobSize), not from the absent arrays.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteCarriesV3StatsBlobSize() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	newSegmentID := s.task.plan.GetPreAllocatedSegmentIDs().GetBegin()
	manifestPath := packed.MarshalManifestPath(path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), common.SegmentInsertLogPath, metautil.JoinIDPath(1, 1, newSegmentID)), 1)
	// V3 simulation: no statsLog / bm25StatsLog FieldBinlogs, only the
	// writer-tracked counter has the cumulative blob size.
	writer := &fakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			100: {FieldID: 100, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 3, MemorySize: 1024}}},
		},
		statsLog:      nil,
		bm25StatsLog:  nil,
		statsBlobSize: 4096,
		manifest:      manifestPath,
		schema:        s.task.plan.GetSchema(),
	}

	newWriterPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer newWriterPatch.UnPatch()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	segment := result.GetSegments()[0]
	s.Require().NotNil(segment.GetStats())
	s.EqualValues(4096, segment.GetStats().GetStatsBinlogSize(),
		"V3 stats blob size must come from writer.GetStatsBlobSize, not the nil statslog arrays")
	s.EqualValues(1024, segment.GetStats().GetInsertBinlogSize())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteRejectsWriterStatsWithoutManifest() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	s.task.currentTime = getMilvusBirthday().Add(time.Hour)
	s.task.plan.CollectionTtl = int64(time.Minute)
	writer := &fakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{},
		statsLog:     &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: path.Join(s.task.compactionParams.StorageConfig.GetRootPath(), "_stats/bloom_filter.100/9530"), MemorySize: 64}}},
		schema:       s.task.plan.GetSchema(),
	}

	newWriterPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer newWriterPatch.UnPatch()

	_, err := s.task.Compact()
	s.ErrorContains(err, "schema bump full rewrite produced stats without manifest")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteRebuildsTextStats() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()
	requestContext := []*commonpb.KeyValuePair{{Key: "cipher-context", Value: "opaque"}}
	pluginContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 17,
		CollectionId:     1,
		EncryptionKey:    "unsafe-key",
	}
	s.task.plan.PluginContext = requestContext
	for _, field := range s.task.plan.GetSchema().GetFields() {
		if field.GetName() == "text" {
			field.TypeParams = append(field.GetTypeParams(), &commonpb.KeyValuePair{Key: "enable_match", Value: "true"})
		}
	}
	parseContext := func(got []*commonpb.KeyValuePair, collectionID int64) (*indexcgopb.StoragePluginContext, error) {
		s.Equal(requestContext, got)
		s.EqualValues(1, collectionID)
		return pluginContext, nil
	}
	parsePatch := mockey.Mock(hookutil.GetCPluginContext).To(parseContext).Build()
	defer parsePatch.UnPatch()

	var captured *indexcgopb.BuildIndexInfo
	createIndexPatch := mockey.Mock(indexcgowrapper.CreateIndex).To(func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
		captured = info
		return fakeTextIndex{}, nil
	}).Build()
	defer createIndexPatch.UnPatch()
	addStatsPatch := mockey.Mock(packed.AddStatsToManifest).To(func(manifestPath string, _ *indexpb.StorageConfig, stats []packed.StatEntry) (string, error) {
		s.Require().NotEmpty(stats)
		if stats[0].Key == "text_index.101" {
			return manifestPath + "-with-text-stats", nil
		}
		return manifestPath, nil
	}).Build()
	defer addStatsPatch.UnPatch()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	segment := result.GetSegments()[0]
	s.Require().NotNil(captured)
	s.Equal(pluginContext, captured.GetStoragePluginContext())
	s.Contains(segment.GetTextStatsLogs(), int64(101))
	s.EqualValues(42, segment.GetTextStatsLogs()[101].GetLogSize())
	files := segment.GetTextStatsLogs()[101].GetFiles()
	s.Require().NotEmpty(files)
	for _, file := range files {
		s.Equal(1, strings.Count(file, "_stats/text_index.101"))
	}
	s.Contains(segment.GetManifest(), "with-text-stats")
}

// TestFullRewriteFillsMissingNullableTextAsBinary is the real-cgo regression for the
// datanode crash-loop reported on PR #51125: adding a nullable TEXT field and then
// dropping any field routes to runFullSchemaRewrite, where the new TEXT column is
// absent from the source segment. The packed writer types TEXT columns as binary
// (WithTextRefsAsBinary), so the materializer must backfill the missing TEXT column
// as a binary null array; a utf8 fill makes array.NewRecord panic and crash-loops the
// datanode. Unlike the mocked-writer tests, this drives the actual packed writer so
// the fill type is exercised end to end.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteFillsMissingNullableTextAsBinary() {
	const newTextFieldID = int64(104)
	// Target schema gains a nullable TEXT field absent from the source segment.
	// enable_match is intentionally omitted so createTextIndex skips it and the test
	// exercises only the record-write path where the panic occurred.
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  newTextFieldID,
		Name:     "note",
		DataType: schemapb.DataType_Text,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "65535"},
		},
	})
	params, err := compaction.GenerateJSONParams(s.task.plan.GetSchema())
	s.Require().NoError(err)
	s.task.plan.JsonParams = params

	// Source segment carries a dropped field (103) -> droppedFieldIDs>0 -> full rewrite.
	s.prepareBumpSchemaVersionCompactionWithDroppedField()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
	s.Require().Len(result.GetSegments(), 1)

	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows())

	// The backfilled TEXT column must be materialized into the rewritten segment (as a
	// binary null column), either as a top-level field or a column-group child.
	found := false
	for _, fb := range segment.GetInsertLogs() {
		if fb.GetFieldID() == newTextFieldID {
			found = true
			break
		}
		for _, cf := range fb.GetChildFields() {
			if cf == newTextFieldID {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	s.True(found, "missing nullable TEXT field must be backfilled into the rewritten segment")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteMaterializesAbsentTTLDefaultWithoutSourceProbe() {
	const ttlFieldID = int64(104)
	expireAt := s.task.currentTime.Add(-time.Hour).UnixMicro()
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ttlFieldID,
		Name:     "expire_at",
		DataType: schemapb.DataType_Timestamptz,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{
			TimestamptzData: expireAt,
		}},
	})
	s.task.plan.Schema.Properties = append(s.task.plan.Schema.Properties, &commonpb.KeyValuePair{
		Key: common.CollectionTTLFieldKey, Value: "expire_at",
	})
	params, err := compaction.GenerateJSONParams(s.task.plan.GetSchema())
	s.Require().NoError(err)
	s.task.plan.JsonParams = params

	// Field 103 exists only in the source, so the schema bump takes the full
	// rewrite path while the target TTL field is still absent from that source.
	s.prepareBumpSchemaVersionCompactionWithDroppedField()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	segment := result.GetSegments()[0]
	s.EqualValues(3, segment.GetNumOfRows())

	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		typeutil.GetField(s.task.plan.GetSchema(), ttlFieldID),
	}}
	reader, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), readSchema,
		storage.WithCollectionID(1),
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(s.task.compactionParams.StorageConfig),
	)
	s.Require().NoError(err)
	defer reader.Close()
	record, err := reader.Next()
	s.Require().NoError(err)
	ttlColumn, ok := record.Column(ttlFieldID).(*array.Int64)
	s.Require().True(ok)
	s.Equal(3, ttlColumn.Len())
	// #52781 (merged): the added TTL field materializes its declared default; the
	// bump does not retroactively expire existing rows, so all three survive.
	for i := 0; i < ttlColumn.Len(); i++ {
		s.False(ttlColumn.IsNull(i))
		s.EqualValues(expireAt, ttlColumn.Value(i))
	}
}

// TestFullRewriteMergesLOBRefsBeforeBuildingTextIndex guards the ordering fixed for
// liliu-z's review. createTextIndex reads the TEXT column through the output segment's
// manifest, so applyLOBCompaction -- which merges the carried source LOB file references
// into that manifest -- must run BEFORE createTextIndex. Otherwise the text-match index
// for an out-of-line (>=64KB) TEXT field is built against a manifest that does not yet
// list the LOB files, silently missing that data. Mirrors sort/mix. We stamp the manifest
// on LOB apply and assert createTextIndex observes the stamped (LOB-merged) manifest; the
// old order (createTextIndex first) fails this assertion.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteMergesLOBRefsBeforeBuildingTextIndex() {
	s.prepareBumpSchemaVersionCompactionWithDroppedField()

	const lobStamp = "-lob-merged"
	applyPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).applyLOBCompaction).
		To(func(_ *bumpSchemaVersionCompactionTask, _ context.Context, seg *datapb.CompactionSegment) error {
			seg.Manifest = seg.GetManifest() + lobStamp
			return nil
		}).Build()
	defer applyPatch.UnPatch()

	var manifestAtTextIndex string
	textIndexPatch := mockey.Mock(createTextIndex).
		To(func(_ context.Context, _ storage.ChunkManager, _ *datapb.CompactionPlan, _ compaction.Params,
			_ int64, _ int64, _ int64, _ int64, _ int64, seg *datapb.CompactionSegment,
		) (map[int64]*datapb.TextIndexStats, error) {
			manifestAtTextIndex = seg.GetManifest()
			return map[int64]*datapb.TextIndexStats{}, nil
		}).Build()
	defer textIndexPatch.UnPatch()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Contains(manifestAtTextIndex, lobStamp,
		"applyLOBCompaction must run before createTextIndex so the text-match index is built "+
			"against a manifest that already references the carried LOB files")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSelectFullRewriteRecordDropsDeletedAndExpiredRows() {
	currentTime := getMilvusBirthday().Add(time.Hour)
	insertTs := tsoutil.ComposeTSByTime(currentTime)
	oldTs := tsoutil.ComposeTSByTime(currentTime.Add(-2 * time.Minute))
	expiredByTTLField := currentTime.Add(-time.Minute).UnixMicro()
	keptTTLField := currentTime.Add(time.Hour).UnixMicro()
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	timestampArray := newInt64Array(s.T(), []int64{int64(insertTs), int64(insertTs), int64(insertTs), int64(oldTs), int64(insertTs)})
	defer timestampArray.Release()
	pkArray := newInt64Array(s.T(), []int64{1, 2, 3, 4, 5})
	defer pkArray.Release()
	ttlArray := newInt64Array(s.T(), []int64{keptTTLField, keptTTLField, expiredByTTLField, keptTTLField, keptTTLField})
	defer ttlArray.Release()
	record := &materializerTestRecord{
		columns: map[storage.FieldID]arrow.Array{
			common.TimeStampField: timestampArray,
			100:                   pkArray,
			102:                   ttlArray,
		},
		len: 5,
	}
	deleteTs := tsoutil.ComposeTSByTime(currentTime.Add(time.Second))
	entityFilter := compaction.NewEntityFilter(map[any]typeutil.Timestamp{int64(2): deleteTs}, int64(time.Minute), currentTime, 0)

	selection, err := selectFullRewriteRecord(record, pkField, entityFilter, 102, true)
	s.Require().NoError(err)
	s.Require().NotNil(selection)
	s.Equal(2, selection.Len())
	s.Equal([]rowRange{{start: 0, end: 1}, {start: 4, end: 5}}, selection.ranges)
	s.Equal(1, entityFilter.GetDeletedCount())
	s.Equal(2, entityFilter.GetExpiredCount())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSchemaVersionBumpOnlyPreservesExistingV3Segment() {
	insertLogs := []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 11}}}}
	statsLogs := []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 12}}}}
	deltaLogs := []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 13}}}}
	expirQuantiles := []int64{100, 200, 300}
	s.task.plan.TotalRows = 9
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{{
		SegmentID:           100,
		FieldBinlogs:        insertLogs,
		Field2StatslogPaths: statsLogs,
		Deltalogs:           deltaLogs,
		InsertChannel:       "test_channel",
		StorageVersion:      storage.StorageV3,
		Manifest:            "manifest-v3",
		ExpirQuantiles:      expirQuantiles,
	}}

	result := s.task.runSchemaVersionBumpOnly()
	s.Require().Len(result.GetSegments(), 1)
	segment := result.GetSegments()[0]
	s.EqualValues(100, segment.GetSegmentID())
	s.EqualValues(9, segment.GetNumOfRows())
	s.Equal(insertLogs, segment.GetInsertLogs())
	s.Equal(statsLogs, segment.GetField2StatslogPaths())
	s.Equal(deltaLogs, segment.GetDeltalogs())
	s.Equal("test_channel", segment.GetChannel())
	s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
	s.Equal("manifest-v3", segment.GetManifest())
	s.Equal(expirQuantiles, segment.GetExpirQuantiles())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestEmptySegmentWithMissingFieldUsesAdditiveReconciliation() {
	s.setupTest()
	s.task.plan.TotalRows = 0
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
	}
	manifestPatch := mockey.Mock(packed.GetManifestFieldIDs).Return(existingFields, nil).Build()
	defer manifestPatch.UnPatch()

	additiveCalled := false
	additivePatch := mockey.Mock((*bumpSchemaVersionCompactionTask).runAdditivePhysicalReconciliation).To(
		func(_ *bumpSchemaVersionCompactionTask, _ context.Context, diff *schemaBumpPhysicalDiff) (*datapb.CompactionPlanResult, error) {
			additiveCalled = true
			s.Equal([]int64{102}, lo.Map(diff.missingOutputFields, func(field *schemapb.FieldSchema, _ int) int64 {
				return field.GetFieldID()
			}))
			return &datapb.CompactionPlanResult{State: datapb.CompactionTaskState_completed}, nil
		},
	).Build()
	defer additivePatch.UnPatch()

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.True(additiveCalled)
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestEmptySegmentWithDroppedFieldStillRoutesFullRewrite() {
	s.setupTest()
	s.task.plan.TotalRows = 0
	const droppedFieldID = int64(103)
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
		102:                   {},
		droppedFieldID:        {},
	}
	manifestPatch := mockey.Mock(packed.GetManifestFieldIDs).Return(existingFields, nil).Build()
	defer manifestPatch.UnPatch()

	fullRewriteCalled := false
	rewritePatch := mockey.Mock((*bumpSchemaVersionCompactionTask).runFullSchemaRewrite).To(
		func(_ *bumpSchemaVersionCompactionTask, got map[int64]struct{}) (*datapb.CompactionPlanResult, error) {
			fullRewriteCalled = true
			s.Contains(got, droppedFieldID)
			return &datapb.CompactionPlanResult{State: datapb.CompactionTaskState_completed}, nil
		},
	).Build()
	defer rewritePatch.UnPatch()

	_, err := s.task.Compact()
	s.Require().NoError(err)
	s.True(fullRewriteCalled)
}

// A zero-row additive reconciliation must not fabricate physical completeness
// by writing an empty materialized record: it fails as a data-integrity error
// and the writer lease aborts the partial writer.
func (s *BumpSchemaVersionCompactionTaskSuite) TestEmptyAdditiveReconciliationFailsAndAbortsWriter() {
	const ordinaryFieldID = int64(103)
	s.task.plan.TotalRows = 0
	s.task.plan.Schema.Functions = nil
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields[:4], &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added_nullable",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	segment := s.task.plan.GetSegmentBinlogs()[0]
	segment.StorageVersion = storage.StorageV3
	segment.Manifest = packed.MarshalManifestPath("/data/segments/1", 10)

	diff := &schemaBumpPhysicalDiff{
		existingFields: map[int64]struct{}{
			common.RowIDField:     {},
			common.TimeStampField: {},
			100:                   {},
			101:                   {},
		},
		absentOrdinaryFields: []*schemapb.FieldSchema{typeutil.GetField(s.task.plan.GetSchema(), ordinaryFieldID)},
	}
	reader := &emptyRecordReader{}
	readerPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).openRecordReader).Return(reader, diff.existingFields, nil).Build()
	defer readerPatch.UnPatch()

	writer := &cleanupTrackingBatchWriter{}
	writerPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).setupWriter).Return(
		&bumpSchemaVersionWriterResult{
			writer: writer,
			columnGroups: []storagecommon.ColumnGroup{{
				GroupID: ordinaryFieldID,
				Columns: []int{0},
				Fields:  []int64{ordinaryFieldID},
			}},
			storageVersion: storage.StorageV3,
			basePath:       "/data/segments/1",
			baseVersion:    10,
		}, nil,
	).Build()
	defer writerPatch.UnPatch()

	_, err := s.task.runAdditivePhysicalReconciliation(context.Background(), diff)
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "observed zero rows")
	s.Empty(writer.writeRows)
	s.Zero(writer.closeCount)
	s.Equal(1, writer.abortCount)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFullRewriteRequiresPreallocatedSegmentID() {
	s.task.plan.PreAllocatedSegmentIDs = nil
	_, err := s.task.fullRewriteSegmentID()
	s.ErrorContains(err, "pre-allocated segment ID")

	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 10, End: 10}
	_, err = s.task.fullRewriteSegmentID()
	s.ErrorContains(err, "pre-allocated segment ID")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAppendBM25StatsFromArrowArrayRejectsNonBinary() {
	arr := newInt64Array(s.T(), []int64{1})
	defer arr.Release()

	_, err := appendBM25StatsFromArrowArray(storage.NewBM25Stats(), arr)
	s.ErrorContains(err, "arrow binary array")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionWithoutFunctions() {
	s.prepareBumpSchemaVersionCompaction()

	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{}
	s.task.plan.Schema.Fields = s.task.plan.Schema.Fields[:4]
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionInvalidInputField() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with wrong input field type (Int64 instead of VarChar)
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{100}, // Int64 field instead of VarChar
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "must be VarChar for schema-bump materialization")
}

// Schema-bump materialization reads persisted inputs, and a sealed StorageV3
// Text column comes back as encoded LOB references that the runner cannot
// decode, so the compaction-side contract admits only VarChar inputs even
// though collection-creation validation admits Text.
func (s *BumpSchemaVersionCompactionTaskSuite) TestValidateMaterializationInputFieldRejectsTextInput() {
	textField := &schemapb.FieldSchema{FieldID: 101, Name: "content", DataType: schemapb.DataType_Text}
	for _, functionType := range []schemapb.FunctionType{schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash} {
		err := validateMaterializationInputField(&schemapb.FunctionSchema{Name: "f", Type: functionType}, textField)
		s.Require().Error(err, functionType.String())
		s.ErrorIs(err, merr.ErrDataIntegrity)
		s.ErrorContains(err, "must be VarChar for schema-bump materialization")
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionInvalidOutputField() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with a missing output whose persisted field has the wrong type.
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{104},
	}}
	// The fixture's field 102 is no longer produced by any function; unmark it
	// so this test exercises output-type validation, not orphan detection.
	typeutil.GetField(s.task.plan.GetSchema(), 102).IsFunctionOutput = false
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID: 104, Name: "bad_output", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true,
	})

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "must be SparseFloatVector")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestValidateSupportedMissingFunctionMaterializationAllowsMultipleInputFields() {
	err := validateSupportedMissingFunctionMaterialization(&schemapb.FunctionSchema{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101, 115},
		OutputFieldIds: []int64{102},
	})
	s.NoError(err)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionInputSchemaIncludesAdditionalInputFields() {
	s.task.plan.Schema = &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
					{Key: common.EnableAnalyzerKey, Value: "true"},
					{Key: "multi_analyzer_params", Value: `{"by_field":"lang","analyzers":{"default":{"type":"standard"},"english":{"type":"english"}}}`},
				},
			},
			{FieldID: 115, Name: "lang", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "32"}}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	missingFunctions := []*schemapb.FunctionSchema{{
		Name:             "BM25",
		Type:             schemapb.FunctionType_BM25,
		InputFieldNames:  []string{"text"},
		InputFieldIds:    []int64{101},
		OutputFieldNames: []string{"sparse"},
		OutputFieldIds:   []int64{102},
	}}

	inputSchema, inputFieldIDs, err := s.task.missingFunctionInputSchema(missingFunctions)
	s.NoError(err)
	s.ElementsMatch([]int64{101, 115}, inputFieldIDs)
	s.Require().Len(inputSchema.GetFields(), 2)
	s.ElementsMatch([]string{"text", "lang"}, []string{inputSchema.GetFields()[0].GetName(), inputSchema.GetFields()[1].GetName()})
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionMissingOutputFieldInMultiOutputFunction() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with one missing output field ID
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102, 114}, // 114 does not exist
	}}

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "output field 114 not found in persisted schema")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionInputFieldNotFound() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with non-existent input field ID
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{999}, // Non-existent field ID
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "input field 999 not found in persisted schema")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionOutputFieldNotFound() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with non-existent output field ID
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{999}, // Non-existent field ID
	}}
	// Field 102 is no longer produced by any function; unmark it so this test
	// exercises the output-not-found path, not orphan detection.
	typeutil.GetField(s.task.plan.GetSchema(), 102).IsFunctionOutput = false

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "output field 999 not found in persisted schema")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionUnsupportedFunctionType() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with unsupported function type
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "Unknown",
		Type:           schemapb.FunctionType_Unknown,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "unsupported materialization type")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionEmptySegmentBinlogs() {
	// Test with empty segment binlogs
	s.task.plan.SegmentBinlogs = nil

	_, err := s.task.Compact()
	s.Error(err)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionRejectsNonV3Segment() {
	s.prepareBumpSchemaVersionCompaction()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      100,
			FieldBinlogs:   []*datapb.FieldBinlog{}, // Empty field binlogs
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV2,
		},
	}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "requires a StorageV3 segment with manifest")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionMultipleSegments() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with multiple segments (should fail, must have exactly one)
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      100,
			FieldBinlogs:   []*datapb.FieldBinlog{{FieldID: 101}},
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV2,
		},
		{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      200,
			FieldBinlogs:   []*datapb.FieldBinlog{{FieldID: 101}},
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV2,
		},
	}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "must have exactly one segment")
	s.Contains(err.Error(), "but got 2 segments")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionContextCanceled() {
	// Cancel context before compact (before prepareBumpSchemaVersionCompaction)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.task.ctx = ctx
	s.task.cancel = cancel

	// Compact should detect context cancel at the beginning
	_, err := s.task.Compact()
	s.Error(err)
	s.ErrorIs(err, context.Canceled)
}

func TestBumpSchemaVersionCompactionTaskBasic(t *testing.T) {
	ctx := context.Background()

	meta := genTestCollectionMetaWithBM25()
	params, err := compaction.GenerateJSONParams(meta.GetSchema())
	assert.NoError(t, err)

	plan := &datapb.CompactionPlan{
		PlanID: 1000,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      200,
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV3,
			Manifest:       "manifest",
		}},
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:                 meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		Functions: []*schemapb.FunctionSchema{{
			Name:           "BM25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
		TotalRows: 0,
		Channel:   "test_channel",
		SlotUsage: 1,
	}

	mockCM := mock_storage.NewMockChunkManager(t)
	task := NewBumpSchemaVersionCompactionTask(ctx, mockCM, plan, compaction.GenParams())
	assert.NotNil(t, task)
	assert.Equal(t, int64(1000), task.GetPlanID())
	assert.Equal(t, int64(1), task.GetCollection())
	assert.Equal(t, "test_channel", task.GetChannelName())
	assert.Equal(t, datapb.CompactionType_BumpSchemaVersionCompaction, task.GetCompactionType())
	assert.Equal(t, int64(1), task.GetSlotUsage())
}

// TestBuildNewInsertLogsV3ReturnsOnlyNewGroups pins that the result carries
// only what this run wrote. The plan's FieldBinlogs are irrelevant — for a
// StorageV3 segment recovered after a DataCoord restart they are empty. The
// returned groups are what the receiver merges onto SegmentInfo.Binlogs, and
// that merge is what makes a materialized function-output field eligible for
// index creation (see completeBumpSchemaVersionCompactionMutation in
// internal/datacoord/meta.go); it must not be dropped.
func (s *BumpSchemaVersionCompactionTaskSuite) TestBuildNewInsertLogsV3ReturnsOnlyNewGroups() {
	s.setupTest()

	writerResult := &bumpSchemaVersionWriterResult{
		columnGroups: []storagecommon.ColumnGroup{{GroupID: 102, Fields: []int64{102}}},
	}

	newInsert, err := s.task.buildNewInsertLogsV3(writerResult, map[int64]int{102: 512}, 1000)
	s.NoError(err)

	s.Require().Len(newInsert, 1)
	s.EqualValues(102, newInsert[0].GetFieldID())
	s.EqualValues(512, newInsert[0].GetBinlogs()[0].GetMemorySize())
	s.EqualValues(1000, newInsert[0].GetBinlogs()[0].GetEntriesNum())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSchemaBumpPhysicalDiffIncludesOrdinaryAndFunctionOutputFields() {
	s.setupTest()
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  ordinaryFieldID,
		Name:     "added",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
	}
	manifestPatch := mockey.Mock(packed.GetManifestFieldIDs).Return(existingFields, nil).Build()
	defer manifestPatch.UnPatch()

	diff, err := s.task.schemaBumpDecision()
	s.Require().NoError(err)
	s.Empty(diff.droppedFieldIDs)
	s.Require().Len(diff.absentOrdinaryFields, 1)
	s.EqualValues(ordinaryFieldID, diff.absentOrdinaryFields[0].GetFieldID())
	s.Require().Len(diff.missingOutputFields, 1)
	s.EqualValues(102, diff.missingOutputFields[0].GetFieldID())
	s.Require().Len(diff.missingFunctions, 1)
	s.Equal(existingFields, diff.existingFields)

	appended := diff.appendedFields()
	s.Require().Len(appended, 2)
	s.EqualValues(ordinaryFieldID, appended[0].GetFieldID())
	s.EqualValues(102, appended[1].GetFieldID())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSchemaBumpPhysicalDiffSkipsPresentOutputWhenOrdinaryFieldIsAbsent() {
	s.setupTest()
	const ordinaryFieldID = int64(103)
	s.task.plan.Schema.Fields = append(s.task.plan.Schema.Fields, &schemapb.FieldSchema{
		FieldID: ordinaryFieldID, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	})
	existingFields := map[int64]struct{}{
		common.RowIDField: {}, common.TimeStampField: {}, 100: {}, 101: {}, 102: {},
	}
	manifestPatch := mockey.Mock(packed.GetManifestFieldIDs).Return(existingFields, nil).Build()
	defer manifestPatch.UnPatch()

	diff, err := s.task.schemaBumpDecision()
	s.Require().NoError(err)
	s.Require().Len(diff.absentOrdinaryFields, 1)
	s.EqualValues(ordinaryFieldID, diff.absentOrdinaryFields[0].GetFieldID())
	s.Empty(diff.missingOutputFields)
	s.Empty(diff.missingFunctions)
	s.Equal([]int64{ordinaryFieldID}, lo.Map(diff.appendedFields(), func(field *schemapb.FieldSchema, _ int) int64 {
		return field.GetFieldID()
	}))
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReadSchemaAnchorPlusReaderFilledAbsents() {
	s.setupTest()
	var absentInput *schemapb.FieldSchema
	for _, field := range s.task.plan.GetSchema().GetFields() {
		if field.GetFieldID() == 101 {
			absentInput = field
		}
	}
	s.Require().NotNil(absentInput)
	diff := &schemaBumpPhysicalDiff{
		existingFields: map[int64]struct{}{
			common.RowIDField:     {},
			common.TimeStampField: {},
			100:                   {},
			// Function input 101 is intentionally absent.
		},
		missingFunctions:     s.task.plan.GetSchema().GetFunctions(),
		absentOrdinaryFields: []*schemapb.FieldSchema{absentInput},
	}

	readSchema, logicalInputFieldIDs, err := s.task.additiveReadSchema(diff)
	s.Require().NoError(err)
	s.Equal([]int64{101}, logicalInputFieldIDs)
	// The physical anchor is read as-is; the absent ordinary input stays in the
	// read schema so the reader fills it (default/null) for function execution.
	s.ElementsMatch([]int64{common.RowIDField, 101}, lo.Map(readSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 {
		return field.GetFieldID()
	}))
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestAdditiveReadSchemaRejectsMissingSystemAnchor() {
	s.setupTest()
	_, _, err := s.task.additiveReadSchema(&schemaBumpPhysicalDiff{
		existingFields: map[int64]struct{}{100: {}},
	})
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "physical RowID or Timestamp anchor")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSchemaContainsTextField() {
	s.False(schemaContainsTextField(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64},
	}}))
	s.True(schemaContainsTextField(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_Text},
	}}))
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestSchemaBumpPhysicalDiffRejectsOrphanFunctionOutput() {
	s.setupTest()
	s.task.plan.Schema.Functions = nil
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
	}
	manifestPatch := mockey.Mock(packed.GetManifestFieldIDs).Return(existingFields, nil).Build()
	defer manifestPatch.UnPatch()

	_, err := s.task.schemaBumpDecision()
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "has no producing function")
}

// TestMaterializationStatsDeltaIgnoresPlanArrays is the regression test for
// issue #51729: the increment must be identical whether or not the plan
// carries the input segment's FieldBinlog arrays. After #50410 a recovered
// StorageV3 segment ships empty arrays, and the old absolute-Stats
// implementation silently undercounted the whole segment.
func (s *BumpSchemaVersionCompactionTaskSuite) TestMaterializationStatsDeltaIgnoresPlanArrays() {
	s.setupTest()

	columnGroups := []storagecommon.ColumnGroup{{GroupID: 102, Fields: []int64{102}}}
	memorySizes := map[int64]int{102: 4096}
	nullCounts := map[int64]int64{102: 0}

	// The builder takes no plan input at all, which is the property under test:
	// there is no argument through which an empty or populated plan array could
	// change the answer.
	got := buildMaterializationStatsDelta(columnGroups, memorySizes, nullCounts, 256)

	s.EqualValues(4096, got.GetInsertBinlogSize())
	s.EqualValues(1, got.GetInsertBinlogCount())
	s.EqualValues(256, got.GetStatsBinlogSize())
	s.EqualValues(0, got.GetDeleteNumRows())
	s.EqualValues(0, got.GetDeltaBinlogSize())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionMaterializationsRejectsPartialState() {
	s.setupTest()
	schema := schemaBumpMultiOutputBM25Schema()
	s.task.plan.Schema = schema
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
		102:                   {},
	}

	_, _, err := missingFunctionMaterializations(schema, existingFields)
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "partially materialized output fields")
}

// Existing BM25 fixtures expose one output; these tests need a partial multi-output state.
func schemaBumpMultiOutputBM25Schema() *schemapb.CollectionSchema {
	fields := append(schemaBumpBaseFields(),
		&schemapb.FieldSchema{
			FieldID:  102,
			Name:     "sparse_1",
			DataType: schemapb.DataType_SparseFloatVector,
		},
		&schemapb.FieldSchema{
			FieldID:  103,
			Name:     "sparse_2",
			DataType: schemapb.DataType_SparseFloatVector,
		},
	)
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		Fields:      fields,
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Id:               100,
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			InputFieldIds:    []int64{101},
			OutputFieldNames: []string{"sparse_1", "sparse_2"},
			OutputFieldIds:   []int64{102, 103},
		}},
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestCompleteAndStop() {
	s.setupTest()
	s.task.Complete()
	s.task.Stop()
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestGetStorageConfig() {
	s.setupTest()
	s.NotNil(s.task.GetStorageConfig())
}

// TestMaterializationRejectsDroppedFields pins the add-only invariant the
// stats increment depends on. Dropped fields must route to
// runFullSchemaRewrite, which ships an absolute Stats; reaching
// materialization with drops would produce an increment that over-estimates
// the segment, because the removed columns' bytes are never subtracted.
func (s *BumpSchemaVersionCompactionTaskSuite) TestMaterializationRejectsDroppedFields() {
	s.setupTest()

	_, err := s.task.runAdditivePhysicalReconciliation(
		context.Background(),
		&schemaBumpPhysicalDiff{droppedFieldIDs: []int64{101}},
	)
	s.Error(err)
	s.ErrorIs(err, merr.ErrServiceInternal)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionInputSchemaMalformedMultiAnalyzerParamsIsDataIntegrityError() {
	functionSchema := &schemapb.FunctionSchema{
		Name: "BM25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
	}
	s.task.plan.Schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.EnableAnalyzerKey, Value: "true"},
				{Key: "multi_analyzer_params", Value: "{bad"},
			}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}

	_, _, err := s.task.missingFunctionInputSchema([]*schemapb.FunctionSchema{functionSchema})
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "failed to parse multi_analyzer_params for function BM25")
	s.ErrorContains(err, "invalid character")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionInputSchemaByFieldWrongTypeIsDataIntegrityError() {
	functionSchema := &schemapb.FunctionSchema{
		Name: "BM25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
	}
	// DDL only admits a VarChar by_field; a resolved by_field of another type
	// is persisted-schema corruption, classified the same as the other branches.
	s.task.plan.Schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.EnableAnalyzerKey, Value: "true"},
				{Key: "multi_analyzer_params", Value: `{"by_field":"lang"}`},
			}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 103, Name: "lang", DataType: schemapb.DataType_Int64},
		},
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}

	_, _, err := s.task.missingFunctionInputSchema([]*schemapb.FunctionSchema{functionSchema})
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrDataIntegrity)
	s.ErrorContains(err, "references by_field lang of type")
	s.ErrorContains(err, "only VarChar is allowed")
}

// TestNewCompactionSegmentRecordReaderWithFieldsThreadsPresentFields pins that on
// the manifest path the compactor hands its already-computed existingFields to the
// reader via WithPresentFields, so the reader does not re-open the manifest with a
// second GetManifestFieldIDs (the P5 perf fix).
func TestNewCompactionSegmentRecordReaderWithFieldsThreadsPresentFields(t *testing.T) {
	seg := &datapb.CompactionSegmentBinlogs{Manifest: "manifest-json"}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
	}}
	existing := map[int64]struct{}{100: {}}

	mockEnc := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(false).Build()
	defer mockEnc.UnPatch()
	mockFieldIDs := mockey.Mock(packed.GetManifestFieldIDs).To(
		func(string, *indexpb.StorageConfig) (map[int64]struct{}, error) {
			t.Fatal("reader must not re-derive present fields; the compactor already passed existingFields")
			return nil, nil
		}).Build()
	defer mockFieldIDs.UnPatch()
	mockReader := mockey.Mock(storage.NewRecordReaderFromManifest).Return(nil, nil).Build()
	defer mockReader.UnPatch()

	_, got, err := newCompactionSegmentRecordReaderWithFields(context.Background(), seg, schema,
		&indexpb.StorageConfig{RootPath: "root"}, existing,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(&indexpb.StorageConfig{RootPath: "root"}),
	)
	assert.NoError(t, err)
	assert.Equal(t, existing, got)
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionInputSchemaIncludesMultiAnalyzerByField() {
	functionSchema := &schemapb.FunctionSchema{
		Name: "BM25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
	}
	s.task.plan.Schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.EnableAnalyzerKey, Value: "true"},
				{Key: "multi_analyzer_params", Value: `{"by_field":"lang"}`},
			}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 103, Name: "lang", DataType: schemapb.DataType_VarChar, Nullable: true},
		},
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}

	inputSchema, fieldIDs, err := s.task.missingFunctionInputSchema([]*schemapb.FunctionSchema{functionSchema})
	s.Require().NoError(err)
	s.Equal([]int64{101, 103}, fieldIDs)
	s.Equal([]int64{101, 103}, lo.Map(inputSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 {
		return field.GetFieldID()
	}))
}
