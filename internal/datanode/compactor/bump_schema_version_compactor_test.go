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
	"math"
	"path"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
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
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	schema           *schemapb.CollectionSchema
	writtenTimestamp []int64
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
	for _, field := range s.task.plan.GetSchema().GetFields() {
		if field.GetName() == "text" {
			field.TypeParams = append(field.GetTypeParams(), &commonpb.KeyValuePair{Key: "enable_match", Value: "true"})
		}
	}

	createIndexPatch := mockey.Mock(indexcgowrapper.CreateIndex).To(func(context.Context, *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
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
	s.Contains(segment.GetTextStatsLogs(), int64(101))
	s.EqualValues(42, segment.GetTextStatsLogs()[101].GetLogSize())
	s.Contains(segment.GetManifest(), "with-text-stats")
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

	selection, ttlValues, err := selectFullRewriteRecord(record, pkField, entityFilter, 102, true, nil)
	s.Require().NoError(err)
	s.Require().NotNil(selection)
	s.Equal(2, selection.Len())
	s.Equal([]rowRange{{start: 0, end: 1}, {start: 4, end: 5}}, selection.ranges)
	s.Equal([]int64{keptTTLField, keptTTLField}, ttlValues)
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

func (s *BumpSchemaVersionCompactionTaskSuite) TestPreserveDeltaLogsV3CountMismatch() {
	patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string{"delta-1", "delta-2"}, nil).Build()
	defer patch.UnPatch()

	manifestPath, err := s.task.preserveDeltaLogsV3(&datapb.CompactionSegmentBinlogs{
		Manifest: "old-manifest",
		Deltalogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 10}}},
		},
	}, packed.MarshalManifestPath("/data/segments/1", 10))

	s.Empty(manifestPath)
	s.ErrorContains(err, "V3 delta manifest path count 2 does not match segment delta summary count 1")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionWithoutFunctions() {
	s.prepareBumpSchemaVersionCompaction()

	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{}
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
	s.Error(err)
	s.Contains(err.Error(), "input field data type must be varchar")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBumpSchemaVersionCompactionInvalidOutputField() {
	s.prepareBumpSchemaVersionCompaction()

	// Test with wrong output field type (VarChar instead of SparseFloatVector)
	s.task.plan.Schema.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{101, 102},
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "output field data type must be sparse float vector")
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
	s.Error(err)
	s.Contains(err.Error(), "output field not found in schema")
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
	s.Error(err)
	s.Contains(err.Error(), "input field not found in schema")
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

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "output field not found in schema")
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
	s.Error(err)
	s.Contains(err.Error(), "unsupported function type")
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

func (s *BumpSchemaVersionCompactionTaskSuite) TestFinalizeMergedLogs() {
	s.setupTest()

	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "original/100", EntriesNum: 1000},
				},
			},
		},
	}
	newInsertLogs := map[int64]*datapb.FieldBinlog{
		102: {
			FieldID: 102,
			Binlogs: []*datapb.Binlog{
				{LogPath: "new/102", EntriesNum: 1000, MemorySize: 500},
			},
		},
	}
	mergedInsert, err := s.task.finalizeMergedLogs(segment, newInsertLogs)
	s.NoError(err)

	s.Equal(2, len(mergedInsert))
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())
}

// TestFinalizeMergedLogsCrashReplay verifies that retrying finalizeMergedLogs replaces rewritten output logs without duplicates.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFinalizeMergedLogsCrashReplay() {
	s.setupTest()

	// Segment state after first successful run: field 102 already present in etcd.
	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100", EntriesNum: 1000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: "first-run/102", EntriesNum: 1000, LogID: 99}}},
		},
	}
	// Second run allocates a new logID for the same output field.
	newInsertLogs := map[int64]*datapb.FieldBinlog{
		102: {FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: "second-run/102", EntriesNum: 1000, LogID: 100}}},
	}

	mergedInsert, err := s.task.finalizeMergedLogs(segment, newInsertLogs)
	s.NoError(err)

	s.Require().Equal(2, len(mergedInsert))
	var fieldIDs []int64
	for _, fb := range mergedInsert {
		fieldIDs = append(fieldIDs, fb.GetFieldID())
	}
	s.ElementsMatch([]int64{100, 102}, fieldIDs)
	for _, fb := range mergedInsert {
		if fb.GetFieldID() == 102 {
			s.Equal("second-run/102", fb.GetBinlogs()[0].GetLogPath())
		}
	}
}

// TestFinalizeMergedLogsV3CrashReplay verifies V3 column-group replacement through ChildFields.
func (s *BumpSchemaVersionCompactionTaskSuite) TestFinalizeMergedLogsV3CrashReplay() {
	s.setupTest()

	// V3: column group 102 already present in segment after first run.
	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100"}}},
			{FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogPath: "first-run/cg102"}}},
		},
	}
	newInsertLogs := map[int64]*datapb.FieldBinlog{
		102: {FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogPath: "second-run/cg102"}}},
	}

	mergedInsert, err := s.task.finalizeMergedLogs(segment, newInsertLogs)
	s.NoError(err)

	s.Require().Equal(2, len(mergedInsert))
	for _, fb := range mergedInsert {
		if fb.GetFieldID() == 102 {
			s.Equal("second-run/cg102", fb.GetBinlogs()[0].GetLogPath())
		}
	}
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestBuildMergedLogsV3() {
	s.setupTest()

	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100", EntriesNum: 1000}}},
		},
	}
	writerResult := &bumpSchemaVersionWriterResult{
		columnGroups: []storagecommon.ColumnGroup{
			{GroupID: 102, Fields: []int64{102}, Format: "vortex"},
		},
	}

	mergedInsert, err := s.task.buildMergedLogsV3(segment, writerResult, map[int64]int{102: 512}, 1000)
	s.NoError(err)

	// Original + new
	s.Equal(2, len(mergedInsert))
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())
	s.Equal("vortex", mergedInsert[1].GetFormat())
	s.Equal(int64(512), mergedInsert[1].GetBinlogs()[0].GetMemorySize())
	s.Equal(int64(1000), mergedInsert[1].GetBinlogs()[0].GetEntriesNum())
	// V3 binlog presence marker must carry a non-zero LogID so buildBinlogKvs validation
	// passes (requires LogID!=0 AND LogPath=="" for V3 segments).
	s.NotZero(mergedInsert[1].GetBinlogs()[0].GetLogID(),
		"V3 schema bump binlog presence marker must have non-zero LogID")
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestMissingFunctionOutputFieldsSelectsAllOutputsOnPartialState() {
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

	outputFields, outputFieldIDs, err := s.task.missingFunctionOutputFields(schema.GetFunctions(), existingFields)
	s.NoError(err)

	s.Equal([]int64{102, 103}, outputFieldIDs)
	s.Require().Len(outputFields, 2)
	s.Equal(int64(102), outputFields[0].GetFieldID())
	s.Equal(int64(103), outputFields[1].GetFieldID())
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestPartialMaterializerExistingFieldsTreatsAllFunctionOutputsAsMissing() {
	schema := schemaBumpMultiOutputBM25Schema()
	existingFields := map[int64]struct{}{
		common.RowIDField:     {},
		common.TimeStampField: {},
		100:                   {},
		101:                   {},
		102:                   {},
	}

	materializerFields := partialMaterializerExistingFields(schema, schema.GetFunctions(), existingFields)

	s.NotContains(materializerFields, int64(102))
	s.NotContains(materializerFields, int64(103))
}

func (s *BumpSchemaVersionCompactionTaskSuite) TestFinalizeMergedLogsReplacesExistingOutputFieldLogs() {
	s.setupTest()

	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100"}}},
			{FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogPath: "old/cg102"}}},
		},
	}
	newInsertLogs := map[int64]*datapb.FieldBinlog{
		102: {FieldID: 102, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{LogPath: "new/cg102"}}},
		103: {FieldID: 103, ChildFields: []int64{103}, Binlogs: []*datapb.Binlog{{LogPath: "new/cg103"}}},
	}

	mergedInsert, err := s.task.finalizeMergedLogs(segment, newInsertLogs)
	s.NoError(err)

	s.Require().Len(mergedInsert, 3)
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal("original/100", mergedInsert[0].GetBinlogs()[0].GetLogPath())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())
	s.Equal("new/cg102", mergedInsert[1].GetBinlogs()[0].GetLogPath())
	s.Equal(int64(103), mergedInsert[2].GetFieldID())
	s.Equal("new/cg103", mergedInsert[2].GetBinlogs()[0].GetLogPath())
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
