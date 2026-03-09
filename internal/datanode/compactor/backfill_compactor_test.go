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
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	mock_storage "github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestBackfillCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(BackfillCompactionTaskSuite))
}

type BackfillCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO

	meta           *etcdpb.CollectionMeta
	multiSegWriter *MultiSegmentWriter

	task *backfillCompactionTask
}

func (s *BackfillCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *BackfillCompactionTaskSuite) setupTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())

	s.meta = genTestCollectionMetaWithBM25()

	params, err := compaction.GenerateJSONParams()
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
			StorageVersion:      storage.StorageV2,
		}},
		Type:                   datapb.CompactionType_BackfillCompaction,
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
	s.task = NewBackfillCompactionTask(context.Background(), cm, plan, compaction.GenParams())
}

func (s *BackfillCompactionTaskSuite) SetupTest() {
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

func (s *BackfillCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	paramtable.Get().Reset("common.storage.enablev2")
	initcore.CleanArrowFileSystemSingleton()
}

func (s *BackfillCompactionTaskSuite) prepareBackfillCompaction() {
	segID := int64(100)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create multi segment writer with varchar field (input) but without sparse vector field (output)
	// This simulates a segment that needs backfill
	s.initSegBufferForBackfill(segID)

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

	// Write blobs to disk from the uploaded blobs
	// Note: MultiSegmentWriter handles upload internally, but we can still write to disk for testing
	log.Info("created segment with MultiSegmentWriter",
		zap.Int64("segmentID", segID),
		zap.Int("numRows", int(segment.GetNumOfRows())),
		zap.Int("insertLogsCount", len(segment.GetInsertLogs())))
}

func (s *BackfillCompactionTaskSuite) initSegBufferForBackfill(segID int64) {
	// Create schema without sparse vector field (output field)
	// Only include input field (varchar)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
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
		},
	}

	// Create allocator for MultiSegmentWriter
	// Use segID as the starting point for segmentID allocation to ensure consistent paths
	segIDAlloc := allocator.NewLocalAllocator(segID, math.MaxInt64)
	logIDAlloc := allocator.NewLocalAllocator(9530, 19530)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)

	// Create MultiSegmentWriter with StorageV2
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
		storage.WithVersion(storage.StorageV2),
	)
	s.Require().NoError(err)

	// Write test data with varchar field
	for i := 0; i < 3; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(segID + int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value: map[int64]interface{}{
				common.RowIDField:     segID + int64(i),
				common.TimeStampField: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
				100:                   segID + int64(i),
				101:                   "test string " + string(rune('0'+i)),
			},
		}
		err = multiSegWriter.WriteValue(&v)
		s.Require().NoError(err)
	}

	s.multiSegWriter = multiSegWriter
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionSuccess() {
	s.prepareBackfillCompaction()

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
	// For V3 segments (with manifest), BM25 stats are embedded in the manifest
	// atomically with the column groups. PackSegmentLoadInfo relies on StatsResolver
	// to read them from the manifest instead of Bm25Logs.
	// For V2 segments (no manifest), BM25 stats are returned in Bm25Logs as usual.
	if segment.GetManifest() != "" {
		s.Empty(segment.GetBm25Logs())
		s.NotEmpty(segment.GetManifest())
	} else {
		s.NotEmpty(segment.GetBm25Logs())
		s.Equal(1, len(segment.GetBm25Logs()))
		// V2 BM25 stats binlog must have non-zero LogID so buildBinlogKvs does not reject it.
		s.NotZero(segment.GetBm25Logs()[0].GetBinlogs()[0].GetLogID(),
			"V2 BM25 stats binlog must have non-zero LogID")
	}

	// The backfilled output field (ID=102) must carry a non-zero LogID in its insert binlog.
	// buildBinlogKvs requires LogID!=0 or LogPath!="" for V2; this guards against regression
	// where LogID is accidentally left at zero (pre-existing fields are exempt — they were
	// written by the test setup without LogID allocation).
	const backfillOutputFieldID = int64(102)
	for _, fl := range segment.GetInsertLogs() {
		if fl.GetFieldID() == backfillOutputFieldID {
			s.Require().NotEmpty(fl.GetBinlogs(), "backfilled output field must have at least one binlog entry")
			s.NotZero(fl.GetBinlogs()[0].GetLogID(),
				"V2 backfill insert binlog (fieldID=%d) must have non-zero LogID", backfillOutputFieldID)
		}
	}
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionInvalidFunctionCount() {
	s.prepareBackfillCompaction()

	// Test with zero functions
	s.task.plan.Functions = []*schemapb.FunctionSchema{}
	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "backfill functions should be exactly one")

	// Test with multiple functions
	s.task.plan.Functions = []*schemapb.FunctionSchema{
		{Type: schemapb.FunctionType_BM25},
		{Type: schemapb.FunctionType_BM25},
	}
	_, err = s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "backfill functions should be exactly one")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionInvalidInputField() {
	s.prepareBackfillCompaction()

	// Test with wrong input field type (Int64 instead of VarChar)
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{100}, // Int64 field instead of VarChar
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "input field data type must be varchar")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionInvalidOutputField() {
	s.prepareBackfillCompaction()

	// Test with wrong output field type (VarChar instead of SparseFloatVector)
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{101}, // VarChar field instead of SparseFloatVector
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "output field data type must be sparse float vector")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionMultipleInputFields() {
	s.prepareBackfillCompaction()

	// Test with multiple input fields
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101, 115}, // Multiple input fields
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "bm25 function should have exactly one input field")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionMultipleOutputFields() {
	s.prepareBackfillCompaction()

	// Test with multiple output fields
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102, 114}, // Multiple output fields
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "bm25 function should only have one output field")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionInputFieldNotFound() {
	s.prepareBackfillCompaction()

	// Test with non-existent input field ID
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{999}, // Non-existent field ID
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "input field not found in schema")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionOutputFieldNotFound() {
	s.prepareBackfillCompaction()

	// Test with non-existent output field ID
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "BM25",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{999}, // Non-existent field ID
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "no output field")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionUnsupportedFunctionType() {
	s.prepareBackfillCompaction()

	// Test with unsupported function type
	s.task.plan.Functions = []*schemapb.FunctionSchema{{
		Name:           "Unknown",
		Type:           schemapb.FunctionType_Unknown,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}

	_, err := s.task.Compact()
	s.Error(err)
	s.Contains(err.Error(), "unknown functionRunner type")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionEmptySegmentBinlogs() {
	// Test with empty segment binlogs
	s.task.plan.SegmentBinlogs = nil

	_, err := s.task.Compact()
	s.Error(err)
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionEmptyColumnGroups() {
	s.prepareBackfillCompaction()

	// Create segment binlogs with empty field binlogs to trigger empty field binlogs error
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
	s.Contains(err.Error(), "segment's field binlogs are empty")
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionMultipleSegments() {
	s.prepareBackfillCompaction()

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

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionContextCanceled() {
	// Cancel context before compact (before prepareBackfillCompaction)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.task.ctx = ctx
	s.task.cancel = cancel

	// Compact should detect context cancel at the beginning
	_, err := s.task.Compact()
	s.Error(err)
	s.ErrorIs(err, context.Canceled)
}

func TestBackfillCompactionTaskBasic(t *testing.T) {
	ctx := context.Background()

	meta := genTestCollectionMetaWithBM25()
	params, err := compaction.GenerateJSONParams()
	assert.NoError(t, err)

	plan := &datapb.CompactionPlan{
		PlanID: 1000,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:   1,
			PartitionID:    1,
			SegmentID:      200,
			InsertChannel:  "test_channel",
			StorageVersion: storage.StorageV2,
		}},
		Type:                   datapb.CompactionType_BackfillCompaction,
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
	task := NewBackfillCompactionTask(ctx, mockCM, plan, compaction.GenParams())
	assert.NotNil(t, task)
	assert.Equal(t, int64(1000), task.GetPlanID())
	assert.Equal(t, int64(1), task.GetCollection())
	assert.Equal(t, "test_channel", task.GetChannelName())
	assert.Equal(t, datapb.CompactionType_BackfillCompaction, task.GetCompactionType())
	assert.Equal(t, int64(1), task.GetSlotUsage())
}

func (s *BackfillCompactionTaskSuite) TestProcessBatch() {
	s.Run("success", func() {
		s.setupTest()
		mockRunner := function.NewMockFunctionRunner(s.T())
		sparseArray := &schemapb.SparseFloatArray{
			Contents: [][]byte{{1, 2, 3, 4}},
			Dim:      100,
		}
		mockRunner.EXPECT().BatchRun([]string{"hello world"}).Return([]interface{}{sparseArray}, nil)

		insertData, memSize, err := s.task.processBatch(mockRunner, []string{"hello world"}, 102)
		s.NoError(err)
		s.Greater(memSize, 0)
		s.NotNil(insertData)
		s.NotNil(insertData.Data[102])
	})

	s.Run("BatchRun error", func() {
		s.setupTest()
		mockRunner := function.NewMockFunctionRunner(s.T())
		mockRunner.EXPECT().BatchRun([]string{"test"}).Return(nil, errors.New("batch run failed"))

		_, _, err := s.task.processBatch(mockRunner, []string{"test"}, 102)
		s.Error(err)
		s.Contains(err.Error(), "batch run failed")
	})

	s.Run("wrong output count", func() {
		s.setupTest()
		mockRunner := function.NewMockFunctionRunner(s.T())
		mockRunner.EXPECT().BatchRun([]string{"test"}).Return([]interface{}{nil, nil}, nil)

		_, _, err := s.task.processBatch(mockRunner, []string{"test"}, 102)
		s.Error(err)
		s.Contains(err.Error(), "exactly one output")
	})

	s.Run("wrong output type", func() {
		s.setupTest()
		mockRunner := function.NewMockFunctionRunner(s.T())
		mockRunner.EXPECT().BatchRun([]string{"test"}).Return([]interface{}{"not_sparse"}, nil)

		_, _, err := s.task.processBatch(mockRunner, []string{"test"}, 102)
		s.Error(err)
		s.Contains(err.Error(), "unexpected output type")
	})
}

func (s *BackfillCompactionTaskSuite) TestFinalizeMergedLogs() {
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
	bm25Bytes := []byte{1, 2, 3, 4, 5}

	mergedInsert, mergedBm25, err := s.task.finalizeMergedLogs(segment, newInsertLogs, 1000, bm25Bytes, "bm25/stats/path", 77, 102)
	s.NoError(err)

	// Check merged insert logs contain both original and new
	s.Equal(2, len(mergedInsert))
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())

	// Check BM25 logs
	s.Equal(1, len(mergedBm25))
	s.Equal(int64(102), mergedBm25[0].GetFieldID())
	s.Equal(int64(5), mergedBm25[0].GetBinlogs()[0].GetLogSize())
	s.Equal("bm25/stats/path", mergedBm25[0].GetBinlogs()[0].GetLogPath())
	s.Equal(int64(1000), mergedBm25[0].GetBinlogs()[0].GetEntriesNum())
	// LogID must be populated so buildBinlogKvs does not reject it with "invalid log id"
	s.Equal(int64(77), mergedBm25[0].GetBinlogs()[0].GetLogID())
}

// TestFinalizeMergedLogsCrashReplay verifies that re-running finalizeMergedLogs
// when the segment already contains the output field (DC crashed after AlterSegments
// but before task-state transition) does not produce duplicate InsertLog entries.
func (s *BackfillCompactionTaskSuite) TestFinalizeMergedLogsCrashReplay() {
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

	mergedInsert, _, err := s.task.finalizeMergedLogs(segment, newInsertLogs, 1000, []byte{1}, "bm25/path", 77, 102)
	s.NoError(err)

	// Must have exactly 2 entries (field 100 + field 102), not 3.
	s.Require().Equal(2, len(mergedInsert))
	var fieldIDs []int64
	for _, fb := range mergedInsert {
		fieldIDs = append(fieldIDs, fb.GetFieldID())
	}
	s.ElementsMatch([]int64{100, 102}, fieldIDs)
	// Retained entry must be the first-run binlog, not the replay copy.
	for _, fb := range mergedInsert {
		if fb.GetFieldID() == 102 {
			s.Equal("first-run/102", fb.GetBinlogs()[0].GetLogPath())
		}
	}
}

// TestFinalizeMergedLogsV3CrashReplay verifies crash-replay dedup for V3 column groups
// where the output field is identified via ChildFields rather than FieldID directly.
func (s *BackfillCompactionTaskSuite) TestFinalizeMergedLogsV3CrashReplay() {
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

	mergedInsert, _, err := s.task.finalizeMergedLogs(segment, newInsertLogs, 0, []byte{1}, "bm25/path", 0, 102)
	s.NoError(err)

	s.Require().Equal(2, len(mergedInsert))
	for _, fb := range mergedInsert {
		if fb.GetFieldID() == 102 {
			s.Equal("first-run/cg102", fb.GetBinlogs()[0].GetLogPath())
		}
	}
}

func (s *BackfillCompactionTaskSuite) TestBuildMergedLogsV2() {
	s.setupTest()

	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100", EntriesNum: 1000}}},
		},
	}
	// Simulate a V2 writer result: one column group with explicit paths, logIDs, and file sizes.
	writerResult := &backfillWriterResult{
		columnGroups: []storagecommon.ColumnGroup{
			{GroupID: 102, Fields: []int64{102}},
		},
		paths:          []string{"/local/insert_log/1/1/1/102/77"},
		truePaths:      []string{"/local/insert_log/1/1/1/102/77"},
		logIDs:         []int64{77},
		fileSizes:      []int64{2048},
		storageVersion: storage.StorageV2,
	}

	mergedInsert, mergedBm25, err := s.task.buildMergedLogsV2(segment, writerResult, 512, 1000, []byte{1, 2, 3}, "bm25/path/55", 55, 102)
	s.NoError(err)

	// Expect original field 100 + new field 102.
	s.Require().Equal(2, len(mergedInsert))
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())

	newBinlog := mergedInsert[1].GetBinlogs()[0]
	s.Equal(int64(2048), newBinlog.GetLogSize(), "LogSize should match file size from CloseAndTell")
	s.Equal(int64(512), newBinlog.GetMemorySize(), "MemorySize should match sparse field memory size")
	s.Equal(int64(1000), newBinlog.GetEntriesNum())
	s.Equal(int64(77), newBinlog.GetLogID(), "LogID must match allocated log ID")
	s.Equal("/local/insert_log/1/1/1/102/77", newBinlog.GetLogPath())

	// BM25 stats log.
	s.Require().Equal(1, len(mergedBm25))
	s.Equal(int64(102), mergedBm25[0].GetFieldID())
	s.Equal("bm25/path/55", mergedBm25[0].GetBinlogs()[0].GetLogPath())
	s.Equal(int64(55), mergedBm25[0].GetBinlogs()[0].GetLogID())
	s.Equal(int64(3), mergedBm25[0].GetBinlogs()[0].GetLogSize(), "BM25 log size should equal len(bytes)")
}

func (s *BackfillCompactionTaskSuite) TestBuildMergedLogsV3() {
	s.setupTest()

	segment := &datapb.CompactionSegmentBinlogs{
		SegmentID: 1,
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "original/100", EntriesNum: 1000}}},
		},
	}
	writerResult := &backfillWriterResult{
		columnGroups: []storagecommon.ColumnGroup{
			{GroupID: 102, Fields: []int64{102}},
		},
	}

	mergedInsert, mergedBm25, err := s.task.buildMergedLogsV3(segment, writerResult, 512, 1000, []byte{1, 2, 3}, "bm25/path", 0, 102)
	s.NoError(err)

	// Original + new
	s.Equal(2, len(mergedInsert))
	s.Equal(int64(100), mergedInsert[0].GetFieldID())
	s.Equal(int64(102), mergedInsert[1].GetFieldID())
	s.Equal(int64(512), mergedInsert[1].GetBinlogs()[0].GetMemorySize())
	s.Equal(int64(1000), mergedInsert[1].GetBinlogs()[0].GetEntriesNum())
	// V3 binlog presence marker must carry a non-zero LogID so buildBinlogKvs validation
	// passes (requires LogID!=0 AND LogPath=="" for V3 segments).
	s.NotZero(mergedInsert[1].GetBinlogs()[0].GetLogID(),
		"V3 backfill binlog presence marker must have non-zero LogID")

	s.Equal(1, len(mergedBm25))
	s.Equal(int64(102), mergedBm25[0].GetFieldID())
}

func (s *BackfillCompactionTaskSuite) TestCompleteAndStop() {
	s.setupTest()
	s.task.Complete()
	s.task.Stop()
}

func (s *BackfillCompactionTaskSuite) TestGetStorageConfig() {
	s.setupTest()
	s.NotNil(s.task.GetStorageConfig())
}
