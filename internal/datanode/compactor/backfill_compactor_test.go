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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
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

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")
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

	s.task = NewBackfillCompactionTask(context.Background(), plan, compaction.GenParams())
}

func (s *BackfillCompactionTaskSuite) SetupTest() {
	paramtable.Get().Save("common.storage.enablev2", "true")
	paramtable.Get().Save("common.storageType", "local")
	s.setupTest()
}

func (s *BackfillCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
	paramtable.Get().Reset("common.storageType")
	paramtable.Get().Reset("common.storage.enablev2")
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
			StorageVersion: storage.StorageV2,
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
	s.NotEmpty(segment.GetBm25Logs())
	s.Equal(1, len(segment.GetBm25Logs()))
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
	s.Contains(err.Error(), "bm25 function should have exactly one output field")
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
	mockBinlogIO := mock_util.NewMockBinlogIO(t)

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
	}

	task := NewBackfillCompactionTask(ctx, plan, compaction.GenParams())
	assert.NotNil(t, task)
	assert.Equal(t, int64(1000), task.GetPlanID())
	assert.Equal(t, int64(1), task.GetCollection())
	assert.Equal(t, "test_channel", task.GetChannelName())
	assert.Equal(t, datapb.CompactionType_BackfillCompaction, task.GetCompactionType())
}
