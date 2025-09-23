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
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestSortCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(SortCompactionTaskSuite))
}

type SortCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO

	meta      *etcdpb.CollectionMeta
	segWriter *SegmentWriter

	task *sortCompactionTask
}

func (s *SortCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *SortCompactionTaskSuite) setupTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())

	s.meta = genTestCollectionMeta()

	params, err := compaction.GenerateJSONParams()
	s.NoError(err)

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		Type:                   datapb.CompactionType_SortCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		SlotUsage:              8,
		JsonParams:             params,
		TotalRows:              1000,
		CollectionTtl:          time.Since(getMilvusBirthday().Add(-time.Hour)).Nanoseconds(),
	}

	pk, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	s.NoError(err)

	s.task = NewSortCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams(), []int64{pk.GetFieldID()})
}

func (s *SortCompactionTaskSuite) SetupTest() {
	s.setupTest()
}

func (s *SortCompactionTaskSuite) TearDownTest() {
}

func (s *SortCompactionTaskSuite) TestNewSortCompactionTask() {
	plan := &datapb.CompactionPlan{
		PlanID:    123,
		Type:      datapb.CompactionType_SortCompaction,
		SlotUsage: 8,
		Schema:    s.meta.GetSchema(),
	}

	pk, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	s.NoError(err)

	task := NewSortCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams(), []int64{pk.GetFieldID()})

	s.NotNil(task)
	s.Equal(plan.GetPlanID(), task.GetPlanID())
	s.Equal(datapb.CompactionType_SortCompaction, task.GetCompactionType())
	s.Equal(int64(8), task.GetSlotUsage())
}

func (s *SortCompactionTaskSuite) TestPreCompactValidation() {
	// Test with multiple segments (should fail)
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{SegmentID: 100},
		{SegmentID: 101},
	}

	err := s.task.preCompact()
	s.Error(err)
	s.Contains(err.Error(), "sort compaction should handle exactly one segment")

	// Test with single segment (should pass basic validation)
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    100,
			CollectionID: 1001,
			PartitionID:  1002,
			FieldBinlogs: []*datapb.FieldBinlog{},
			Deltalogs:    []*datapb.FieldBinlog{},
		},
	}

	err = s.task.preCompact()
	s.NoError(err)
	s.Equal(int64(1001), s.task.collectionID)
	s.Equal(int64(1002), s.task.partitionID)
	s.Equal(int64(100), s.task.segmentID)
}

func (s *SortCompactionTaskSuite) prepareSortCompactionTask() {
	segmentID := int64(1001)
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	// Initialize segment buffer with test data
	s.initSegBuffer(1000, segmentID)
	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
		left, right := lo.Difference(keys, lo.Keys(kvs))
		return len(left) == 0 && len(right) == 0
	})).Return(lo.Values(kvs), nil).Once()

	// Create delta log for deletion
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday(), 5)
	blob, err := getInt64DeltaBlobs(segmentID, []int64{segmentID}, []uint64{deleteTs})
	s.Require().NoError(err)
	deltaPath := "deltalog/1001"
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltaPath}).
		Return([][]byte{blob.GetValue()}, nil).Once()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			CollectionID: CollectionID,
			PartitionID:  PartitionID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogPath: deltaPath}}},
			},
		},
	}
}

func (s *SortCompactionTaskSuite) TestSortCompactionBasic() {
	s.prepareSortCompactionTask()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(datapb.CompactionTaskState_completed, result.GetState())
	s.Equal(datapb.CompactionType_SortCompaction, result.GetType())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.True(segment.GetIsSorted()) // Sort compaction should mark segment as sorted
	// s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
	// delete 5 counts
	s.Equal(int64(995), segment.GetNumOfRows())
}

func (s *SortCompactionTaskSuite) TestSortCompactionWithBM25() {
	s.setupBM25Test()
	s.prepareSortCompactionWithBM25Task()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.True(segment.GetIsSorted())
	// s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.EqualValues(1, len(segment.Bm25Logs)) // Should have BM25 logs
	s.Empty(segment.Deltalogs)
}

func (s *SortCompactionTaskSuite) setupBM25Test() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMetaWithBM25()
	params, err := compaction.GenerateJSONParams()
	if err != nil {
		panic(err)
	}

	plan := &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		Type:                   datapb.CompactionType_SortCompaction,
		Schema:                 s.meta.GetSchema(),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 9530, End: 19530},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             params,
		TotalRows:              3,
	}

	pk, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	s.NoError(err)

	s.task = NewSortCompactionTask(context.Background(), s.mockBinlogIO, plan, compaction.GenParams(), []int64{pk.GetFieldID()})
}

func (s *SortCompactionTaskSuite) prepareSortCompactionWithBM25Task() {
	segmentID := int64(1001)
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	s.initSegBufferWithBM25(segmentID)
	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
		left, right := lo.Difference(keys, lo.Keys(kvs))
		return len(left) == 0 && len(right) == 0
	})).Return(lo.Values(kvs), nil).Once()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			CollectionID: CollectionID,
			PartitionID:  PartitionID,
			FieldBinlogs: lo.Values(fBinlogs),
		},
	}
}

func (s *SortCompactionTaskSuite) TestSortCompactionWithExpiredData() {
	segmentID := int64(1001)

	s.initSegBuffer(1, segmentID)
	collTTL := 864000 // 10 days
	s.task.currentTime = getMilvusBirthday().Add(time.Second * (time.Duration(collTTL) + 1))
	s.task.plan.CollectionTtl = int64(collTTL)
	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)

	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, paths []string) ([][]byte, error) {
			s.Require().Equal(len(paths), len(kvs))
			return lo.Values(kvs), nil
		})
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			CollectionID: CollectionID,
			PartitionID:  PartitionID,
			FieldBinlogs: lo.Values(fBinlogs),
		},
	}

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	// Due to TTL expiration, some data might be filtered out
	segment := result.GetSegments()[0]
	s.True(segment.GetIsSorted())
}

func (s *SortCompactionTaskSuite) TestSortCompactionFail() {
	// Test with invalid plan (no segments)
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{}

	result, err := s.task.Compact()
	s.NoError(err) // Should not return error, but result should indicate failure
	s.NotNil(result)
	s.Equal(datapb.CompactionTaskState_failed, result.GetState())
}

func (s *SortCompactionTaskSuite) TestTaskInterface() {
	// Test Compactor interface methods
	s.Equal(int64(999), s.task.GetPlanID())
	s.Equal(datapb.CompactionType_SortCompaction, s.task.GetCompactionType())
	s.Equal(int64(8), s.task.GetSlotUsage())

	// Test task lifecycle
	s.task.Complete()

	s.task.Stop()
}

// Helper methods (copied/adapted from mix compaction tests)
func (s *SortCompactionTaskSuite) initSegBuffer(size int, seed int64) {
	s.segWriter, _ = NewSegmentWriter(s.meta.GetSchema(), int64(size), compactionBatchSize, 1, PartitionID, CollectionID, []int64{})

	for i := 0; i < size; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(seed),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), int64(i))),
			Value:     getRow(seed, int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), int64(i)))),
		}
		err := s.segWriter.Write(&v)
		s.Require().NoError(err)
	}
	s.segWriter.FlushAndIsFull()
}

func (s *SortCompactionTaskSuite) initSegBufferWithBM25(seed int64) {
	s.segWriter, _ = NewSegmentWriter(s.meta.GetSchema(), 1, compactionBatchSize, 1, PartitionID, CollectionID, []int64{102})

	v := storage.Value{
		PK:        storage.NewInt64PrimaryKey(seed),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     genRowWithBM25(seed),
	}
	err := s.segWriter.Write(&v)
	s.Require().NoError(err)
	s.segWriter.FlushAndIsFull()
}

func TestSortCompactionTaskBasic(t *testing.T) {
	ctx := context.Background()
	mockBinlogIO := mock_util.NewMockBinlogIO(t)

	plan := &datapb.CompactionPlan{
		PlanID: 123,
		Type:   datapb.CompactionType_SortCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{SegmentID: 100},
		},
		Schema: genTestCollectionMeta().GetSchema(),
	}

	pk, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	assert.NoError(t, err)

	task := NewSortCompactionTask(ctx, mockBinlogIO, plan, compaction.GenParams(), []int64{pk.GetFieldID()})

	assert.NotNil(t, task)
	assert.Equal(t, int64(123), task.GetPlanID())
	assert.Equal(t, datapb.CompactionType_SortCompaction, task.GetCompactionType())
}
