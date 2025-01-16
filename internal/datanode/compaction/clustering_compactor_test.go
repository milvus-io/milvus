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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestClusteringCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskSuite))
}

type ClusteringCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *io.MockBinlogIO
	mockAlloc    *allocator.MockAllocator
	mockID       atomic.Int64

	task *clusteringCompactionTask

	plan *datapb.CompactionPlan
}

func (s *ClusteringCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())

	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockID.Store(time.Now().UnixMilli())
	s.mockAlloc.EXPECT().Alloc(mock.Anything).RunAndReturn(func(x uint32) (int64, int64, error) {
		start := s.mockID.Load()
		end := s.mockID.Add(int64(x))
		return start, end, nil
	}).Maybe()
	s.mockAlloc.EXPECT().AllocOne().RunAndReturn(func() (int64, error) {
		end := s.mockID.Add(1)
		return end, nil
	}).Maybe()

	s.task = NewClusteringCompactionTask(context.Background(), s.mockBinlogIO, nil)

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")

	s.plan = &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		TimeoutInSeconds: 10,
		Type:             datapb.CompactionType_ClusteringCompaction,
	}
	s.task.plan = s.plan
}

func (s *ClusteringCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ClusteringCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func (s *ClusteringCompactionTaskSuite) TestWrongCompactionType() {
	s.plan.Type = datapb.CompactionType_MixCompaction
	result, err := s.task.Compact()
	s.Empty(result)
	s.Require().Error(err)
	s.Equal(true, errors.Is(err, merr.ErrIllegalCompactionPlan))
}

func (s *ClusteringCompactionTaskSuite) TestContextDown() {
	ctx, cancel := context.WithCancel(context.Background())
	s.task.ctx = ctx
	cancel()
	result, err := s.task.Compact()
	s.Empty(result)
	s.Require().Error(err)
}

func (s *ClusteringCompactionTaskSuite) TestIsVectorClusteringKey() {
	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.init()
	s.Equal(false, s.task.isVectorClusteringKey)
	s.task.plan.ClusteringKeyField = 103
	s.task.init()
	s.Equal(true, s.task.isVectorClusteringKey)
}

func (s *ClusteringCompactionTaskSuite) TestCompactionWithEmptyBinlog() {
	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	_, err := s.task.Compact()
	s.Require().Error(err)
	s.Equal(true, errors.Is(err, merr.ErrIllegalCompactionPlan))
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{}
	_, err2 := s.task.Compact()
	s.Require().Error(err2)
	s.Equal(true, errors.Is(err2, merr.ErrIllegalCompactionPlan))
}

func (s *ClusteringCompactionTaskSuite) TestCompactionWithEmptySchema() {
	s.task.plan.ClusteringKeyField = 100
	_, err := s.task.Compact()
	s.Require().Error(err)
	s.Equal(true, errors.Is(err, merr.ErrIllegalCompactionPlan))
}

func (s *ClusteringCompactionTaskSuite) TestCompactionInit() {
	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID: 100,
		},
	}
	err := s.task.init()
	s.Require().NoError(err)
	s.Equal(s.task.primaryKeyField, s.task.plan.Schema.Fields[2])
	s.Equal(false, s.task.isVectorClusteringKey)
	s.Equal(true, s.task.memoryBufferSize > 0)
	s.Equal(8, s.task.getWorkerPoolSize())
	s.Equal(8, s.task.mappingPool.Cap())
	s.Equal(8, s.task.flushPool.Cap())
}

func (s *ClusteringCompactionTaskSuite) TestScalarCompactionNormal() {
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"1"}).
		Return([][]byte{dblobs.GetValue()}, nil).Once()

	schema := genCollectionSchema()
	var segmentID int64 = 1001
	segWriter, err := NewSegmentWriter(schema, 1000, compactionBatchSize, segmentID, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)
	for i := 0; i < 10240; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     genRow(int64(i)),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	kvs, fBinlogs, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)
	s.NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(lo.Values(kvs), nil)

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "1"}}},
			},
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: time.Now().UnixMilli(),
		End:   time.Now().UnixMilli() + 1000,
	}

	// 8+8+8+4+7+4*4=51
	// 51*1024 = 52224
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "52223")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(5, len(s.task.clusterBuffers))
	s.Equal(5, len(compactionResult.GetSegments()))
	totalBinlogNum := 0
	totalRowNum := int64(0)
	for _, fb := range compactionResult.GetSegments()[0].GetInsertLogs() {
		for _, b := range fb.GetBinlogs() {
			totalBinlogNum++
			if fb.GetFieldID() == 100 {
				totalRowNum += b.GetEntriesNum()
			}
		}
	}
	statsBinlogNum := 0
	statsRowNum := int64(0)
	for _, sb := range compactionResult.GetSegments()[0].GetField2StatslogPaths() {
		for _, b := range sb.GetBinlogs() {
			statsBinlogNum++
			statsRowNum += b.GetEntriesNum()
		}
	}
	s.Equal(2, totalBinlogNum/len(schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)

	s.EqualValues(10239,
		lo.SumBy(compactionResult.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
			return seg.GetNumOfRows()
		}),
	)
}

func (s *ClusteringCompactionTaskSuite) TestScalarCompactionNormalByMemoryLimit() {
	schema := genCollectionSchema()
	var segmentID int64 = 1001
	segWriter, err := NewSegmentWriter(schema, 1000, compactionBatchSize, segmentID, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)
	for i := 0; i < 10240; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     genRow(int64(i)),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	kvs, fBinlogs, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)
	s.NoError(err)
	var one sync.Once
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, strings []string) ([][]byte, error) {
			// 32m, only two buffers can be generated
			one.Do(func() {
				s.task.memoryBufferSize = 32 * 1024 * 1024
			})
			return lo.Values(kvs), nil
		})

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			FieldBinlogs: lo.Values(fBinlogs),
		},
	}

	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 3000
	s.task.plan.MaxSegmentRows = 3000
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: time.Now().UnixMilli(),
		End:   time.Now().UnixMilli() + 1000,
	}

	// 8+8+8+4+7+4*4=51
	// 51*1024 = 52224
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "52223")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(2, len(s.task.clusterBuffers))
	s.Equal(4, len(compactionResult.GetSegments()))
	totalBinlogNum := 0
	totalRowNum := int64(0)
	for _, fb := range compactionResult.GetSegments()[0].GetInsertLogs() {
		for _, b := range fb.GetBinlogs() {
			totalBinlogNum++
			if fb.GetFieldID() == 100 {
				totalRowNum += b.GetEntriesNum()
			}
		}
	}
	statsBinlogNum := 0
	statsRowNum := int64(0)
	for _, sb := range compactionResult.GetSegments()[0].GetField2StatslogPaths() {
		for _, b := range sb.GetBinlogs() {
			statsBinlogNum++
			statsRowNum += b.GetEntriesNum()
		}
	}
	s.Equal(3, totalBinlogNum/len(schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)
}

func (s *ClusteringCompactionTaskSuite) TestCompactionWithBM25Function() {
	schema := genCollectionSchemaWithBM25()
	var segmentID int64 = 1001
	segWriter, err := NewSegmentWriter(schema, 1000, compactionBatchSize, segmentID, PartitionID, CollectionID, []int64{102})
	s.Require().NoError(err)

	for i := 0; i < 10240; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     genRowWithBM25(int64(i)),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	kvs, fBinlogs, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)
	s.NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(lo.Values(kvs), nil)

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:    segmentID,
			FieldBinlogs: lo.Values(fBinlogs),
		},
	}

	s.task.bm25FieldIds = []int64{102}
	s.task.plan.Schema = schema
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: time.Now().UnixMilli(),
		End:   time.Now().UnixMilli() + 1000,
	}

	// 8 + 8 + 8 + 7 + 8 = 39
	// 39*1024 = 39936
	// plus buffer on null bitsets etc., let's make it 45000
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "45000")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(5, len(s.task.clusterBuffers))
	s.Equal(5, len(compactionResult.GetSegments()))
	totalBinlogNum := 0
	totalRowNum := int64(0)
	for _, fb := range compactionResult.GetSegments()[0].GetInsertLogs() {
		for _, b := range fb.GetBinlogs() {
			totalBinlogNum++
			if fb.GetFieldID() == 100 {
				totalRowNum += b.GetEntriesNum()
			}
		}
	}
	statsBinlogNum := 0
	statsRowNum := int64(0)
	for _, sb := range compactionResult.GetSegments()[0].GetField2StatslogPaths() {
		for _, b := range sb.GetBinlogs() {
			statsBinlogNum++
			statsRowNum += b.GetEntriesNum()
		}
	}
	s.Equal(2, totalBinlogNum/len(schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)

	bm25BinlogNum := 0
	bm25RowNum := int64(0)
	for _, bmb := range compactionResult.GetSegments()[0].GetBm25Logs() {
		for _, b := range bmb.GetBinlogs() {
			bm25BinlogNum++
			bm25RowNum += b.GetEntriesNum()
		}
	}

	s.Equal(1, bm25BinlogNum)
	s.Equal(totalRowNum, bm25RowNum)
}

func (s *ClusteringCompactionTaskSuite) TestCheckBuffersAfterCompaction() {
	s.Run("no leak", func() {
		task := &clusteringCompactionTask{clusterBuffers: []*ClusterBuffer{{}}}

		s.NoError(task.checkBuffersAfterCompaction())
	})

	s.Run("leak binlog", func() {
		task := &clusteringCompactionTask{
			clusterBuffers: []*ClusterBuffer{
				{
					flushedBinlogs: map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog{
						1: {
							101: {
								FieldID: 101,
								Binlogs: []*datapb.Binlog{{LogID: 1000}},
							},
						},
					},
				},
			},
		}
		s.Error(task.checkBuffersAfterCompaction())
	})
}

func (s *ClusteringCompactionTaskSuite) TestGenerateBM25Stats() {
	s.Run("normal case", func() {
		segmentID := int64(1)
		task := &clusteringCompactionTask{
			collectionID: 111,
			partitionID:  222,
			bm25FieldIds: []int64{102},
			logIDAlloc:   s.mockAlloc,
			binlogIO:     s.mockBinlogIO,
		}

		statsMap := make(map[int64]*storage.BM25Stats)
		statsMap[102] = storage.NewBM25Stats()
		statsMap[102].Append(map[uint32]float32{1: 1})

		binlogs, err := task.generateBM25Stats(context.Background(), segmentID, statsMap)
		s.NoError(err)
		s.Equal(1, len(binlogs))
		s.Equal(1, len(binlogs[0].Binlogs))
		s.Equal(int64(102), binlogs[0].FieldID)
		s.Equal(int64(1), binlogs[0].Binlogs[0].GetEntriesNum())
	})

	s.Run("alloc ID failed", func() {
		segmentID := int64(1)
		mockAlloc := allocator.NewMockAllocator(s.T())
		mockAlloc.EXPECT().Alloc(mock.Anything).Return(0, 0, fmt.Errorf("mock error")).Once()

		task := &clusteringCompactionTask{
			collectionID: 111,
			partitionID:  222,
			bm25FieldIds: []int64{102},
			logIDAlloc:   mockAlloc,
		}

		statsMap := make(map[int64]*storage.BM25Stats)
		statsMap[102] = storage.NewBM25Stats()
		statsMap[102].Append(map[uint32]float32{1: 1})

		_, err := task.generateBM25Stats(context.Background(), segmentID, statsMap)
		s.Error(err)
	})

	s.Run("upload failed", func() {
		segmentID := int64(1)
		mockBinlogIO := io.NewMockBinlogIO(s.T())
		mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error")).Once()

		task := &clusteringCompactionTask{
			collectionID: 111,
			partitionID:  222,
			bm25FieldIds: []int64{102},
			logIDAlloc:   s.mockAlloc,
			binlogIO:     mockBinlogIO,
		}

		statsMap := make(map[int64]*storage.BM25Stats)
		statsMap[102] = storage.NewBM25Stats()
		statsMap[102].Append(map[uint32]float32{1: 1})

		_, err := task.generateBM25Stats(context.Background(), segmentID, statsMap)
		s.Error(err)
	})
}

func (s *ClusteringCompactionTaskSuite) TestGeneratePkStats() {
	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
	}
	s.Run("num rows zero", func() {
		task := &clusteringCompactionTask{
			primaryKeyField: pkField,
		}
		binlogs, err := task.generatePkStats(context.Background(), 1, 0, nil)
		s.Error(err)
		s.Nil(binlogs)
	})

	s.Run("download binlogs failed", func() {
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error"))
		task := &clusteringCompactionTask{
			binlogIO:        s.mockBinlogIO,
			primaryKeyField: pkField,
		}
		binlogs, err := task.generatePkStats(context.Background(), 1, 100, [][]string{{"abc", "def"}})
		s.Error(err)
		s.Nil(binlogs)
	})

	s.Run("NewInsertBinlogIterator failed", func() {
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return([][]byte{[]byte("mock")}, nil)
		task := &clusteringCompactionTask{
			binlogIO:        s.mockBinlogIO,
			primaryKeyField: pkField,
		}
		binlogs, err := task.generatePkStats(context.Background(), 1, 100, [][]string{{"abc", "def"}})
		s.Error(err)
		s.Nil(binlogs)
	})

	s.Run("upload failed", func() {
		schema := genCollectionSchema()
		segWriter, err := NewSegmentWriter(schema, 1000, compactionBatchSize, SegmentID, PartitionID, CollectionID, []int64{})
		s.Require().NoError(err)
		for i := 0; i < 2000; i++ {
			v := storage.Value{
				PK:        storage.NewInt64PrimaryKey(int64(i)),
				Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
				Value:     genRow(int64(i)),
			}
			err = segWriter.Write(&v)
			s.Require().NoError(err)
		}
		segWriter.FlushAndIsFull()

		kvs, _, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)
		s.NoError(err)
		mockBinlogIO := io.NewMockBinlogIO(s.T())
		mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(lo.Values(kvs), nil)
		mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		task := &clusteringCompactionTask{
			collectionID: CollectionID,
			partitionID:  PartitionID,
			plan: &datapb.CompactionPlan{
				Schema: genCollectionSchema(),
			},
			binlogIO:        mockBinlogIO,
			primaryKeyField: pkField,
			logIDAlloc:      s.mockAlloc,
		}

		binlogs, err := task.generatePkStats(context.Background(), 1, 100, [][]string{{"abc", "def"}})
		s.Error(err)
		s.Nil(binlogs)
	})
}

func genRow(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)
	return map[int64]interface{}{
		common.RowIDField:     magic,
		common.TimeStampField: int64(ts),
		100:                   magic,
		101:                   int32(magic),
		102:                   "varchar",
		103:                   []float32{4, 5, 6, 7},
	}
}

func genCollectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
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
				Name:     "field_int32",
				DataType: schemapb.DataType_Int32,
			},
			{
				FieldID:  102,
				Name:     "field_varchar",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:     103,
				Name:        "field_float_vector",
				Description: "float_vector",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
		},
	}
}

func genCollectionSchemaWithBM25() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
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
						Value: "8",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "sparse",
				DataType: schemapb.DataType_SparseFloatVector,
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Id:               100,
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			InputFieldIds:    []int64{101},
			OutputFieldNames: []string{"sparse"},
			OutputFieldIds:   []int64{102},
		}},
	}
}

func genRowWithBM25(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)
	return map[int64]interface{}{
		common.RowIDField:     magic,
		common.TimeStampField: int64(ts),
		100:                   magic,
		101:                   "varchar",
		102:                   typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1}),
	}
}
