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
	"fmt"

	// "fmt"
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
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestClusteringCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskSuite))
}

type ClusteringCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO
	mockAlloc    *allocator.MockAllocator
	mockID       atomic.Int64

	task *clusteringCompactionTask

	plan *datapb.CompactionPlan
}

func (s *ClusteringCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())

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

func (s *ClusteringCompactionTaskSuite) storageV2Setup() {
	paramtable.Get().Save("common.storageType", "local")
	paramtable.Get().Save("common.storage.enableV2", "true")
	initcore.InitStorageV2FileSystem(paramtable.Get())
}

func (s *ClusteringCompactionTaskSuite) storageV2Cleanup() {
	defer paramtable.Get().Reset("common.storageType")
	defer paramtable.Get().Reset("common.storage.enableV2")
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
	s.preparScalarCompactionNormalTask()
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
	s.Equal(2, totalBinlogNum/len(s.plan.Schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)

	s.EqualValues(10239,
		lo.SumBy(compactionResult.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
			return seg.GetNumOfRows()
		}),
	)

	// setup storage v2 compaction
	s.storageV2Setup()
	defer s.storageV2Cleanup()

	s.preparScalarCompactionNormalTask()
	compactionResultV2, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(len(compactionResult.GetSegments()), len(compactionResultV2.GetSegments()))

	for i := 0; i < len(compactionResult.GetSegments()); i++ {
		v1Seg := compactionResult.GetSegments()[i]
		v2Seg := compactionResultV2.GetSegments()[i]
		s.Equal(v1Seg.GetSegmentID(), v2Seg.GetSegmentID())
		s.Equal(v1Seg.GetNumOfRows(), v2Seg.GetNumOfRows())
		v1Binlogs := v1Seg.GetInsertLogs()
		v2Binlogs := v2Seg.GetInsertLogs()
		s.Less(len(v2Binlogs), len(v1Binlogs))
	}
}

func (s *ClusteringCompactionTaskSuite) preparScalarCompactionNormalTask() {
	s.SetupTest()
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
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // max segment size = 1GB, we won't touch this value
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   101,
	}
}

// func (s *ClusteringCompactionTaskSuite) TestScalarCompactionNormal_V2ToV2() {
// 	s.SetupTest()
// 	s.storageV2Setup()
// 	defer s.storageV2Cleanup()

// 	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, nil)
// 	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
// 	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

// 	var segmentID int64 = 1001
// 	binlogs, _, _, _, _, err := s.initStorageV2Segments(10240, segmentID, alloc)
// 	s.NoError(err)

// 	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
// 		{
// 			SegmentID:      segmentID,
// 			FieldBinlogs:   lo.Values(binlogs),
// 			StorageVersion: storage.StorageV2,
// 		},
// 	}

// 	s.task.plan.Schema = genCollectionSchema()
// 	s.task.plan.ClusteringKeyField = 100
// 	s.task.plan.PreferSegmentRows = 2048
// 	s.task.plan.MaxSegmentRows = 2048
// 	s.task.plan.MaxSize = 1024 * 1024 * 1024 // max segment size = 1GB, we won't touch this value
// 	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
// 		Begin: 1,
// 		End:   101,
// 	}

// 	compactionResultV2, err := s.task.Compact()
// 	s.Require().NoError(err)
// 	fmt.Println(compactionResultV2)
// }

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
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // max segment size = 1GB, we won't touch this value
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   1000,
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
	s.Equal(2, len(compactionResult.GetSegments()))
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
	s.Equal(5, totalBinlogNum/len(schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)
}

func (s *ClusteringCompactionTaskSuite) TestCompactionWithBM25Function() {
	s.prepareCompactionWithBM25FunctionTask()
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
	s.Equal(2, totalBinlogNum/len(s.task.plan.Schema.GetFields()))
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

	s.storageV2Setup()
	defer s.storageV2Cleanup()

	s.prepareCompactionWithBM25FunctionTask()
	compactionResultV2, err := s.task.Compact()
	s.Require().NoError(err)
	s.Equal(len(compactionResult.GetSegments()), len(compactionResultV2.GetSegments()))

	for i := 0; i < len(compactionResult.GetSegments()); i++ {
		v1Seg := compactionResult.GetSegments()[i]
		v2Seg := compactionResultV2.GetSegments()[i]
		s.Equal(v1Seg.GetSegmentID(), v2Seg.GetSegmentID())
		s.Equal(v1Seg.GetNumOfRows(), v2Seg.GetNumOfRows())
		v1Binlogs := v1Seg.GetInsertLogs()
		v2Binlogs := v2Seg.GetInsertLogs()
		s.Less(len(v2Binlogs), len(v1Binlogs))
		v1Bm25logs := v1Seg.GetBm25Logs()
		v2Bm25logs := v2Seg.GetBm25Logs()
		s.Equal(v1Bm25logs[0].GetBinlogs()[0].GetEntriesNum(), v2Bm25logs[0].GetBinlogs()[0].GetEntriesNum())
		s.Equal(v1Bm25logs[0].GetBinlogs()[0].GetMemorySize(), v2Bm25logs[0].GetBinlogs()[0].GetMemorySize())
	}
}

func (s *ClusteringCompactionTaskSuite) prepareCompactionWithBM25FunctionTask() {
	s.SetupTest()
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
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // 1GB
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   1000,
	}
}

func (s *ClusteringCompactionTaskSuite) initStorageV2Segments(rows int, seed int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().Schema().Return(genCollectionSchema()).Maybe()
	mc.EXPECT().GetSegmentByID(seed).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	pack := new(syncmgr.SyncPack).WithCollectionID(CollectionID).WithPartitionID(PartitionID).WithSegmentID(seed).WithChannelName(channelName).WithInsertData(genInsertData(rows, seed, genCollectionSchema()))
	bw := syncmgr.NewBulkPackWriterV2(mc, cm, alloc, packed.DefaultWriteBufferSize, 0)
	return bw.Write(context.Background(), pack)
}

func genInsertData(size int, seed int64, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	for i := 0; i < size; i++ {
		buf.Append(genRow(seed))
	}
	return []*storage.InsertData{buf}
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
