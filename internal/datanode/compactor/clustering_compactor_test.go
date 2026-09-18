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
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

func (s *ClusteringCompactionTaskSuite) setupTest() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, s.T().TempDir())
	initcore.InitStorageV2FileSystem(paramtable.Get())

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

	s.task = NewClusteringCompactionTask(context.Background(), s.mockBinlogIO, nil, compaction.GenParams())

	params, err := compaction.GenerateJSONParams(nil)
	if err != nil {
		panic(err)
	}

	s.plan = &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			CollectionID:        CollectionID,
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		Type: datapb.CompactionType_ClusteringCompaction,
		PreAllocatedLogIDs: &datapb.IDRange{
			Begin: 200,
			End:   2000,
		},
		JsonParams: params,
	}
	s.task.plan = s.plan
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	s.setupTest()
}

func (s *ClusteringCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ClusteringCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
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
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{}
	_, err := s.task.Compact()
	s.Require().Error(err)
	s.Equal(true, errors.Is(err, merr.ErrIllegalCompactionPlan))
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
			CollectionID: CollectionID,
			SegmentID:    100,
		},
	}
	err := s.task.init()
	s.Require().NoError(err)
	s.Equal(s.task.primaryKeyField, s.task.plan.Schema.Fields[2])
	s.Equal(false, s.task.isVectorClusteringKey)
	s.Equal(true, s.task.memoryLimit > 0)
	s.Equal(8, s.task.getWorkerPoolSize())
	s.Equal(8, s.task.mappingPool.Cap())
	s.Equal(8, s.task.flushPool.Cap())
}

func (s *ClusteringCompactionTaskSuite) preparScalarCompactionNormalTask() {
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second))},
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
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     genRow(int64(i)),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	kvs, fBinlogs, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)

	s.NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, strings []string) ([][]byte, error) {
		result := make([][]byte, 0, len(strings))
		for _, path := range strings {
			result = append(result, kvs[path])
		}
		return result, nil
	})

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID: CollectionID,
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
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 200,
		End:   2000,
	}
}

func (s *ClusteringCompactionTaskSuite) TestScalarCompactionNormal() {
	s.T().Skip("no chunking for storage v2, skip legacy test")
	s.preparScalarCompactionNormalTask()
	// 8+8+8+4+7+4*4=51
	// 51*1024 = 52224
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "60000")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	s.task.compactionParams = compaction.GenParams()

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
}

func (s *ClusteringCompactionTaskSuite) prepareScalarCompactionNormalByMemoryLimit() {
	schema := genCollectionSchema()
	var segmentID int64 = 1001
	segWriter, err := NewSegmentWriter(schema, 1000, compactionBatchSize, segmentID, PartitionID, CollectionID, []int64{})
	s.Require().NoError(err)
	for i := 0; i < 10240; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
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
				s.task.memoryLimit = 32 * 1024 * 1024
			})
			result := make([][]byte, 0, len(strings))
			for _, path := range strings {
				result = append(result, kvs[path])
			}
			return result, nil
		})

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID: CollectionID,
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
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 1001,
		End:   2000,
	}
}

func (s *ClusteringCompactionTaskSuite) TestScalarCompactionNormalByMemoryLimit() {
	s.T().Skip("no chunking for storage v2, skip legacy test")
	s.prepareScalarCompactionNormalByMemoryLimit()
	// 8+8+8+4+7+4*4=51
	// 51*1024 = 52224
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "60000")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)
	s.task.compactionParams = compaction.GenParams()

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
	s.Equal(5, totalBinlogNum/len(s.task.plan.Schema.GetFields()))
	s.Equal(1, statsBinlogNum)
	s.Equal(totalRowNum, statsRowNum)
}

func (s *ClusteringCompactionTaskSuite) prepareCompactionWithBM25FunctionTask() {
	s.SetupTest()
	s.prepareCompactionWithBM25OutputTask(10240)
}

func (s *ClusteringCompactionTaskSuite) prepareCompactionWithMissingBM25OutputTask(rowNum int) {
	s.prepareCompactionWithBM25OutputTask(rowNum, 102)
}

func (s *ClusteringCompactionTaskSuite) prepareCompactionWithBM25OutputTask(rowNum int, removeFieldIDs ...int64) {
	schema := genCollectionSchemaWithBM25()
	segmentID := int64(1001)
	segWriter, err := NewSegmentWriter(schema, int64(rowNum), compactionBatchSize, segmentID, PartitionID, CollectionID, []int64{102})
	s.Require().NoError(err)

	for i := 0; i < rowNum; i++ {
		v := storage.Value{
			PK:        storage.NewInt64PrimaryKey(int64(i)),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
			Value:     genRowWithBM25(int64(i)),
		}
		err = segWriter.Write(&v)
		s.Require().NoError(err)
	}
	segWriter.FlushAndIsFull()

	kvs, fBinlogs, err := serializeWrite(context.TODO(), s.mockAlloc, segWriter)
	s.Require().NoError(err)
	for _, fieldID := range removeFieldIDs {
		removeFieldBinlogForTest(kvs, fBinlogs, fieldID)
	}
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
		return downloadValuesForPathsForTest(kvs, paths)
	})

	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		{
			CollectionID: CollectionID,
			SegmentID:    segmentID,
			FieldBinlogs: lo.Values(fBinlogs),
		},
	}

	s.task.plan.Schema = schema
	s.task.plan.ClusteringKeyField = 100
	s.task.plan.PreferSegmentRows = 2048
	s.task.plan.MaxSegmentRows = 2048
	s.task.plan.MaxSize = 1024 * 1024 * 1024 // 1GB
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{
		Begin: 1,
		End:   1000,
	}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{
		Begin: 1001,
		End:   2000,
	}
}

func (s *ClusteringCompactionTaskSuite) TestCompactionWithBM25Function() {
	s.T().Skip("no chunking for storage v2, skip legacy test")
	// 8 + 8 + 8 + 7 + 8 = 39
	// 39*1024 = 39936
	// plus buffer on null bitsets etc., let's make it 50000
	// writer will automatically flush after 1024 rows.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, "50000")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)
	s.task.compactionParams = compaction.GenParams()
	s.prepareCompactionWithBM25FunctionTask()

	err := s.task.init()
	s.Require().NoError(err)

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
}

func (s *ClusteringCompactionTaskSuite) TestScalarClusteringMaterializesMissingBM25OutputFromOldSegment() {
	s.prepareCompactionWithMissingBM25OutputTask(3)

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)

	s.EqualValues(3, lo.SumBy(result.GetSegments(), func(segment *datapb.CompactionSegment) int64 {
		return segment.GetNumOfRows()
	}))
	bm25Rows := int64(0)
	for _, segment := range result.GetSegments() {
		bm25Rows += fieldBinlogEntriesForTest(segment.GetBm25Logs(), 102)
	}
	s.EqualValues(3, bm25Rows)
}

func (s *ClusteringCompactionTaskSuite) TestScalarClusteringPrefillsMissingBM25InputBeforeOutput() {
	s.prepareCompactionWithBM25OutputTask(3, 101, 102)
	typeutil.GetField(s.task.plan.GetSchema(), 101).Nullable = true

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	var inputRows, bm25Rows int64
	for _, segment := range result.GetSegments() {
		inputRows += fieldBinlogEntriesForTest(segment.GetInsertLogs(), 101)
		bm25Rows += fieldBinlogEntriesForTest(segment.GetBm25Logs(), 102)
	}
	s.EqualValues(3, inputRows)
	s.EqualValues(3, bm25Rows)
}

func (s *ClusteringCompactionTaskSuite) TestScalarAnalyzeSegmentFiltersDroppedOrMissingFields() {
	s.prepareCompactionWithMissingBM25OutputTask(2)
	s.task.plan.ClusteringKeyField = 101
	s.task.plan.SegmentBinlogs[0].FieldBinlogs = append(s.task.plan.SegmentBinlogs[0].FieldBinlogs, &datapb.FieldBinlog{
		FieldID: common.StartOfUserFieldID + 1000,
		Binlogs: []*datapb.Binlog{{
			LogPath: "dropped-field-should-not-be-read",
		}},
	})

	err := s.task.init()
	s.Require().NoError(err)
	defer s.task.cleanUp(context.Background())

	analyzeResult, err := s.task.scalarAnalyzeSegment(context.Background(), s.task.plan.SegmentBinlogs[0])
	s.Require().NoError(err)
	s.Equal(map[interface{}]int64{"varchar": 2}, analyzeResult)
}

// An import segment committed at birthday+2s carries a deltalog entry deleting
// PK=100 at birthday+1s. That delete predates the commit, so it must not remove
// the row, and every output row timestamp must be normalized to commit_ts.
func (s *ClusteringCompactionTaskSuite) TestScalarCompactionPreservesImportCommitTimestamp() {
	s.preparScalarCompactionNormalTask()
	commitTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(2 * time.Second))
	s.plan.SegmentBinlogs[0].CommitTimestamp = commitTs
	s.task.compactionParams = compaction.GenParams()

	compactionResult, err := s.task.Compact()
	s.Require().NoError(err)

	s.EqualValues(10240, lo.SumBy(compactionResult.GetSegments(), func(seg *datapb.CompactionSegment) int64 {
		return seg.GetNumOfRows()
	}))

	for _, seg := range compactionResult.GetSegments() {
		for _, fieldBinlog := range seg.GetInsertLogs() {
			for _, b := range fieldBinlog.GetBinlogs() {
				s.EqualValues(commitTs, b.GetTimestampFrom())
				s.EqualValues(commitTs, b.GetTimestampTo())
			}
		}
	}
}

type clusteringBatchRecorder struct {
	storage.BinlogRecordWriter
	batchRows []int
}

func (w *clusteringBatchRecorder) Write(r storage.Record) error {
	w.batchRows = append(w.batchRows, r.Len())
	return w.BinlogRecordWriter.Write(r)
}

func (s *ClusteringCompactionTaskSuite) TestMappingBatchesAcrossInputRecords() {
	s.checkMappingBatchesAcrossInputRecords(storage.StorageV1)
}

func (s *ClusteringCompactionTaskSuite) TestMappingBatchesAcrossPackedInputRecords() {
	s.checkMappingBatchesAcrossInputRecords(storage.StorageV2)
}

func (s *ClusteringCompactionTaskSuite) checkMappingBatchesAcrossInputRecords(inputVersion int64) {
	ctx := context.Background()
	schema := genCollectionSchema()
	kvs := make(map[string][]byte)
	binlogs := make(map[int64]*datapb.FieldBinlog)
	expected := make(map[int64]map[int64]interface{})
	// Ten small binlog groups produce multiple input records. Each record
	// contributes only eleven rows per bucket, below the serializer batch size.
	for chunk := 0; chunk < 10; chunk++ {
		var values []*storage.Value
		for i := 0; i < 22; i++ {
			pk := int64(chunk*22 + i)
			row := genRow(pk)
			row[102] = strconv.FormatInt(pk, 10)
			row[103] = []float32{float32(pk), 5, 6, 7}
			expected[pk] = row
			values = append(values, &storage.Value{
				PK: storage.NewInt64PrimaryKey(pk), Timestamp: row[common.TimeStampField].(int64), Value: row,
			})
		}
		var fields map[int64]*datapb.FieldBinlog
		if inputVersion == storage.StorageV1 {
			input, err := NewSegmentWriter(schema, 22, compactionBatchSize, 1001, PartitionID, CollectionID, nil)
			s.Require().NoError(err)
			for _, value := range values {
				s.Require().NoError(input.Write(value))
			}
			input.FlushAndIsFull()
			blobs, logs, err := serializeWrite(ctx, s.mockAlloc, input)
			s.Require().NoError(err)
			fields = logs
			for path, data := range blobs {
				kvs[path] = data
			}
		} else {
			input, err := storage.NewBinlogRecordWriter(ctx, CollectionID, PartitionID, 1001, schema,
				s.mockAlloc, 64<<20, 22, storage.WithVersion(storage.StorageV2),
				storage.WithStorageConfig(s.task.compactionParams.StorageConfig), storage.WithUploader(s.mockBinlogIO.Upload))
			s.Require().NoError(err)
			record, err := storage.ValueSerializer(values, schema)
			s.Require().NoError(err)
			s.Require().NoError(input.Write(record))
			record.Release()
			s.Require().NoError(input.Close())
			fields, _, _, _, _ = input.GetLogs()
		}
		for id, field := range fields {
			if binlogs[id] == nil {
				binlogs[id] = &datapb.FieldBinlog{FieldID: id}
			}
			binlogs[id].Binlogs = append(binlogs[id].Binlogs, field.Binlogs...)
		}
	}
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, paths []string) ([][]byte, error) {
		result := make([][]byte, 0, len(paths))
		for _, path := range paths {
			result = append(result, kvs[path])
		}
		return result, nil
	}).Maybe()
	s.plan.Schema = schema
	s.plan.ClusteringKeyField = 100
	s.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{{
		CollectionID: CollectionID, PartitionID: PartitionID, SegmentID: 1001,
		FieldBinlogs: lo.Values(binlogs), StorageVersion: inputVersion,
	}}
	s.Require().NoError(s.task.init())
	defer s.task.cleanUp(ctx)
	s.task.memoryLimit = 1 << 30
	var writers []*clusteringBatchRecorder
	for bucket := 0; bucket < 2; bucket++ {
		writer, err := NewMultiSegmentWriter(ctx, s.mockBinlogIO, NewCompactionAllocator(s.mockAlloc, s.mockAlloc),
			1<<30, schema, s.task.compactionParams, 110, PartitionID, CollectionID, "channel", 100,
			storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
		s.Require().NoError(err)
		s.Require().NoError(writer.rotateWriter())
		recorder := &clusteringBatchRecorder{BinlogRecordWriter: writer.writer.BinlogRecordWriter}
		writer.writer = storage.NewBinlogValueWriter(recorder, 100)
		writers = append(writers, recorder)
		s.task.clusterBuffers = append(s.task.clusterBuffers, newClusterBuffer(bucket, writer, nil))
		defer writer.Close()
	}
	s.task.keyToBufferFunc = func(key interface{}) *ClusterBuffer {
		return s.task.clusterBuffers[key.(int64)%2]
	}
	s.Require().NoError(s.task.mappingSegment(ctx, s.plan.SegmentBinlogs[0]))
	for _, writer := range writers {
		s.Equal([]int{100}, writer.batchRows, "input record boundaries must not submit partial batches")
	}
	s.Require().NoError(s.task.flushAll())
	for bucket, writer := range writers {
		s.Equal([]int{100, 10}, writer.batchRows)
		logs, _, _, _, _ := writer.GetLogs()
		reader, err := storage.NewBinlogRecordReader(ctx, lo.Values(logs), schema,
			storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(s.task.compactionParams.StorageConfig), storage.WithUseLoonFFI(false))
		s.Require().NoError(err)
		defer reader.Close()
		seen := 0
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			s.Require().NoError(err)
			values := make([]*storage.Value, record.Len())
			s.Require().NoError(storage.ValueDeserializerWithSchema(record, values, schema, true))
			for _, value := range values {
				pk := int64(bucket + seen*2)
				s.Equal(expected[pk], value.Value, "values must survive release of earlier input records")
				seen++
			}
		}
		s.Equal(110, seen)
	}
}

func genRow(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday())
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
				FieldID:          102,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
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
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday())
	return map[int64]interface{}{
		common.RowIDField:     magic,
		common.TimeStampField: int64(ts),
		100:                   magic,
		101:                   "varchar",
		102:                   typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1}),
	}
}
