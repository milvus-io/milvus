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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
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

// Observe the Record interface while retaining the real binlog writer below it.
type clusteringRecordObserver struct {
	storage.BinlogRecordWriter
	rows          []int
	values        []*storage.Value
	captureValues bool
	writeError    error
	blobs         map[string][]byte
}

func (w *clusteringRecordObserver) Write(r storage.Record) error {
	if w.writeError != nil {
		return w.writeError
	}
	if w.captureValues {
		values := make([]*storage.Value, r.Len())
		if err := storage.ValueDeserializerWithSchema(r, values, w.Schema(), true); err != nil {
			return err
		}
		w.values = append(w.values, values...)
	}
	if err := w.BinlogRecordWriter.Write(r); err != nil {
		return err
	}
	w.rows = append(w.rows, r.Len())
	return nil
}

func (s *ClusteringCompactionTaskSuite) TestMappingAccumulatesRecords() {
	s.checkMappingRecords(false, -1)
}

func (s *ClusteringCompactionTaskSuite) TestMappingVectorOffsetsAfterDelete() {
	s.checkMappingRecords(true, -1)
}

func (s *ClusteringCompactionTaskSuite) TestMappingRecordTTL() {
	s.checkMappingRecords(false, 511)
}

type clusteringOffsetReader struct {
	binlogio.BinlogIO
	offsets []byte
}

func (r *clusteringOffsetReader) Download(ctx context.Context, paths []string) ([][]byte, error) {
	if len(paths) == 1 && paths[0] == "offsets" {
		return [][]byte{r.offsets}, nil
	}
	return r.BinlogIO.Download(ctx, paths)
}

func (s *ClusteringCompactionTaskSuite) checkMappingRecords(vector bool, ttlCutoff int64) {
	s.preparScalarCompactionNormalTask()
	s.Require().NoError(s.task.init())
	defer s.task.cleanUp(context.Background())
	writer, err := NewMultiSegmentWriter(context.Background(), s.mockBinlogIO,
		NewCompactionAllocator(s.task.segIDAlloc, s.task.logIDAlloc),
		s.plan.MaxSize, s.plan.Schema, s.task.compactionParams, s.plan.MaxSegmentRows,
		PartitionID, CollectionID, "", 100, storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
	s.Require().NoError(err)
	s.Require().NoError(writer.rotateWriter())
	observer := &clusteringRecordObserver{BinlogRecordWriter: writer.writer.BinlogRecordWriter, captureValues: true}
	writer.writer = storage.NewBinlogValueWriter(observer, 100)
	buffer := newClusterBuffer(0, writer, nil)
	s.task.clusterBuffers = []*ClusterBuffer{buffer}
	s.task.keyToBufferFunc = func(any) *ClusterBuffer { return buffer }
	s.task.memoryLimit = 1 << 30
	if ttlCutoff >= 0 {
		// Reuse the fixture's int64 PK column as expiration timestamps. Rows
		// through ttlCutoff expire, while the existing delete still filters 100.
		s.task.ttlFieldID = 100
		s.task.currentTime = time.UnixMicro(ttlCutoff)
	}
	if vector {
		ids := make([]uint32, 10240)
		for i := range ids {
			ids[i] = uint32(i % 7)
		}
		encoded, err := proto.Marshal(&clusteringpb.ClusteringCentroidIdMappingStats{CentroidIdMapping: ids})
		s.Require().NoError(err)
		s.task.binlogIO = &clusteringOffsetReader{BinlogIO: s.mockBinlogIO, offsets: encoded}
		s.task.isVectorClusteringKey = true
		s.task.segmentIDOffsetMapping = map[int64]string{s.plan.SegmentBinlogs[0].SegmentID: "offsets"}
		nextOffset := int64(0)
		s.task.offsetToBufferFunc = func(offset int64, mapping []uint32) *ClusterBuffer {
			if nextOffset == 100 {
				nextOffset++
			}
			s.Equal(nextOffset, offset)
			s.Equal(uint32(offset%7), mapping[offset])
			nextOffset++
			return buffer
		}
	}
	defer buffer.Close()

	s.Require().NoError(s.task.mappingSegment(context.Background(), s.plan.SegmentBinlogs[0]))
	s.Empty(observer.rows, "input Record boundaries must not submit underfilled output batches")
	s.Require().NoError(buffer.Close())
	expectedRows := 10239
	if ttlCutoff >= 100 {
		expectedRows = 10240 - int(ttlCutoff) - 1
	}
	s.Equal([]int{expectedRows}, observer.rows)
	s.Require().Len(observer.values, expectedRows)
	for i, v := range observer.values {
		id := int64(i)
		if ttlCutoff >= 100 {
			id += ttlCutoff + 1
		}
		if ttlCutoff < 100 && id >= 100 {
			id++
		} // deleted by the fixture's deltalog
		s.Equal(id, v.PK.GetValue())
		s.Equal(genRow(id), v.Value)
	}
}

// Use both a wide vector and variable-length nullable columns: row count alone
// must not determine when a bucket submits a Record.
func clusteringWideSchema() *schemapb.CollectionSchema {
	schema := genCollectionSchema()
	schema.Fields[4].Nullable = true
	schema.Fields[5].TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2048"}}
	for i := int64(104); i < 112; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID: i, Name: fmt.Sprint(i), DataType: schemapb.DataType_VarChar,
			Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
		})
	}
	return schema
}

func clusteringTestRecord(t *testing.T, schema *schemapb.CollectionSchema, first, rows int) storage.Record {
	t.Helper()
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	field2Col := make(map[int64]int)
	for i, field := range schema.Fields {
		field2Col[field.FieldID] = i
		for row := first; row < first+rows; row++ {
			b := builder.Field(i)
			if field.Nullable && row%3 == 0 {
				b.AppendNull()
				continue
			}
			switch b := b.(type) {
			case *array.Int64Builder:
				b.Append(int64(row))
			case *array.Int32Builder:
				b.Append(int32(row))
			case *array.Float64Builder:
				b.Append(float64(row) + 0.25)
			case *array.StringBuilder:
				b.Append(strings.Repeat("x", 128+row%17) + fmt.Sprint(row))
			case *array.FixedSizeBinaryBuilder:
				vector := make([]byte, b.Type().(*arrow.FixedSizeBinaryType).ByteWidth)
				for dim := 0; dim < len(vector)/4; dim++ {
					binary.LittleEndian.PutUint32(vector[dim*4:], math.Float32bits(float32(row+dim)))
				}
				b.Append(vector)
			default:
				t.Fatalf("unsupported fixture field %v", field)
			}
		}
	}
	return storage.NewSimpleArrowRecord(builder.NewRecord(), field2Col)
}

func newClusteringTestBuffer(t *testing.T, schema *schemapb.CollectionSchema, batchBytes uint64, version int64, overrides ...storage.RwOption) (*ClusterBuffer, *clusteringRecordObserver) {
	t.Helper()
	paramtable.Init()
	params := compaction.GenParams()
	params.BinLogMaxSize = batchBytes
	params.StorageVersion = version
	params.StorageConfig = &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	binlogIO := mock_util.NewMockBinlogIO(t)
	blobs := make(map[string][]byte)
	binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, data map[string][]byte) error {
		for key, value := range data {
			blobs[key] = append([]byte(nil), value...)
		}
		return nil
	}).Maybe()
	task := &clusteringCompactionTask{bufferSize: int64(batchBytes), compactionParams: params}
	opts := append(task.getWriterOpts(), storage.WithColumnGroups([]storagecommon.ColumnGroup{
		{GroupID: 0, Fields: []int64{0, 1, 100, 101, 102}, Columns: []int{0, 1, 2, 3, 4}},
		{GroupID: 103, Fields: []int64{103}, Columns: []int{5}},
		{GroupID: 104, Fields: []int64{104, 105, 106, 107, 108, 109, 110, 111}, Columns: []int{6, 7, 8, 9, 10, 11, 12, 13}},
	}))
	opts = append(opts, overrides...)
	writer, err := NewMultiSegmentWriter(context.Background(), binlogIO,
		NewCompactionAllocator(allocator.NewLocalAllocator(1, 100), allocator.NewLocalAllocator(100, 10000)),
		1<<30, schema, params, 10000, PartitionID, CollectionID, "", 100, opts...)
	require.NoError(t, err)
	require.NoError(t, writer.rotateWriter())
	observer := &clusteringRecordObserver{BinlogRecordWriter: writer.writer.BinlogRecordWriter, blobs: blobs}
	writer.writer = storage.NewBinlogValueWriter(observer, 100)
	buffer := newClusterBuffer(0, writer, nil)
	t.Cleanup(func() { require.NoError(t, buffer.Close()) })
	return buffer, observer
}

func TestClusteringMappingRejectsInvalidTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
		wrapped bool
	}{
		{name: "wrong type"},
		{name: "missing", missing: true},
		{name: "missing materialized", missing: true, wrapped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := genCollectionSchema()
			malformed := proto.Clone(schema).(*schemapb.CollectionSchema)
			if tc.missing {
				malformed.Fields = append(malformed.Fields[:1], malformed.Fields[2:]...)
			} else {
				malformed.Fields[1].DataType = schemapb.DataType_VarChar
			}
			record := clusteringTestRecord(t, malformed, 0, 1)
			defer record.Release()
			var readerRecord storage.Record
			readerRecord = record
			if tc.wrapped {
				readerRecord = &materializedRecord{base: record}
			}
			existingFields := make(map[int64]struct{}, len(malformed.Fields))
			for _, field := range malformed.Fields {
				existingFields[field.FieldID] = struct{}{}
			}
			patch := mockey.Mock(newTextDecodedCompactionSegmentRecordReader).
				Return(&mockReader{records: []storage.Record{readerRecord}}, existingFields, nil).Build()
			defer patch.UnPatch()
			segment := &datapb.CompactionSegmentBinlogs{CollectionID: CollectionID, SegmentID: 1}
			task := &clusteringCompactionTask{
				plan:             &datapb.CompactionPlan{Schema: schema, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{segment}},
				binlogIO:         mock_util.NewMockBinlogIO(t),
				primaryKeyField:  typeutil.GetField(schema, 100),
				compactionParams: compaction.GenParams(),
			}
			err := task.mappingSegment(context.Background(), segment)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.ErrorContains(t, err, "timestamp field is not an int64 column")
		})
	}
}

func TestClusterBufferV3BatchUsesWriterSchema(t *testing.T) {
	schema := clusteringWideSchema()
	schema.Fields[3].ExternalField = "source_field_101"
	writer := &MultiSegmentWriter{schema: schema, storageVersion: storage.StorageV3, binLogMaxSize: 64 << 20}
	buffer := newClusterBuffer(0, writer, nil)
	defer buffer.releaseBuilder()
	input := clusteringTestRecord(t, schema, 1, 1)
	require.NoError(t, buffer.WriteRecord(input, 0))
	input.Release()
	batch := buffer.builder.Build()
	defer batch.Release()
	wanted, err := storage.ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	require.True(t, batch.(interface{ ArrowSchema() *arrow.Schema }).ArrowSchema().Equal(wanted),
		"V3 batches should match the writer schema and reuse the Arrow record")
}

func TestClusterBufferRecordBatches(t *testing.T) {
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			schema := clusteringWideSchema()
			// Half of the 64 KiB binlog budget is for allocated Arrow buffers.
			// The third wide row grows their capacity past 32 KiB.
			buffer, observer := newClusteringTestBuffer(t, schema, 64<<10, version)
			observer.captureValues = true
			for i := 0; i < 7; i++ {
				record := clusteringTestRecord(t, schema, i, 1)
				require.NoError(t, buffer.WriteRecord(record, 0))
				record.Release() // bucket must own copies across input lifetimes
				if i == 0 {
					require.Empty(t, observer.rows)
				}
			}
			require.Equal(t, []int{3, 3}, observer.rows)
			require.GreaterOrEqual(t, buffer.GetBufferSize(), uint64(8192))
			require.NoError(t, buffer.Close())
			require.Equal(t, []int{3, 3, 1}, observer.rows)
			require.Len(t, observer.values, 7)
			for i, value := range observer.values {
				require.Equal(t, int64(i), value.PK.GetValue())
				row := value.Value.(map[int64]interface{})
				vector := row[103].([]float32)
				require.Len(t, vector, 2048)
				for dim, v := range vector {
					require.Equal(t, float32(i+dim), v)
				}
				if i%3 == 0 {
					require.Nil(t, row[102])
				} else {
					require.Equal(t, strings.Repeat("x", 128+i%17)+fmt.Sprint(i), row[102])
				}
			}
		})
	}
}

func TestClusterBufferCloseFlushesLazyWriter(t *testing.T) {
	schema := clusteringWideSchema()
	params := compaction.GenParams()
	params.StorageVersion = storage.StorageV1
	params.BinLogMaxSize = 64 << 20
	params.StorageConfig = &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	binlogIO := mock_util.NewMockBinlogIO(t)
	binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	writer, err := NewMultiSegmentWriter(context.Background(), binlogIO,
		NewCompactionAllocator(allocator.NewLocalAllocator(1, 100), allocator.NewLocalAllocator(100, 10000)),
		1<<30, schema, params, 10000, PartitionID, CollectionID, "", 100,
		storage.WithStorageConfig(params.StorageConfig))
	require.NoError(t, err)
	require.Nil(t, writer.writer)
	buffer := newClusterBuffer(0, writer, nil)
	input := clusteringTestRecord(t, schema, 1, 1)
	require.NoError(t, buffer.WriteRecord(input, 0))
	input.Release()
	require.Nil(t, writer.writer, "the writer should still be unopened before final close")
	require.NoError(t, buffer.Close())
	require.Len(t, writer.res, 1)
	require.EqualValues(t, 1, writer.res[0].NumOfRows)
	require.NoError(t, buffer.Close())
}

func TestClusterBufferOversizedRowAndWriteError(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 1024, storage.StorageV1)
	record := clusteringTestRecord(t, clusteringWideSchema(), 1, 2)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	require.Equal(t, []int{1}, observer.rows)
	want := errors.New("write record failed")
	observer.writeError = want
	require.ErrorIs(t, buffer.WriteRecord(record, 1), want)
	observer.writeError = nil
	require.ErrorIs(t, buffer.WriteRecord(record, 0), want, "a failed write must poison the bucket")
	require.Equal(t, []int{1}, observer.rows, "later rows must not reach the writer")
	require.ErrorIs(t, buffer.FlushChunk(), want)
	require.ErrorIs(t, buffer.Close(), want)
	require.Nil(t, buffer.builder)
}

func TestClusterBufferConcurrentRecords(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 64<<10, storage.StorageV1)
	observer.captureValues = true
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		record := clusteringTestRecord(t, clusteringWideSchema(), worker*10, 10)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer record.Release()
			for row := 0; row < record.Len(); row++ {
				if err := buffer.WriteRecord(record, row); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, buffer.Close())
	seen := make(map[int64]bool)
	for _, value := range observer.values {
		key := value.PK.GetValue().(int64)
		require.False(t, seen[key])
		seen[key] = true
	}
	require.Len(t, seen, 40)
}

func TestClusteringScalarNullAndDefault(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(dataType.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{DataType: dataType, Nullable: true}
			var builder array.Builder
			var expected any
			if dataType == schemapb.DataType_Int64 {
				b := array.NewInt64Builder(memory.DefaultAllocator)
				b.Append(42)
				builder, expected = b, int64(42)
			} else {
				b := array.NewStringBuilder(memory.DefaultAllocator)
				b.Append("key")
				builder, expected = b, "key"
			}
			defer builder.Release()
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			value, err := clusteringScalarValue(values, field, 0)
			require.NoError(t, err)
			require.Equal(t, expected, value)
			value, err = clusteringScalarValue(values, field, 1)
			require.NoError(t, err)
			require.Nil(t, value)
			if dataType == schemapb.DataType_Int64 {
				field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}
			} else {
				field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "key"}}
			}
			value, err = clusteringScalarValue(values, field, 1)
			require.NoError(t, err)
			require.Equal(t, expected, value)
		})
	}
}

func TestClusterBufferFlushChunkAndCleanup(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 64<<20, storage.StorageV1)
	record := clusteringTestRecord(t, clusteringWideSchema(), 0, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	require.Empty(t, observer.rows)
	require.NoError(t, buffer.FlushChunk())
	require.Equal(t, []int{1}, observer.rows)
	require.Zero(t, buffer.GetBufferSize(), "V1 pressure flush must include the pending Builder")
	require.NoError(t, buffer.WriteRecord(record, 0))
	task := &clusteringCompactionTask{clusterBuffers: []*ClusterBuffer{buffer}}
	task.cleanUp(context.Background())
	require.Nil(t, buffer.builder, "cleanup discards an unfinished batch on task failure")
	require.Equal(t, []int{1}, observer.rows)
}

func TestClusterBufferAccountsForNullColumns(t *testing.T) {
	schema := clusteringWideSchema()
	for id := int64(112); id < 352; id++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID: id, Name: fmt.Sprint(id), DataType: schemapb.DataType_Double, Nullable: true,
		})
	}
	buffer, observer := newClusteringTestBuffer(t, schema, 2<<20, storage.StorageV1)
	record := clusteringTestRecord(t, schema, 0, 1)
	defer record.Release()
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	original := memory.DefaultAllocator
	memory.DefaultAllocator = checked
	buffer.builder = storage.NewRecordBuilder(schema)
	memory.DefaultAllocator = original
	defer func() {
		require.NoError(t, buffer.Close())
		checked.AssertSize(t, 0)
	}()
	for range 32 {
		require.NoError(t, buffer.WriteRecord(record, 0))
	}
	require.Empty(t, observer.rows)
	require.EqualValues(t, checked.CurrentAlloc(), buffer.GetBufferSize(), "pressure accounting must include all null column buffers")
	for range 33 {
		require.NoError(t, buffer.WriteRecord(record, 0))
	}
	require.NotEmpty(t, observer.rows, "allocated capacity must trigger a batch before payload bytes reach the threshold")
}

func TestClusterBufferWideReadback(t *testing.T) {
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			const rows = 9
			schema := clusteringWideSchema()
			schema.Fields[4].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "fallback"}}
			schema.Fields[6] = &schemapb.FieldSchema{
				FieldID: 104, Name: "double", DataType: schemapb.DataType_Double, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_DoubleData{DoubleData: 3.5}},
			}
			const defaultTimestamp int64 = 1_700_000_000_000_000
			schema.Fields[7] = &schemapb.FieldSchema{
				FieldID: 105, Name: "timestamp", DataType: schemapb.DataType_Timestamptz, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: defaultTimestamp}},
			}
			buffer, observer := newClusteringTestBuffer(t, schema, 16<<10, version)
			for row := 0; row < rows; row++ {
				record := clusteringTestRecord(t, schema, row, 1)
				err := buffer.WriteRecord(record, 0)
				record.Release()
				require.NoError(t, err)
			}
			require.NoError(t, buffer.Close())
			segments := buffer.GetCompactionSegments()
			require.Len(t, segments, 1)
			segment := segments[0]
			require.EqualValues(t, rows, segment.NumOfRows)
			reader, err := storage.NewBinlogRecordReader(context.Background(), segment.InsertLogs, schema,
				storage.WithVersion(version), storage.WithStorageConfig(buffer.writer.params.StorageConfig),
				storage.WithDownloader(func(_ context.Context, paths []string) ([][]byte, error) {
					data := make([][]byte, len(paths))
					for i, key := range paths {
						require.Contains(t, observer.blobs, key)
						data[i] = observer.blobs[key]
					}
					return data, nil
				}))
			require.NoError(t, err)
			defer reader.Close()
			rowID := 0
			for {
				record, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				for row := 0; row < record.Len(); row++ {
					for _, field := range schema.Fields {
						column := record.Column(field.FieldID)
						null := field.Nullable && rowID%3 == 0 && field.DefaultValue == nil
						require.Equal(t, null, column.IsNull(row), "field %d row %d", field.FieldID, rowID)
						if null {
							continue
						}
						switch column := column.(type) {
						case *array.Int64:
							want := int64(rowID)
							if field.GetDataType() == schemapb.DataType_Timestamptz && rowID%3 == 0 {
								want = defaultTimestamp
							}
							require.Equal(t, want, column.Value(row))
						case *array.Int32:
							require.EqualValues(t, rowID, column.Value(row))
						case *array.Float64:
							want := float64(rowID) + 0.25
							if rowID%3 == 0 {
								want = 3.5
							}
							require.Equal(t, want, column.Value(row))
						case *array.String:
							want := strings.Repeat("x", 128+rowID%17) + fmt.Sprint(rowID)
							if rowID%3 == 0 {
								want = "fallback"
							}
							require.Equal(t, want, column.Value(row))
						case *array.FixedSizeBinary:
							for dim := 0; dim < 2048; dim++ {
								require.Equal(t, float32(rowID+dim), math.Float32frombits(binary.LittleEndian.Uint32(column.Value(row)[dim*4:])))
							}
						default:
							t.Fatalf("unexpected column %T", column)
						}
					}
					rowID++
				}
			}
			require.Equal(t, rows, rowID)
			for _, group := range segment.InsertLogs {
				var entries int64
				for _, log := range group.Binlogs {
					require.EqualValues(t, entries, log.TimestampFrom)
					entries += log.EntriesNum
					require.EqualValues(t, entries-1, log.TimestampTo)
				}
				require.EqualValues(t, rows, entries)
			}
			if version == storage.StorageV2 {
				for _, field := range schema.Fields {
					want := int64(0)
					if field.Nullable && field.DefaultValue == nil {
						want = 3
					}
					require.Contains(t, segment.GetStats().GetNullCounts(), field.FieldID)
					require.Equal(t, want, segment.GetStats().GetNullCounts()[field.FieldID], "field %d", field.FieldID)
				}
			}
			require.NotEmpty(t, segment.Field2StatslogPaths)
			for _, field := range segment.Field2StatslogPaths {
				for _, log := range field.Binlogs {
					stats, err := storage.DeserializeStats([]*storage.Blob{{Value: observer.blobs[log.LogPath]}})
					require.NoError(t, err)
					require.Len(t, stats, 1)
					require.EqualValues(t, 0, stats[0].MinPk.GetValue())
					require.EqualValues(t, rows-1, stats[0].MaxPk.GetValue())
				}
			}
		})
	}
}

// The second Next releases the input and pauses while the bucket still owns an
// underfilled batch. This exercises Compact's cleanup after real mapping work.
type pausedClusteringReader struct {
	ctx    context.Context
	record storage.Record
	read   bool
	ready  chan<- struct{}
	resume <-chan struct{}
	closed chan struct{}
	err    error
}

func (r *pausedClusteringReader) Next() (storage.Record, error) {
	if !r.read {
		r.read = true
		return r.record, nil
	}
	r.record.Release()
	r.record = nil
	r.ready <- struct{}{}
	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-r.resume:
		return nil, r.err
	}
}

func (r *pausedClusteringReader) Close() error {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	close(r.closed)
	return nil
}

func TestClusteringReleasesPendingBuildersOnFailure(t *testing.T) {
	for _, cancelTask := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelTask), func(t *testing.T) {
			schema := clusteringWideSchema()
			buffer, observer := newClusteringTestBuffer(t, schema, 64<<20, storage.StorageV1)
			workers := &paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize
			previousWorkers := workers.GetValue()
			require.NoError(t, paramtable.Get().Save(workers.Key, "2"))
			t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(workers.Key, previousWorkers)) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			originalAllocator := memory.DefaultAllocator
			memory.DefaultAllocator = checked
			defer func() { memory.DefaultAllocator = originalAllocator }()
			ready := make(chan struct{}, 2)
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			readFailure := errors.New("injected read failure after a buffered record")
			readers := make([]*pausedClusteringReader, 2)
			for i := range readers {
				readers[i] = &pausedClusteringReader{
					ctx: ctx, record: clusteringTestRecord(t, schema, i, 1),
					ready: ready, resume: gates[i], closed: make(chan struct{}), err: readFailure,
				}
			}
			plan := &datapb.CompactionPlan{
				Type: datapb.CompactionType_ClusteringCompaction, Schema: schema, ClusteringKeyField: 100,
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{CollectionID: CollectionID, SegmentID: 0}, {CollectionID: CollectionID, SegmentID: 1}},
			}
			task := NewClusteringCompactionTask(ctx, buffer.writer.binlogIO, plan, buffer.writer.params)
			analyze := mockey.Mock((*clusteringCompactionTask).getScalarAnalyzeResult).To(func(task *clusteringCompactionTask, _ context.Context) error {
				task.clusterBuffers = []*ClusterBuffer{buffer}
				task.keyToBufferFunc = func(any) *ClusterBuffer { return buffer }
				return nil
			}).Build()
			defer analyze.UnPatch()
			readerFactory := mockey.Mock(newTextDecodedCompactionSegmentRecordReader).To(func(_ context.Context, segment *datapb.CompactionSegmentBinlogs,
				_ *schemapb.CollectionSchema, _ *indexpb.StorageConfig, _ []packed.TextColumnConfig, _ ...storage.RwOption,
			) (storage.RecordReader, map[int64]struct{}, error) {
				fields := make(map[int64]struct{})
				for _, field := range schema.Fields {
					fields[field.FieldID] = struct{}{}
				}
				return readers[segment.SegmentID], fields, nil
			}).Build()
			defer readerFactory.UnPatch()
			finished := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, err := task.Compact()
				finished <- err
			}()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("mapping workers did not stop during test cleanup")
				}
			}()
			for range readers {
				select {
				case <-ready:
				case <-time.After(10 * time.Second):
					t.Fatal("mapping did not reach the pending batch")
				}
			}
			require.Greater(t, checked.CurrentAlloc(), 0)
			require.Equal(t, 2, buffer.builder.GetRowNum())
			require.Empty(t, observer.rows)
			if cancelTask {
				cancel()
			} else {
				close(gates[0])
				select {
				case <-readers[0].closed:
				case <-time.After(10 * time.Second):
					t.Fatal("failed reader did not close")
				}
				select {
				case err := <-finished:
					t.Fatalf("Compact returned while another mapping worker still owned a pending batch: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				close(gates[1])
			}
			select {
			case err := <-finished:
				if cancelTask {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, readFailure)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Compact did not finish after readers stopped")
			}
			require.Nil(t, buffer.builder)
			checked.AssertSize(t, 0)
			require.Empty(t, observer.rows)
		})
	}
}

type clusteringFailingWriter struct {
	storage.BinlogRecordWriter
	writeErr error
	closeErr error
	writes   int
	closes   int
}

func (w *clusteringFailingWriter) GetWrittenUncompressed() uint64 { return 0 }
func (w *clusteringFailingWriter) GetBufferUncompressed() uint64  { return 0 }
func (w *clusteringFailingWriter) FlushChunk() error              { return nil }
func (w *clusteringFailingWriter) Write(storage.Record) error {
	w.writes++
	return w.writeErr
}

func (w *clusteringFailingWriter) Close() error {
	w.closes++
	return w.closeErr
}

func newClusteringFailureBuffer(t *testing.T, schema *schemapb.CollectionSchema, writer *clusteringFailingWriter) *ClusterBuffer {
	t.Helper()
	buffer := newClusterBuffer(0, &MultiSegmentWriter{
		schema: schema, binLogMaxSize: 64 << 20, segmentSize: 1 << 30,
		allocator: &compactionAlloactor{}, writer: storage.NewBinlogValueWriter(writer, 100),
	}, nil)
	t.Cleanup(buffer.releaseBuilder)
	return buffer
}

func TestClusterBufferRejectsWritesAfterPartialAppend(t *testing.T) {
	schema := clusteringWideSchema()
	writer := &clusteringFailingWriter{closeErr: errors.New("injected close error")}
	buffer := newClusteringFailureBuffer(t, schema, writer)
	good := clusteringTestRecord(t, schema, 1, 1)
	defer good.Release()
	require.NoError(t, buffer.WriteRecord(good, 0))
	badSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	badSchema.Fields[len(badSchema.Fields)-1].DataType = schemapb.DataType_Int64
	bad := clusteringTestRecord(t, badSchema, 2, 1)
	defer bad.Release()
	appendErr := buffer.WriteRecord(bad, 0)
	require.ErrorContains(t, appendErr, "failed to append value")

	// Simulate workers that were already mapping when another worker failed.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.ErrorIs(t, buffer.WriteRecord(good, 0), appendErr)
		}()
	}
	wg.Wait()
	require.ErrorIs(t, buffer.FlushChunk(), appendErr)
	require.Nil(t, buffer.builder, "the partially appended batch must be discarded")
	require.Zero(t, writer.writes, "no partial or later batch may reach the writer")
}

func TestClusteringMappingCancelsWorkersOnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task := &clusteringCompactionTask{
		plan:        &datapb.CompactionPlan{SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 0}, {SegmentID: 1}}},
		mappingPool: conc.NewPool[any](2),
	}
	defer task.mappingPool.Release()
	started := make(chan struct{})
	exited := make(chan struct{})
	want := errors.New("injected append failure")
	patch := mockey.Mock((*clusteringCompactionTask).mappingSegment).To(func(_ *clusteringCompactionTask, ctx context.Context, segment *datapb.CompactionSegmentBinlogs) error {
		if segment.SegmentID == 0 {
			close(started)
			<-ctx.Done()
			close(exited)
			return ctx.Err()
		}
		<-started
		return want
	}).Build()
	defer patch.UnPatch()
	segments, stats, err := task.mapping(ctx)
	require.ErrorIs(t, err, want, "sibling cancellation must not mask the original failure")
	require.Nil(t, segments)
	require.Nil(t, stats)
	select {
	case <-exited:
	default:
		t.Fatal("mapping returned before the canceled worker exited")
	}
}

func TestClusterBufferClosesWriterAfterFlushFailure(t *testing.T) {
	schema := clusteringWideSchema()
	writeErr := errors.New("injected write error")
	closeErr := errors.New("injected close error")
	writer := &clusteringFailingWriter{writeErr: writeErr, closeErr: closeErr}
	buffer := newClusteringFailureBuffer(t, schema, writer)
	record := clusteringTestRecord(t, schema, 1, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	err := buffer.Close()
	require.ErrorIs(t, err, writeErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, 1, writer.closes)
	require.Nil(t, buffer.builder)
	require.Nil(t, buffer.writer.writer)
	require.NoError(t, buffer.Close())
	require.Equal(t, 1, writer.closes)
}

func TestClusteringCleanupClosesWriterWithoutFlushingPendingRows(t *testing.T) {
	schema := clusteringWideSchema()
	writer := &clusteringFailingWriter{closeErr: errors.New("injected close error")}
	buffer := newClusteringFailureBuffer(t, schema, writer)
	record := clusteringTestRecord(t, schema, 1, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	task := &clusteringCompactionTask{clusterBuffers: []*ClusterBuffer{buffer}}
	task.cleanUp(context.Background())
	require.Zero(t, writer.writes)
	require.Equal(t, 1, writer.closes)
	require.Nil(t, buffer.builder)
	require.Nil(t, buffer.writer.writer)
	task.cleanUp(context.Background())
	require.Equal(t, 1, writer.closes)
}
