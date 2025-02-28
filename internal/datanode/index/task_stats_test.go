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

package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestTaskStatsSuite(t *testing.T) {
	suite.Run(t, new(TaskStatsSuite))
}

type TaskStatsSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	clusterID    string
	schema       *schemapb.CollectionSchema

	mockBinlogIO *io.MockBinlogIO
	segWriter    *compaction.SegmentWriter
}

func (s *TaskStatsSuite) SetupSuite() {
	s.collectionID = 100
	s.partitionID = 101
	s.clusterID = "102"
}

func (s *TaskStatsSuite) SetupSubTest() {
	paramtable.Init()
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
}

func (s *TaskStatsSuite) GenSegmentWriterWithBM25(magic int64) {
	segWriter, err := compaction.NewSegmentWriter(s.schema, 100, statsBatchSize, magic, s.partitionID, s.collectionID, []int64{102})
	s.Require().NoError(err)

	v := storage.Value{
		PK:        storage.NewInt64PrimaryKey(magic),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     genRowWithBM25(magic),
	}
	err = segWriter.Write(&v)
	s.Require().NoError(err)
	segWriter.FlushAndIsFull()

	s.segWriter = segWriter
}

func (s *TaskStatsSuite) TestSortSegmentWithBM25() {
	s.Run("normal case", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.GenSegmentWriterWithBM25(0)
		_, kvs, fBinlogs, err := serializeWrite(context.TODO(), "root_path", 0, s.segWriter)
		s.NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			result := make([][]byte, len(paths))
			for i, path := range paths {
				result[i] = kvs[path]
			}
			return result, nil
		})
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

		ctx, cancel := context.WithCancel(context.Background())

		testTaskKey := Key{ClusterID: s.clusterID, TaskID: 100}
		manager := NewManager(ctx)
		manager.LoadOrStoreStatsTask(s.clusterID, testTaskKey.TaskID, &StatsTaskInfo{SegID: 1})
		task := NewStatsTask(ctx, cancel, &workerpb.CreateStatsRequest{
			CollectionID:    s.collectionID,
			PartitionID:     s.partitionID,
			ClusterID:       s.clusterID,
			TaskID:          testTaskKey.TaskID,
			TargetSegmentID: 1,
			InsertLogs:      lo.Values(fBinlogs),
			Schema:          s.schema,
			NumRows:         1,
			StartLogID:      0,
			EndLogID:        7,
			BinlogMaxSize:   64 * 1024 * 1024,
			StorageConfig: &indexpb.StorageConfig{
				RootPath: "root_path",
			},
		}, manager, s.mockBinlogIO)
		err = task.PreExecute(ctx)
		s.Require().NoError(err)
		binlog, err := task.sort(ctx)
		s.Require().NoError(err)
		s.Equal(5, len(binlog))

		// check bm25 log
		s.Equal(1, len(manager.statsTasks))
		for key, task := range manager.statsTasks {
			s.Equal(testTaskKey.ClusterID, key.ClusterID)
			s.Equal(testTaskKey.TaskID, key.TaskID)
			s.Equal(1, len(task.Bm25Logs))
		}
	})

	s.Run("upload bm25 binlog failed", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.GenSegmentWriterWithBM25(0)
		_, kvs, fBinlogs, err := serializeWrite(context.TODO(), "root_path", 0, s.segWriter)
		s.NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			result := make([][]byte, len(paths))
			for i, path := range paths {
				result[i] = kvs[path]
			}
			return result, nil
		})
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error")).Once()

		ctx, cancel := context.WithCancel(context.Background())

		testTaskKey := Key{ClusterID: s.clusterID, TaskID: 100}
		manager := NewManager(ctx)
		manager.LoadOrStoreStatsTask(s.clusterID, testTaskKey.TaskID, &StatsTaskInfo{SegID: 1})
		task := NewStatsTask(ctx, cancel, &workerpb.CreateStatsRequest{
			CollectionID:    s.collectionID,
			PartitionID:     s.partitionID,
			ClusterID:       s.clusterID,
			TaskID:          testTaskKey.TaskID,
			TargetSegmentID: 1,
			InsertLogs:      lo.Values(fBinlogs),
			Schema:          s.schema,
			NumRows:         1,
			StartLogID:      0,
			EndLogID:        7,
			BinlogMaxSize:   64 * 1024 * 1024,
			StorageConfig: &indexpb.StorageConfig{
				RootPath: "root_path",
			},
		}, manager, s.mockBinlogIO)
		err = task.PreExecute(ctx)
		s.Require().NoError(err)
		_, err = task.sort(ctx)
		s.Error(err)
	})
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

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}
