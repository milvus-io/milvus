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

package importv2

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type L0ImportSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channel      string

	delCnt     int
	deleteData *storage.DeleteData
	schema     *schemapb.CollectionSchema

	cm      storage.ChunkManager
	reader  *importutilv2.MockReader
	syncMgr *syncmgr.MockSyncManager
	manager TaskManager
}

func (s *L0ImportSuite) SetupSuite() {
	paramtable.Init()
}

func (s *L0ImportSuite) SetupTest() {
	s.collectionID = 1
	s.partitionID = 2
	s.segmentID = 3
	s.channel = "ch-0"
	s.delCnt = 100

	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
		},
	}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())

	deleteData := storage.NewDeleteData(nil, nil)
	for i := 0; i < s.delCnt; i++ {
		deleteData.Append(storage.NewVarCharPrimaryKey(fmt.Sprintf("No.%d", i)), uint64(i+1))
	}
	s.deleteData = deleteData
	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(s.collectionID, s.partitionID, s.segmentID, deleteData)
	s.NoError(err)

	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Read(mock.Anything, mock.Anything).Return(blob.Value, nil)
	cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, walkFunc storage.ChunkObjectWalkFunc) error {
			for _, file := range []string{"a/b/c/"} {
				walkFunc(&storage.ChunkObjectInfo{FilePath: file})
			}
			return nil
		})
	s.cm = cm
}

func (s *L0ImportSuite) TestL0PreImport() {
	req := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy-prefix"}}},
	}
	task := NewL0PreImportTask(req, s.manager, s.cm)
	s.manager.Add(task)
	fu := task.Execute()
	err := conc.AwaitAll(fu...)
	s.NoError(err)
	l0Task := s.manager.Get(task.GetTaskID()).(*L0PreImportTask)
	s.Equal(1, len(l0Task.GetFileStats()))
	fileStats := l0Task.GetFileStats()[0]
	s.Equal(int64(s.delCnt), fileStats.GetTotalRows())
	s.Equal(s.deleteData.Size(), fileStats.GetTotalMemorySize())
	partitionStats := fileStats.GetHashedStats()[s.channel]
	s.Equal(int64(s.delCnt), partitionStats.GetPartitionRows()[s.partitionID])
	s.Equal(s.deleteData.Size(), partitionStats.GetPartitionDataSize()[s.partitionID])
}

func (s *L0ImportSuite) TestL0Import() {
	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().Alloc(mock.Anything).Return(1, int64(s.delCnt)+1, nil)
			task.(*syncmgr.SyncTask).WithAllocator(alloc)

			s.cm.(*mocks.ChunkManager).EXPECT().RootPath().Return("mock-rootpath")
			s.cm.(*mocks.ChunkManager).EXPECT().MultiWrite(mock.Anything, mock.Anything).Return(nil)
			task.(*syncmgr.SyncTask).WithChunkManager(s.cm)

			err := task.Run()
			s.NoError(err)

			future := conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			})
			return future, nil
		})

	req := &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		Files:        []*internalpb.ImportFile{{Paths: []string{"dummy-prefix"}}},
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   s.segmentID,
				PartitionID: s.partitionID,
				Vchannel:    s.channel,
			},
		},
	}
	task := NewL0ImportTask(req, s.manager, s.syncMgr, s.cm)
	s.manager.Add(task)
	fu := task.Execute()
	err := conc.AwaitAll(fu...)
	s.NoError(err)

	l0Task := s.manager.Get(task.GetTaskID()).(*L0ImportTask)
	s.Equal(1, len(l0Task.GetSegmentsInfo()))

	segmentInfo := l0Task.GetSegmentsInfo()[0]
	s.Equal(s.segmentID, segmentInfo.GetSegmentID())
	s.Equal(int64(0), segmentInfo.GetImportedRows())
	s.Equal(0, len(segmentInfo.GetBinlogs()))
	s.Equal(0, len(segmentInfo.GetStatslogs()))
	s.Equal(1, len(segmentInfo.GetDeltalogs()))

	actual := segmentInfo.GetDeltalogs()[0]
	s.Equal(1, len(actual.GetBinlogs()))

	deltaLog := actual.GetBinlogs()[0]
	s.Equal(int64(s.delCnt), deltaLog.GetEntriesNum())
	s.Equal(s.deleteData.Size(), deltaLog.GetMemorySize())
}

func TestL0Import(t *testing.T) {
	suite.Run(t, new(L0ImportSuite))
}
