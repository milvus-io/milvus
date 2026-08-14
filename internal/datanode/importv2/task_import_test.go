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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ImportTaskDeleteModeSuite covers the delete-mode write path of ImportTask.importFile:
// a delete-key file is read, projected to primary keys, and written as delete records
// into the job's L0 request segment.
type ImportTaskDeleteModeSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channel      string
	rowCount     int
	importTs     uint64

	schema  *schemapb.CollectionSchema
	pks     []int64
	manager TaskManager
	syncMgr *syncmgr.MockSyncManager
}

func (s *ImportTaskDeleteModeSuite) SetupSuite() {
	paramtable.Init()
}

func (s *ImportTaskDeleteModeSuite) SetupTest() {
	s.collectionID = 1
	s.partitionID = 2
	s.segmentID = 3
	s.channel = "ch-0"
	s.rowCount = 5
	// A distinctive, nonzero value: if the write path ever stamps deletes with anything
	// else (zero value, wall-clock time, a commit timestamp), this value discriminates it.
	s.importTs = 123456789

	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}

	s.pks = make([]int64, s.rowCount)
	for i := 0; i < s.rowCount; i++ {
		s.pks[i] = int64(i + 1)
	}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
}

// newInputChunkManager returns a ChunkManager mock that serves a delete-key JSON file
// containing s.pks as the "pk" column, the pattern used by the neighbouring preimport tests.
func (s *ImportTaskDeleteModeSuite) newInputChunkManager() *mocks.ChunkManager {
	content := &deleteKeyContent{Rows: make([]deleteKeyRow, 0, s.rowCount)}
	for _, pk := range s.pks {
		content.Rows = append(content.Rows, deleteKeyRow{PK: pk, Extra: "ignored"})
	}
	bytes, err := json.Marshal(content)
	s.Require().NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	return cm
}

func (s *ImportTaskDeleteModeSuite) newImportRequest(segments []*datapb.ImportRequestSegment) *datapb.ImportRequest {
	return &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{s.channel},
		Schema:       s.schema,
		Files:        []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Delete"},
		},
		Ts: s.importTs,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.rowCount) + 100,
		},
		RequestSegments: segments,
	}
}

// TestDeleteMode_WritesL0DeleteOnly drives a full ImportTask.Execute() over a delete-key
// file and checks the resulting sync: it targets the request's L0 segment, carries no
// insert data, holds one delete record per input row, and every delete record is stamped
// with the import request's own timetick (req.Ts) — never a later commit timestamp.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_WritesL0DeleteOnly() {
	cm := s.newInputChunkManager()
	var capturedBlob []byte
	cm.EXPECT().RootPath().Return("mock-rootpath")
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, content []byte) error {
			capturedBlob = append([]byte(nil), content...)
			return nil
		})

	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			syncTask := task.(*syncmgr.SyncTask)
			syncTask.WithChunkManager(chunkManager)
			err := syncTask.Run(ctx)
			s.Require().NoError(err)
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		})

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Require().NoError(err)

	s.NotEqual(datapb.ImportTaskStateV2_Failed, s.manager.Get(task.GetTaskID()).GetState())

	importTask := s.manager.Get(task.GetTaskID()).(*ImportTask)
	segInfos := importTask.GetSegmentsInfo()
	s.Require().Len(segInfos, 1)
	info := segInfos[0]
	s.Equal(s.segmentID, info.GetSegmentID())
	s.Empty(info.GetBinlogs())
	s.Empty(info.GetStatslogs())
	s.Require().Len(info.GetDeltalogs(), 1)
	s.Require().Len(info.GetDeltalogs()[0].GetBinlogs(), 1)
	s.EqualValues(s.rowCount, info.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())

	// Decode the actual bytes written to the delta log: this is what segcore reads,
	// so it is the only conclusive check of the per-record timestamp invariant.
	s.Require().NotEmpty(capturedBlob)
	reader, err := storage.CreateDeltalogReader([]*storage.Blob{{Value: capturedBlob}})
	s.Require().NoError(err)
	defer reader.Close()

	logs := make([]*storage.DeleteLog, 0, s.rowCount)
	for {
		log, err := reader.NextValue()
		if err != nil {
			break
		}
		if log != nil {
			logs = append(logs, *log)
		}
	}
	s.Require().Len(logs, s.rowCount)
	gotPks := make([]int64, 0, s.rowCount)
	for _, l := range logs {
		s.EqualValues(s.importTs, l.Ts)
		gotPks = append(gotPks, l.Pk.GetValue().(int64))
	}
	s.ElementsMatch(s.pks, gotPks)
}

// TestDeleteMode_RequiresL0RequestSegment checks that delete-mode picks a request segment
// by L0 level specifically: with only an L1 segment offered for the channel/partition, the
// task must fail rather than silently writing into the L1 segment.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_RequiresL0RequestSegment() {
	cm := s.newInputChunkManager()
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L1},
	}
	req := s.newImportRequest(segments)

	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	futures := task.Execute()
	err := conc.AwaitAll(futures...)
	s.Error(err)

	got := s.manager.Get(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, got.GetState())
	s.Contains(got.GetReason(), "L0")
}

// TestDeleteMode_ProcessesAllReadBatches checks that importFile keeps reading and syncing
// every batch a reader returns, rather than stopping after the first one.
func (s *ImportTaskDeleteModeSuite) TestDeleteMode_ProcessesAllReadBatches() {
	segments := []*datapb.ImportRequestSegment{
		{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: s.channel, Level: datapb.SegmentLevel_L0},
	}
	req := s.newImportRequest(segments)

	task := NewImportTask(req, s.manager, s.syncMgr, mocks.NewChunkManager(s.T())).(*ImportTask)

	batch1 := &storage.InsertData{Data: map[int64]storage.FieldData{100: &storage.Int64FieldData{Data: []int64{1, 2}}}}
	batch2 := &storage.InsertData{Data: map[int64]storage.FieldData{100: &storage.Int64FieldData{Data: []int64{3, 4, 5}}}}

	reader := importutilv2.NewMockReader(s.T())
	reader.EXPECT().Read().Return(batch1, nil).Once()
	reader.EXPECT().Read().Return(batch2, nil).Once()
	reader.EXPECT().Read().Return(nil, io.EOF).Once()

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, syncTask syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			return conc.Go(func() (struct{}, error) {
				return struct{}{}, nil
			}), nil
		}).Times(2)

	err := task.importFile(reader)
	s.Require().NoError(err)
}

func TestImportTaskDeleteMode(t *testing.T) {
	suite.Run(t, new(ImportTaskDeleteModeSuite))
}
