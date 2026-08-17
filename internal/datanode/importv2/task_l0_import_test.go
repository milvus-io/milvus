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
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type oneBatchL0Reader struct {
	data *storage.DeleteData
	read bool
}

func (r *oneBatchL0Reader) Read() (*storage.DeleteData, error) {
	if r.read {
		return nil, errors.New("unexpected second read")
	}
	r.read = true
	return r.data, nil
}

type oneBatchThenEOFL0Reader struct {
	data *storage.DeleteData
	read bool
}

func (r *oneBatchThenEOFL0Reader) Read() (*storage.DeleteData, error) {
	if r.read {
		return nil, io.EOF
	}
	r.read = true
	return r.data, nil
}

// deleteDataCoveringChannels builds DeleteData whose primary keys hash to every
// one of the given vchannels, so a single batch fans out into one sync
// submission per channel.
func (s *L0ImportSuite) deleteDataCoveringChannels(channels []string) *storage.DeleteData {
	deleteData := storage.NewDeleteData(nil, nil)
	hash := hashByVChannel(int64(len(channels)), s.schema.GetFields()[0])
	seen := make(map[int64]struct{})
	for i := 0; i < 1000 && len(seen) < len(channels); i++ {
		pk := fmt.Sprintf("pk-%d", i)
		channelIdx := hash(pk)
		if _, ok := seen[channelIdx]; ok {
			continue
		}
		seen[channelIdx] = struct{}{}
		deleteData.Append(storage.NewVarCharPrimaryKey(pk), uint64(i+1))
	}
	s.Require().Len(seen, len(channels))
	return deleteData
}

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
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).Return([][]byte{blob.Value}, nil).Maybe()
	cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, walkFunc storage.ChunkObjectWalkFunc) error {
			for _, file := range []string{"a/b/c/"} {
				walkFunc(&storage.ChunkObjectInfo{FilePath: file})
			}
			return nil
		}).Maybe()
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
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().AllocOne().Return(1, nil)
			task.(*syncmgr.SyncTask).WithAllocator(alloc)

			s.cm.(*mocks.ChunkManager).EXPECT().RootPath().Return("mock-rootpath")
			s.cm.(*mocks.ChunkManager).EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			task.(*syncmgr.SyncTask).WithChunkManager(s.cm)

			taskCtx := context.Background()
			err := task.Prepare(taskCtx)
			if err == nil {
				err = task.Commit(taskCtx)
			}
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
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.delCnt),
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
	// s.Equal(s.deleteData.Size(), deltaLog.GetMemorySize())
}

func (s *L0ImportSuite) TestL0ImportForcesStorageV2ForSyncTask() {
	var task *L0ImportTask
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, syncTask syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			segment, ok := task.metaCaches[s.channel].GetSegmentByID(s.segmentID)
			s.True(ok)
			s.EqualValues(storage.StorageV2, segment.GetStorageVersion())
			s.Empty(segment.ManifestPath())

			return immediateSyncFuture(nil), nil
		})

	req := &datapb.ImportRequest{
		JobID:          1,
		TaskID:         2,
		CollectionID:   s.collectionID,
		PartitionIDs:   []int64{s.partitionID},
		Vchannels:      []string{s.channel},
		Schema:         s.schema,
		StorageVersion: storage.StorageV3,
		UseLoonFfi:     true,
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   s.segmentID,
				PartitionID: s.partitionID,
				Vchannel:    s.channel,
			},
		},
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.delCnt),
		},
	}
	task = NewL0ImportTask(req, s.manager, s.syncMgr, s.cm).(*L0ImportTask)

	futures, syncTasks, err := task.syncDelete([]*storage.DeleteData{s.deleteData})
	s.NoError(err)
	s.Len(futures, 1)
	s.Len(syncTasks, 1)
}

func (s *L0ImportSuite) TestL0ImportDrainsPartialSubmissionBeforeReturning() {
	channels := []string{"ch-0", "ch-1"}
	deleteData := s.deleteDataCoveringChannels(channels)

	release := make(chan struct{})
	firstSubmitted := make(chan struct{})
	submitFailed := make(chan struct{})
	refusedAbandoned := make(chan struct{})
	submitErr := errors.New("second submission failed")
	call := 0
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			call++
			if call == 2 {
				// A refused task never reaches the dispatcher, so releaseOnDone
				// will never fire — the caller must Abandon it by hand. The
				// payload-accounting hook fires exactly once, from Abandon's
				// releasePayload, making the abandon observable.
				task.(*syncmgr.SyncTask).WithPayloadAccounting(1, 0, func(int64) {
					close(refusedAbandoned)
				})
				close(submitFailed)
				return nil, submitErr
			}
			close(firstSubmitted)
			return gatedSyncFuture(release, nil, callbacks...), nil
		}).Times(2)

	req := &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    channels,
		Schema:       s.schema,
		IDRange:      &datapb.IDRange{Begin: 0, End: 1000},
		RequestSegments: []*datapb.ImportRequestSegment{
			{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: channels[0]},
			{SegmentID: s.segmentID + 1, PartitionID: s.partitionID, Vchannel: channels[1]},
		},
	}
	task := NewL0ImportTask(req, s.manager, s.syncMgr, s.cm).(*L0ImportTask)
	result := make(chan error, 1)
	go func() {
		result <- task.importL0(&oneBatchL0Reader{data: deleteData})
	}()

	<-firstSubmitted
	<-submitFailed
	select {
	case err := <-result:
		s.Fail("importL0 returned before the accepted sync completed", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	s.ErrorIs(<-result, submitErr)
	select {
	case <-refusedAbandoned:
	default:
		s.Fail("the refused sync task must be abandoned by the caller")
	}
}

// The read-to-EOF path must fence on ALL submitted syncs (task_l0_import.go's
// explicit BlockOnAll after the loop): a fast-failing first sync must not let
// importL0 return while a sibling sync is still in flight.
func (s *L0ImportSuite) TestL0ImportEOFPathWaitsForInFlightSync() {
	channels := []string{"ch-0", "ch-1"}
	deleteData := s.deleteDataCoveringChannels(channels)

	release := make(chan struct{})
	firstFailed := make(chan struct{})
	secondSubmitted := make(chan struct{})
	firstErr := errors.New("first sync failed fast")
	call := 0
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			call++
			if call == 1 {
				return gatedSyncFuture(nil, firstErr, append(callbacks, closeOnSettle(firstFailed))...), nil
			}
			close(secondSubmitted)
			return gatedSyncFuture(release, nil, callbacks...), nil
		}).Times(2)

	req := &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    channels,
		Schema:       s.schema,
		IDRange:      &datapb.IDRange{Begin: 0, End: 1000},
		RequestSegments: []*datapb.ImportRequestSegment{
			{SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: channels[0]},
			{SegmentID: s.segmentID + 1, PartitionID: s.partitionID, Vchannel: channels[1]},
		},
	}
	task := NewL0ImportTask(req, s.manager, s.syncMgr, s.cm).(*L0ImportTask)
	result := make(chan error, 1)
	go func() {
		result <- task.importL0(&oneBatchThenEOFL0Reader{data: deleteData})
	}()

	<-secondSubmitted
	<-firstFailed
	select {
	case err := <-result:
		s.Fail("importL0 returned before the in-flight sync completed", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	s.ErrorIs(<-result, firstErr)
}

func (s *L0ImportSuite) TestSyncDeletePreservesAcceptedFuturesOnPickSegmentFailure() {
	release := make(chan struct{})
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(gatedSyncStub(release, nil)).Once()

	req := &datapb.ImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		PartitionIDs: []int64{s.partitionID},
		Vchannels:    []string{"ch-0", "ch-1"},
		Schema:       s.schema,
		IDRange:      &datapb.IDRange{Begin: 0, End: 1000},
		RequestSegments: []*datapb.ImportRequestSegment{{
			SegmentID: s.segmentID, PartitionID: s.partitionID, Vchannel: "ch-0",
		}},
	}
	task := NewL0ImportTask(req, s.manager, s.syncMgr, s.cm).(*L0ImportTask)
	first := storage.NewDeleteData([]storage.PrimaryKey{storage.NewVarCharPrimaryKey("a")}, []uint64{1})
	second := storage.NewDeleteData([]storage.PrimaryKey{storage.NewVarCharPrimaryKey("b")}, []uint64{2})
	futures, tasks, err := task.syncDelete([]*storage.DeleteData{first, second})
	s.Error(err)
	s.Len(futures, 1)
	s.Len(tasks, 1)
	close(release)
	_, err = futures[0].Await()
	s.NoError(err)
}

func TestL0Import(t *testing.T) {
	suite.Run(t, new(L0ImportSuite))
}
