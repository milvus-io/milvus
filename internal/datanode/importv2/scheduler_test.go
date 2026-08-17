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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type sampleRow struct {
	FieldString      string    `json:"pk,omitempty"`
	FieldInt64       int64     `json:"int64,omitempty"`
	FieldFloatVector []float32 `json:"vec,omitempty"`
}

type sampleContent struct {
	Rows []sampleRow `json:"rows,omitempty"`
}

type mockReader struct {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
	size int64
}

func (mr *mockReader) Size() (int64, error) {
	return mr.size, nil
}

// gatedSyncFuture builds the future a stubbed SyncDataWithChunkManager returns:
// it blocks until release is closed (nil means no gate), then settles firstErr
// through the submission callbacks exactly like the real dispatcher does.
func gatedSyncFuture(release chan struct{}, firstErr error, callbacks ...func(error) error) *conc.Future[struct{}] {
	return conc.Go(func() (struct{}, error) {
		if release != nil {
			<-release
		}
		err := firstErr
		for _, callback := range callbacks {
			err = callback(err)
		}
		return struct{}{}, err
	})
}

// gatedSyncStub is a whole SyncDataWithChunkManager replacement around
// gatedSyncFuture, for tests that need no extra per-call behavior.
func gatedSyncStub(release chan struct{}, firstErr error) func(context.Context, syncmgr.Task, storage.ChunkManager, ...func(error) error) (*conc.Future[struct{}], error) {
	return func(_ context.Context, _ syncmgr.Task, _ storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
		return gatedSyncFuture(release, firstErr, callbacks...), nil
	}
}

// immediateSyncFuture settles err at once WITHOUT running the submission
// callbacks — the historical shape of the plain success/failure-path stubs.
func immediateSyncFuture(err error) *conc.Future[struct{}] {
	return conc.Go(func() (struct{}, error) {
		return struct{}{}, err
	})
}

// immediateSyncStub is a whole SyncDataWithChunkManager replacement around
// immediateSyncFuture.
func immediateSyncStub(err error) func(context.Context, syncmgr.Task, storage.ChunkManager, ...func(error) error) (*conc.Future[struct{}], error) {
	return func(context.Context, syncmgr.Task, storage.ChunkManager, ...func(error) error) (*conc.Future[struct{}], error) {
		return immediateSyncFuture(err), nil
	}
}

// closeOnSettle returns a settle callback that closes ch and passes the error
// through, used to observe when a stubbed sync settles relative to the real
// submission callbacks.
func closeOnSettle(ch chan struct{}) func(error) error {
	return func(err error) error {
		close(ch)
		return err
	}
}

type SchedulerSuite struct {
	suite.Suite

	numRows int
	schema  *schemapb.CollectionSchema

	cm        storage.ChunkManager
	reader    *importutilv2.MockReader
	syncMgr   *syncmgr.MockSyncManager
	manager   TaskManager
	scheduler *scheduler
}

func (s *SchedulerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *SchedulerSuite) SetupTest() {
	s.numRows = 100
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
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "int64",
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	s.manager = NewTaskManager()
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.scheduler = NewScheduler(s.manager).(*scheduler)
}

func (s *SchedulerSuite) TearDownTest() {
	s.scheduler.Close()
}

// buildImportRequest returns the canonical ImportRequest these tests share —
// JobID 10, TaskID 11, collection 12, partition 13, Ts 1000, IDRange [0, 1000)
// — with the given vchannels and request segments. Tests override individual
// fields on the returned request where they deliberately differ.
func (s *SchedulerSuite) buildImportRequest(vchannels []string, segments []*datapb.ImportRequestSegment) *datapb.ImportRequest {
	return &datapb.ImportRequest{
		JobID:           10,
		TaskID:          11,
		CollectionID:    12,
		PartitionIDs:    []int64{13},
		Vchannels:       vchannels,
		Schema:          s.schema,
		Ts:              1000,
		IDRange:         &datapb.IDRange{Begin: 0, End: 1000},
		RequestSegments: segments,
	}
}

func (s *SchedulerSuite) TestScheduler_Slots() {
	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		TaskSlot:     10,
	}
	preimportTask := NewPreImportTask(preimportReq, s.manager, s.cm)
	s.manager.Add(preimportTask)

	slots := s.scheduler.Slots()
	s.Equal(int64(10), slots)
}

func (s *SchedulerSuite) TestScheduler_Start_Preimport() {
	content := &sampleContent{
		Rows: make([]sampleRow, 0),
	}
	for i := 0; i < 10; i++ {
		row := sampleRow{
			FieldString:      "No." + strconv.FormatInt(int64(i), 10),
			FieldInt64:       int64(99999999999999999 + i),
			FieldFloatVector: []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
		}
		content.Rows = append(content.Rows, row)
	}
	bytes, err := json.Marshal(content)
	s.NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(1024, nil)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	s.cm = cm

	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
	}
	preimportTask := NewPreImportTask(preimportReq, s.manager, s.cm)
	s.manager.Add(preimportTask)

	go s.scheduler.Start()
	defer s.scheduler.Close()
	s.Eventually(func() bool {
		return s.manager.Get(preimportTask.GetTaskID()).GetState() == datapb.ImportTaskStateV2_Completed
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *SchedulerSuite) TestScheduler_Start_Preimport_Failed() {
	content := &sampleContent{
		Rows: make([]sampleRow, 0),
	}
	for i := 0; i < 10; i++ {
		var row sampleRow
		if i == 0 { // make rows not consistent
			row = sampleRow{
				FieldString:      "No." + strconv.FormatInt(int64(i), 10),
				FieldFloatVector: []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
			}
		} else {
			row = sampleRow{
				FieldString:      "No." + strconv.FormatInt(int64(i), 10),
				FieldInt64:       int64(99999999999999999 + i),
				FieldFloatVector: []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
			}
		}
		content.Rows = append(content.Rows, row)
	}
	bytes, err := json.Marshal(content)
	s.NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(1024, nil)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	s.cm = cm

	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
	}
	preimportTask := NewPreImportTask(preimportReq, s.manager, s.cm)
	s.manager.Add(preimportTask)

	go s.scheduler.Start()
	defer s.scheduler.Close()
	s.Eventually(func() bool {
		return s.manager.Get(preimportTask.GetTaskID()).GetState() == datapb.ImportTaskStateV2_Failed
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *SchedulerSuite) TestScheduler_Start_Import() {
	content := &sampleContent{
		Rows: make([]sampleRow, 0),
	}
	for i := 0; i < 10; i++ {
		row := sampleRow{
			FieldString:      "No." + strconv.FormatInt(int64(i), 10),
			FieldInt64:       int64(99999999999999999 + i),
			FieldFloatVector: []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
		}
		content.Rows = append(content.Rows, row)
	}
	bytes, err := json.Marshal(content)
	s.NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	s.cm = cm

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(immediateSyncStub(nil))
	importReq := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	importReq.Files = []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}}
	importReq.IDRange.End = int64(s.numRows)
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)

	go s.scheduler.Start()
	defer s.scheduler.Close()
	s.Eventually(func() bool {
		return s.manager.Get(importTask.GetTaskID()).GetState() == datapb.ImportTaskStateV2_Completed
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *SchedulerSuite) TestScheduler_Start_Import_Failed() {
	content := &sampleContent{
		Rows: make([]sampleRow, 0),
	}
	for i := 0; i < 10; i++ {
		row := sampleRow{
			FieldString:      "No." + strconv.FormatInt(int64(i), 10),
			FieldInt64:       int64(99999999999999999 + i),
			FieldFloatVector: []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
		}
		content.Rows = append(content.Rows, row)
	}
	bytes, err := json.Marshal(content)
	s.NoError(err)

	cm := mocks.NewChunkManager(s.T())
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)
	s.cm = cm

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(immediateSyncStub(errors.New("mock err")))
	importReq := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	importReq.Files = []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}}
	importReq.IDRange.End = int64(s.numRows)
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)

	go s.scheduler.Start()
	defer s.scheduler.Close()
	s.Eventually(func() bool {
		return s.manager.Get(importTask.GetTaskID()).GetState() == datapb.ImportTaskStateV2_Failed
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *SchedulerSuite) TestScheduler_ReadFileStat() {
	importFile := &internalpb.ImportFile{
		Paths: []string{"dummy.json"},
	}

	var once sync.Once
	data, err := testutil.CreateInsertData(s.schema, s.numRows)
	s.NoError(err)
	s.reader = importutilv2.NewMockReader(s.T())
	s.reader.EXPECT().Size().Return(1024, nil)
	s.reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		var res *storage.InsertData
		once.Do(func() {
			res = data
		})
		if res != nil {
			return res, nil
		}
		return nil, io.EOF
	})
	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{importFile},
	}
	preimportTask := NewPreImportTask(preimportReq, s.manager, s.cm)
	s.manager.Add(preimportTask)
	err = preimportTask.(*PreImportTask).readFileStat(s.reader, 0)
	s.NoError(err)
}

func (s *SchedulerSuite) TestScheduler_ImportFile() {
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(immediateSyncStub(nil))
	var once sync.Once
	data, err := testutil.CreateInsertData(s.schema, s.numRows)
	s.NoError(err)
	s.reader = importutilv2.NewMockReader(s.T())
	s.reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		var res *storage.InsertData
		once.Do(func() {
			res = data
		})
		if res != nil {
			return res, nil
		}
		return nil, io.EOF
	})
	importReq := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	importReq.Files = []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}}
	importReq.IDRange.End = int64(s.numRows)
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)
	err = importTask.(*ImportTask).importFile(s.reader)
	s.NoError(err)
}

func (s *SchedulerSuite) TestImportSyncPreservesAcceptedFuturesOnPickSegmentFailure() {
	data, err := testutil.CreateInsertData(s.schema, 1)
	s.Require().NoError(err)
	release := make(chan struct{})
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(gatedSyncStub(release, nil)).Once()

	// Two vchannels but a request segment only for v0: picking a segment for v1
	// fails after v0's sync was already accepted.
	req := s.buildImportRequest([]string{"v0", "v1"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	task := NewImportTask(req, s.manager, s.syncMgr, s.cm).(*ImportTask)
	futures, tasks, err := task.sync(HashedData{{data}, {data}})
	s.Error(err)
	s.Len(futures, 1)
	s.Len(tasks, 1)
	close(release)
	_, err = futures[0].Await()
	s.NoError(err)
}

// A submission the dispatcher refuses (SyncDataWithChunkManager returns an
// error) never gets its releaseOnDone callback, so ImportTask.sync must settle
// it by hand via Abandon. The payload-accounting hook fires exactly once, from
// Abandon's release path, making the abandon observable.
func (s *SchedulerSuite) TestImportSyncAbandonsRefusedSubmission() {
	data, err := testutil.CreateInsertData(s.schema, 1)
	s.Require().NoError(err)
	release := make(chan struct{})
	refusedAbandoned := make(chan struct{})
	submitErr := errors.New("dispatcher refused submission")
	call := 0
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, task syncmgr.Task, _ storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			call++
			if call == 2 {
				task.(*syncmgr.SyncTask).WithPayloadAccounting(1, 0, func(int64) {
					close(refusedAbandoned)
				})
				return nil, submitErr
			}
			return gatedSyncFuture(release, nil, callbacks...), nil
		}).Times(2)

	req := s.buildImportRequest([]string{"v0", "v1"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
		{SegmentID: 15, PartitionID: 13, Vchannel: "v1"},
	})
	task := NewImportTask(req, s.manager, s.syncMgr, s.cm).(*ImportTask)
	futures, tasks, err := task.sync(HashedData{{data}, {data}})
	s.ErrorIs(err, submitErr)
	s.Len(futures, 1)
	s.Len(tasks, 1)
	// sync abandons the refused task synchronously, before returning.
	select {
	case <-refusedAbandoned:
	default:
		s.Fail("the refused sync task must be abandoned by the caller")
	}
	close(release)
	_, err = futures[0].Await()
	s.NoError(err)
}

// The read-to-EOF path must fence on ALL submitted syncs (task_import.go's
// explicit BlockOnAll after the loop): a fast-failing first sync must not let
// importFile return while a sibling sync is still in flight.
func (s *SchedulerSuite) TestScheduler_ImportFileEOFPathWaitsForInFlightSync() {
	data, err := testutil.CreateInsertData(s.schema, 100)
	s.Require().NoError(err)
	// The batch must hash rows into BOTH vchannels so one batch produces two
	// sync submissions.
	hash := hashByVChannel(2, s.schema.GetFields()[0])
	seen := make(map[int64]struct{})
	for _, pk := range data.Data[100].(*storage.StringFieldData).Data {
		seen[hash(pk)] = struct{}{}
	}
	s.Require().Len(seen, 2, "test data must cover both vchannels")

	release := make(chan struct{})
	firstFailed := make(chan struct{})
	secondSubmitted := make(chan struct{})
	firstErr := errors.New("first sync failed fast")
	var calls atomic.Int32
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			if calls.Add(1) == 1 {
				return gatedSyncFuture(nil, firstErr, append(callbacks, closeOnSettle(firstFailed))...), nil
			}
			close(secondSubmitted)
			return gatedSyncFuture(release, nil, callbacks...), nil
		}).Times(2)

	var reads atomic.Int32
	reader := importutilv2.NewMockReader(s.T())
	reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		if reads.Add(1) == 1 {
			return data, nil
		}
		return nil, io.EOF
	})

	req := s.buildImportRequest([]string{"v0", "v1"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
		{SegmentID: 15, PartitionID: 13, Vchannel: "v1"},
	})
	task := NewImportTask(req, s.manager, s.syncMgr, s.cm).(*ImportTask)
	result := make(chan error, 1)
	go func() {
		result <- task.importFile(reader)
	}()

	<-secondSubmitted
	<-firstFailed
	select {
	case err := <-result:
		s.Fail("importFile returned before the in-flight sync completed", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	s.ErrorIs(<-result, firstErr)
}

func (s *SchedulerSuite) TestScheduler_ImportFileDrainsSubmittedSyncOnReadFailure() {
	release := make(chan struct{})
	submitted := make(chan struct{})
	readFailed := make(chan struct{})
	syncErr := errors.New("accepted sync also failed")
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			close(submitted)
			return gatedSyncFuture(release, syncErr, callbacks...), nil
		}).Once()

	data, err := testutil.CreateInsertData(s.schema, 10)
	s.NoError(err)
	readErr := errors.New("read failed after submit")
	var reads atomic.Int32
	reader := importutilv2.NewMockReader(s.T())
	reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		if reads.Add(1) == 1 {
			return data, nil
		}
		close(readFailed)
		return nil, readErr
	})

	req := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	task := NewImportTask(req, s.manager, s.syncMgr, s.cm).(*ImportTask)
	result := make(chan error, 1)
	go func() {
		result <- task.importFile(reader)
	}()

	<-submitted
	<-readFailed
	select {
	case err := <-result:
		s.Fail("importFile returned before its submitted sync completed", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	s.ErrorIs(<-result, readErr)
}

func (s *SchedulerSuite) TestScheduler_FinalFailureWaitsForAllImportFiles() {
	content := &sampleContent{Rows: []sampleRow{{
		FieldString:      "pk",
		FieldInt64:       1,
		FieldFloatVector: []float32{1, 2, 3, 4},
	}}}
	bytes, err := json.Marshal(content)
	s.NoError(err)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
		reader := strings.NewReader(string(bytes))
		return &mockReader{Reader: reader, Closer: io.NopCloser(reader)}, nil
	}).Times(2)

	release := make(chan struct{})
	failedSync := make(chan struct{})
	var calls atomic.Int32
	syncErr := errors.New("first file sync failed")
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			if calls.Add(1) == 1 {
				// close(failedSync) fires when the future settles syncErr, before
				// the submission callbacks run, matching the pre-helper ordering.
				return gatedSyncFuture(nil, syncErr, append([]func(error) error{closeOnSettle(failedSync)}, callbacks...)...), nil
			}
			return gatedSyncFuture(release, nil, callbacks...), nil
		}).Times(2)

	req := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	req.Files = []*internalpb.ImportFile{
		{Paths: []string{"first.json"}},
		{Paths: []string{"second.json"}},
	}
	task := NewImportTask(req, s.manager, s.syncMgr, cm)
	s.manager.Add(task)
	done := make(chan struct{})
	go func() {
		s.scheduler.scheduleTasks()
		close(done)
	}()

	<-failedSync
	s.Eventually(func() bool { return calls.Load() == 2 }, time.Second, 10*time.Millisecond)
	select {
	case <-done:
		s.Fail("scheduler returned before the sibling file completed")
	case <-time.After(100 * time.Millisecond):
	}
	s.Equal(datapb.ImportTaskStateV2_InProgress, s.manager.Get(task.GetTaskID()).GetState())

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.Fail("scheduler did not finish after all file futures completed")
	}
	failed := s.manager.Get(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, failed.GetState())
	// The worker-recorded reason must survive the scheduler's terminal publish:
	// the scheduler sets only the state, never overwrites the reason.
	s.Contains(failed.GetReason(), syncErr.Error())
}

func (s *SchedulerSuite) TestScheduler_ImportFileWithFunction() {
	paramtable.Init()
	paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
		return map[string]string{
			"mock.apikey": "mock",
		}
	}

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(immediateSyncStub(nil))
	ts := embedding.CreateOpenAIEmbeddingServer()
	defer ts.Close()
	paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
		return map[string]string{
			"openai.url": ts.URL,
		}
	}
	schema := &schemapb.CollectionSchema{
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
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "int64",
				DataType: schemapb.DataType_Int64,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "test",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldIds:    []int64{100},
				InputFieldNames:  []string{"text"},
				OutputFieldIds:   []int64{101},
				OutputFieldNames: []string{"vec"},
				Params: []*commonpb.KeyValuePair{
					{Key: "provider", Value: "openai"},
					{Key: "model_name", Value: "text-embedding-ada-002"},
					{Key: "credential", Value: "mock"},
					{Key: "dim", Value: "4"},
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionAllowInsertNonBM25FunctionOutputs, Value: "true"}},
	}

	var once sync.Once
	data, err := testutil.CreateInsertData(schema, s.numRows)
	s.NoError(err)
	s.reader = importutilv2.NewMockReader(s.T())
	s.reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		var res *storage.InsertData
		once.Do(func() {
			res = data
		})
		if res != nil {
			return res, nil
		}
		return nil, io.EOF
	})
	importReq := s.buildImportRequest([]string{"v0"}, []*datapb.ImportRequestSegment{
		{SegmentID: 14, PartitionID: 13, Vchannel: "v0"},
	})
	importReq.Schema = schema
	importReq.Files = []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}}
	importReq.IDRange.End = int64(s.numRows)
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)
	err = importTask.(*ImportTask).importFile(s.reader)
	s.NoError(err)
}

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}
