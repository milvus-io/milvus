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

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, nil
		})
		return future, nil
	})
	importReq := &datapb.ImportRequest{
		JobID:        10,
		TaskID:       11,
		CollectionID: 12,
		PartitionIDs: []int64{13},
		Vchannels:    []string{"v0"},
		Schema:       s.schema,
		Files: []*internalpb.ImportFile{
			{
				Paths: []string{"dummy.json"},
			},
		},
		Ts: 1000,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.numRows),
		},
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   14,
				PartitionID: 13,
				Vchannel:    "v0",
			},
		},
	}
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

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, errors.New("mock err")
		})
		return future, nil
	})
	importReq := &datapb.ImportRequest{
		JobID:        10,
		TaskID:       11,
		CollectionID: 12,
		PartitionIDs: []int64{13},
		Vchannels:    []string{"v0"},
		Schema:       s.schema,
		Files: []*internalpb.ImportFile{
			{
				Paths: []string{"dummy.json"},
			},
		},
		Ts: 1000,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.numRows),
		},
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   14,
				PartitionID: 13,
				Vchannel:    "v0",
			},
		},
	}
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
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, nil
		})
		return future, nil
	})
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
	importReq := &datapb.ImportRequest{
		JobID:        10,
		TaskID:       11,
		CollectionID: 12,
		PartitionIDs: []int64{13},
		Vchannels:    []string{"v0"},
		Schema:       s.schema,
		Files: []*internalpb.ImportFile{
			{
				Paths: []string{"dummy.json"},
			},
		},
		Ts: 1000,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.numRows),
		},
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   14,
				PartitionID: 13,
				Vchannel:    "v0",
			},
		},
	}
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)
	err = importTask.(*ImportTask).importFile(s.reader, nil)
	s.NoError(err)
}

func (s *SchedulerSuite) TestScheduler_ImportFileWithFunction() {
	paramtable.Init()
	paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
		return map[string]string{
			"mock.apikey": "mock",
		}
	}

	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, nil
		})
		return future, nil
	})
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
	importReq := &datapb.ImportRequest{
		JobID:        10,
		TaskID:       11,
		CollectionID: 12,
		PartitionIDs: []int64{13},
		Vchannels:    []string{"v0"},
		Schema:       schema,
		Files: []*internalpb.ImportFile{
			{
				Paths: []string{"dummy.json"},
			},
		},
		Ts: 1000,
		IDRange: &datapb.IDRange{
			Begin: 0,
			End:   int64(s.numRows),
		},
		RequestSegments: []*datapb.ImportRequestSegment{
			{
				SegmentID:   14,
				PartitionID: 13,
				Vchannel:    "v0",
			},
		},
	}
	importTask := NewImportTask(importReq, s.manager, s.syncMgr, s.cm)
	s.manager.Add(importTask)
	err = importTask.(*ImportTask).importFile(s.reader, nil)
	s.NoError(err)
}

// fakeTask embeds ImportTask with a custom Execute function for scheduler tests.
type fakeTask struct {
	*ImportTask
	manager   TaskManager
	executeFn func() []*conc.Future[any]
}

func newFakeImportTask(taskID int64, manager TaskManager, executeFn func() []*conc.Future[any]) *fakeTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeTask{
		ImportTask: &ImportTask{
			ImportTaskV2: &datapb.ImportTaskV2{
				JobID:  taskID,
				TaskID: taskID,
				State:  datapb.ImportTaskStateV2_Pending,
			},
			ctx:     ctx,
			cancel:  cancel,
			req:     &datapb.ImportRequest{},
			manager: manager,
		},
		manager:   manager,
		executeFn: executeFn,
	}
}

func (t *fakeTask) Execute() []*conc.Future[any] {
	t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
	return t.executeFn()
}

// Clone returns an ImportTask for the manager's state updates.
func (t *fakeTask) Clone() Task {
	return t.ImportTask.Clone()
}

func (s *SchedulerSuite) TestScheduler_ScheduleWithoutBatchBlocking() {
	release1 := make(chan struct{})
	started1 := make(chan struct{})
	var once1 sync.Once
	task1 := newFakeImportTask(1001, s.manager, func() []*conc.Future[any] {
		once1.Do(func() { close(started1) })
		return []*conc.Future[any]{conc.Go(func() (any, error) {
			<-release1
			return nil, nil
		})}
	})
	s.manager.Add(task1)

	go s.scheduler.Start()
	defer s.scheduler.Close()

	// Wait for task1 to start.
	select {
	case <-started1:
	case <-time.After(10 * time.Second):
		s.FailNow("task1 was not scheduled")
	}

	// Schedule task2 while task1 is still running.
	task2 := newFakeImportTask(1002, s.manager, func() []*conc.Future[any] {
		return []*conc.Future[any]{conc.Go(func() (any, error) {
			return nil, nil
		})}
	})
	s.manager.Add(task2)

	s.Eventually(func() bool {
		return s.manager.Get(1002).GetState() == datapb.ImportTaskStateV2_Completed
	}, 10*time.Second, 100*time.Millisecond)
	s.Equal(datapb.ImportTaskStateV2_InProgress, s.manager.Get(1001).GetState())

	close(release1)
	s.Eventually(func() bool {
		return s.manager.Get(1001).GetState() == datapb.ImportTaskStateV2_Completed
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *SchedulerSuite) TestScheduler_ReapCompletedTasks() {
	doneTask := newFakeImportTask(2001, s.manager, nil)
	blockedTask := newFakeImportTask(2002, s.manager, nil)
	s.manager.Add(doneTask)
	s.manager.Add(blockedTask)

	release := make(chan struct{})
	defer close(release)
	doneFuture := conc.Go(func() (any, error) { return nil, nil })
	blockedFuture := conc.Go(func() (any, error) {
		<-release
		return nil, nil
	})
	s.scheduler.futures[2001] = []*conc.Future[any]{doneFuture}
	s.scheduler.futures[2002] = []*conc.Future[any]{blockedFuture}

	s.Eventually(func() bool { return doneFuture.Done() }, 5*time.Second, 10*time.Millisecond)

	s.scheduler.reapCompletedTasks()

	_, ok := s.scheduler.futures[2001]
	s.False(ok)
	_, ok = s.scheduler.futures[2002]
	s.True(ok)
	s.Equal(datapb.ImportTaskStateV2_Completed, s.manager.Get(2001).GetState())
	s.Equal(datapb.ImportTaskStateV2_Pending, s.manager.Get(2002).GetState())
}

func (s *SchedulerSuite) TestScheduler_ReapDropsFailedAndRemovedTasks() {
	failedTask := newFakeImportTask(3001, s.manager, nil)
	removedTask := newFakeImportTask(3002, s.manager, nil)
	s.manager.Add(failedTask)
	s.manager.Add(removedTask)

	// Both tasks still have an unfinished file future.
	release := make(chan struct{})
	defer close(release)
	blocked := func() *conc.Future[any] {
		return conc.Go(func() (any, error) {
			<-release
			return nil, nil
		})
	}
	s.scheduler.futures[3001] = []*conc.Future[any]{blocked()}
	s.scheduler.futures[3002] = []*conc.Future[any]{blocked()}

	// Mark one task Failed and remove the other.
	s.manager.Update(3001, UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason("mock failure"))
	s.manager.Remove(3002)

	s.scheduler.reapCompletedTasks()

	s.Empty(s.scheduler.futures)
	s.Equal(datapb.ImportTaskStateV2_Failed, s.manager.Get(3001).GetState())
	s.Nil(s.manager.Get(3002))
}

func (s *SchedulerSuite) TestScheduler_FailedTaskWithNoFuturesStaysFailed() {
	// Simulate validation failure with no returned futures.
	task := newFakeImportTask(3003, s.manager, nil)
	task.executeFn = func() []*conc.Future[any] {
		s.manager.Update(3003, UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason("validation failed"))
		return nil
	}
	s.manager.Add(task)

	s.scheduler.scheduleTasks()

	s.Empty(s.scheduler.futures)
	s.Equal(datapb.ImportTaskStateV2_Failed, s.manager.Get(3003).GetState())
	s.Equal("validation failed", s.manager.Get(3003).GetReason())
}

func (s *SchedulerSuite) TestScheduler_ReapBetweenTasks() {
	// Schedule a completed future followed by an unfinished one.
	doneTask := newFakeImportTask(4001, s.manager, func() []*conc.Future[any] {
		f := conc.Go(func() (any, error) { return nil, nil })
		_, _ = f.Await()
		return []*conc.Future[any]{f}
	})
	release := make(chan struct{})
	defer close(release)
	// Execute of the second task runs inside scheduleTasks, right after the
	// first task was submitted. Reaping between tasks must already have marked
	// the first task Completed at that point; reaping only after the loop
	// would still leave it InProgress here.
	var firstStateSeenBySecond datapb.ImportTaskStateV2
	blockedTask := newFakeImportTask(4002, s.manager, func() []*conc.Future[any] {
		firstStateSeenBySecond = s.manager.Get(4001).GetState()
		return []*conc.Future[any]{conc.Go(func() (any, error) {
			<-release
			return nil, nil
		})}
	})
	s.manager.Add(doneTask)
	s.manager.Add(blockedTask)

	s.scheduler.scheduleTasks()

	s.Equal(datapb.ImportTaskStateV2_Completed, firstStateSeenBySecond)
	s.Equal(datapb.ImportTaskStateV2_Completed, s.manager.Get(4001).GetState())
	s.Equal(datapb.ImportTaskStateV2_InProgress, s.manager.Get(4002).GetState())
	_, ok := s.scheduler.futures[4001]
	s.False(ok)
	_, ok = s.scheduler.futures[4002]
	s.True(ok)
}

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}
