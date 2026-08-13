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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/resource"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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
	guardMock *mockey.Mocker
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

	// Admission goes through a double rather than the process-wide guard: that
	// one freezes on the host's live memory reading, which would make these
	// tests pass or hang depending on what else the machine is doing.
	s.guardMock = mockey.Mock(resource.GetGuard).Return(resource.NewRecordingGuard()).Build()
}

func (s *SchedulerSuite) TearDownTest() {
	s.scheduler.Close()
	s.guardMock.UnPatch()
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

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

// useRecordingGuard routes the scheduler's admission calls at a double for the
// duration of the test. The process-wide guard is deliberately kept out of the
// unit tests: it samples the machine's real memory in the background, so a test
// that reserved from it would pass or hang depending on the host's mood.
func useRecordingGuard(t *testing.T) *resource.RecordingGuard {
	g := resource.NewRecordingGuard()
	mk := mockey.Mock(resource.GetGuard).Return(g).Build()
	t.Cleanup(func() { mk.UnPatch() })
	return g
}

func TestImportSchedulerAdmission(t *testing.T) {
	paramtable.Init()

	const taskID = int64(5001)
	req := taskresource.Requirement{CPU: 2, Memory: 3 << 30}

	newTask := func(t *testing.T) *MockTask {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(taskID).Maybe()
		task.EXPECT().GetJobID().Return(int64(1)).Maybe()
		task.EXPECT().GetCollectionID().Return(int64(1)).Maybe()
		task.EXPECT().GetType().Return(ImportTaskType).Maybe()
		task.EXPECT().GetState().Return(datapb.ImportTaskStateV2_Pending).Maybe()
		task.EXPECT().GetResourceRequirement().Return(req).Maybe()
		return task
	}

	newScheduler := func(t *testing.T, task Task) *scheduler {
		manager := NewMockTaskManager(t)
		manager.EXPECT().GetBy(mock.Anything).Return([]Task{task}).Maybe()
		manager.EXPECT().Update(taskID, mock.Anything).Return().Maybe()
		return NewScheduler(manager).(*scheduler)
	}

	t.Run("reserves before executing and releases when the task ends", func(t *testing.T) {
		g := useRecordingGuard(t)
		task := newTask(t)
		task.EXPECT().Execute().RunAndReturn(func() []*conc.Future[any] {
			g.Note("execute")
			return nil
		}).Once()

		newScheduler(t, task).scheduleTasks()

		assert.Equal(t, []string{"acquire", "execute", "release"}, g.Events())
		acquires := g.Acquires()
		require.Len(t, acquires, 1)
		assert.Equal(t, taskID, acquires[0].TaskID)
		assert.Equal(t, taskcommon.Import, acquires[0].Type)
		assert.Equal(t, req, acquires[0].Req)
	})

	t.Run("holds the reservation until the task's work is done", func(t *testing.T) {
		g := useRecordingGuard(t)
		gate := make(chan struct{})
		task := newTask(t)
		task.EXPECT().Execute().RunAndReturn(func() []*conc.Future[any] {
			return []*conc.Future[any]{conc.Go(func() (any, error) {
				<-gate
				return nil, nil
			})}
		}).Once()

		sched := newScheduler(t, task)
		done := make(chan struct{})
		go func() {
			defer close(done)
			sched.scheduleTasks()
		}()

		// Execute only dispatches the work; the reading happens afterwards in
		// the pool. Releasing when Execute returns would hand the budget away
		// while the task is still holding memory.
		require.Eventually(t, func() bool { return len(g.Acquires()) == 1 }, time.Second, 10*time.Millisecond)
		assert.Empty(t, g.Releases())

		close(gate)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.Fail(t, "scheduler never finished the task")
		}
		assert.Equal(t, []int64{taskID}, g.Releases())
	})

	t.Run("does not execute or release when the wait is cut short", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.FailAcquire(context.Canceled)
		task := newTask(t)
		// Execute is not expected: running it would fail the test outright.

		manager := NewMockTaskManager(t)
		manager.EXPECT().GetBy(mock.Anything).Return([]Task{task}).Maybe()
		// Update is not expected either: the task must stay Pending so the next
		// tick picks it up again.
		sched := NewScheduler(manager).(*scheduler)
		sched.scheduleTasks()

		assert.Empty(t, g.Releases(), "a task that never acquired must not release")
	})

	t.Run("parks in Acquire instead of polling TryAcquire", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.Block()
		task := newTask(t)
		task.EXPECT().Execute().RunAndReturn(func() []*conc.Future[any] {
			g.Note("execute")
			return nil
		}).Once()

		sched := newScheduler(t, task)
		done := make(chan struct{})
		go func() {
			defer close(done)
			sched.scheduleTasks()
		}()

		time.Sleep(100 * time.Millisecond)
		assert.NotContains(t, g.Events(), "execute", "import started before its reservation was granted")
		assert.Empty(t, g.TryAcquires(), "waiting must happen in Acquire, where the guard can hold the queue head")

		g.Unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.Fail(t, "import never ran after the guard admitted it")
		}
		assert.Equal(t, []string{"acquire", "execute", "release"}, g.Events())
	})

	t.Run("reports each family to the ledger under its own type", func(t *testing.T) {
		cases := []struct {
			taskType TaskType
			expected taskcommon.Type
		}{
			{PreImportTaskType, taskcommon.PreImport},
			{ImportTaskType, taskcommon.Import},
			{L0PreImportTaskType, taskcommon.PreImport},
			{L0ImportTaskType, taskcommon.Import},
			{CopySegmentTaskType, taskcommon.CopySegment},
			// A kind this package does not know about must not be silently
			// filed under an existing family: the ledger's logs and metrics
			// would then attribute it to work it is not.
			{TaskType(99), taskcommon.TypeNone},
		}
		for _, c := range cases {
			assert.Equal(t, c.expected, ledgerTaskType(c.taskType), c.taskType.String())
		}
	})
}

func TestImportTaskResourceRequirements(t *testing.T) {
	paramtable.Init()

	t.Run("preimport charges a base buffer per file in flight", func(t *testing.T) {
		task := &PreImportTask{
			PreImportTask: &datapb.PreImportTask{
				FileStats: []*datapb.ImportFileStats{{}, {}, {}},
			},
		}
		assert.Equal(t, taskresource.EstimateImport(taskresource.ImportInput{
			IsPreImport: true,
			FileNum:     3,
		}), task.GetResourceRequirement())
	})

	t.Run("import charges the vchannel and partition fan-out", func(t *testing.T) {
		task := &ImportTask{
			ImportTaskV2: &datapb.ImportTaskV2{
				FileStats: []*datapb.ImportFileStats{{TotalMemorySize: 1 << 30}, {TotalMemorySize: 2 << 30}},
			},
			req: &datapb.ImportRequest{
				Vchannels:    []string{"ch-0", "ch-1"},
				PartitionIDs: []int64{1, 2, 3},
			},
		}
		expected := taskresource.EstimateImport(taskresource.ImportInput{
			FileNum:           2,
			VChannelNum:       2,
			PartitionNum:      3,
			MaxFileMemorySize: 2 << 30,
		})
		assert.Equal(t, expected, task.GetResourceRequirement())
		// The fan-out has to reach the estimate: a task charged as if it had one
		// vchannel and one partition would be six times too cheap here.
		assert.Greater(t, expected.Memory, taskresource.EstimateImport(taskresource.ImportInput{
			FileNum:           2,
			VChannelNum:       1,
			PartitionNum:      1,
			MaxFileMemorySize: 2 << 30,
		}).Memory)
	})

	t.Run("l0 preimport charges a delete buffer per file", func(t *testing.T) {
		task := &L0PreImportTask{
			PreImportTask: &datapb.PreImportTask{
				FileStats: []*datapb.ImportFileStats{{}, {}},
			},
		}
		expected := taskresource.EstimateImport(taskresource.ImportInput{
			IsL0:        true,
			IsPreImport: true,
			FileNum:     2,
		})
		assert.Equal(t, expected, task.GetResourceRequirement())
		assert.Greater(t, expected.Memory, int64(0))
	})

	t.Run("l0 import charges a delete buffer per file", func(t *testing.T) {
		task := &L0ImportTask{
			req: &datapb.ImportRequest{
				Files: []*internalpb.ImportFile{{}, {}, {}},
			},
		}
		expected := taskresource.EstimateImport(taskresource.ImportInput{
			IsL0:    true,
			FileNum: 3,
		})
		assert.Equal(t, expected, task.GetResourceRequirement())
		assert.Greater(t, expected.Memory, int64(0))
	})

	t.Run("copy segment charges no segment bytes", func(t *testing.T) {
		task := &CopySegmentTask{
			segmentResults: map[int64]*datapb.CopySegmentResult{1: {}, 2: {}},
		}
		assert.Equal(t, taskresource.EstimateCopySegment(2), task.GetResourceRequirement())
	})
}
