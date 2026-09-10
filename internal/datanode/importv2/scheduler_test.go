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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotL0BothImportPhases(t *testing.T) {
	type phaseRecordReader struct {
		storage.RecordReader
		record storage.Record
		read   bool
	}
	paramtable.Init()
	pool := conc.NewPool[any](1)
	defer pool.Release()
	poolPatch := mockey.Mock(GetExecPool).Return(pool).Build()
	defer poolPatch.UnPatch()
	ma := NewMemoryAllocator(1024 * 1024 * 1024).(*memoryAllocator)
	memoryPatch := mockey.Mock(GetMemoryAllocator).Return(ma).Build()
	defer memoryPatch.UnPatch()
	for _, mode := range []string{"reinsert", "source_commit", "zero_rows", "missing_between_phases", "preimport_admission", "import_admission", "cancel_import"} {
		t.Run(mode, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
			cm := storage.NewLocalChunkManager()
			deltaPath := filepath.Join(t.TempDir(), "l0.delta")
			ts := uint64(200)
			if mode == "zero_rows" {
				ts = 400
			}
			deltaRecord, _, _, err := storage.BuildDeleteRecord([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)}, []uint64{ts, ts})
			require.NoError(t, err)
			writer, err := storage.NewDeltalogWriter(context.Background(), 1, 10, 30, 1, schemapb.DataType_Int64, deltaPath,
				storage.WithVersion(storage.StorageV1), storage.WithUploader(cm.MultiWrite))
			require.NoError(t, err)
			require.NoError(t, writer.Write(deltaRecord))
			deltaRecord.Release()
			require.NoError(t, writer.Close())
			nextPatch := mockey.Mock((*phaseRecordReader).Next).To(func(r *phaseRecordReader) (storage.Record, error) {
				if r.read {
					return nil, io.EOF
				}
				r.read = true
				return r.record, nil
			}).Build()
			defer nextPatch.UnPatch()
			closePatch := mockey.Mock((*phaseRecordReader).Close).To(func(r *phaseRecordReader) error {
				if r.record != nil {
					r.record.Release()
					r.record = nil
				}
				return nil
			}).Build()
			defer closePatch.UnPatch()
			manifestPatch := mockey.Mock(storage.NewManifestRecordReader).To(func(_ context.Context, _ string, _ *schemapb.CollectionSchema, _ ...storage.RwOption) (storage.RecordReader, error) {
				var values []*storage.Value
				for i, row := range [][2]int64{{1, 100}, {1, 300}, {2, 100}} {
					values = append(values, &storage.Value{Value: map[int64]any{0: int64(i + 1), 1: row[1], 100: row[0]}})
				}
				record, err := storage.ValueSerializer(values, typeutil.AppendSystemFields(schema))
				return &phaseRecordReader{record: record}, err
			}).Build()
			defer manifestPatch.UnPatch()
			fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
			defer fragmentsPatch.UnPatch()
			lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
			defer lobPatch.UnPatch()
			deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
			defer deltaPatch.UnPatch()
			var imported []int64
			syncPatch := mockey.Mock((*ImportTask).sync).To(func(_ *ImportTask, data HashedData) ([]*conc.Future[struct{}], []syncmgr.Task, error) {
				for _, partitions := range data {
					for _, rows := range partitions {
						for i := 0; i < rows.GetRowNum(); i++ {
							imported = append(imported, rows.Data[100].GetRow(i).(int64))
						}
					}
				}
				return nil, nil, nil
			}).Build()
			defer syncPatch.UnPatch()
			file := &internalpb.ImportFile{Id: 1, SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 1, ManifestPath: packed.MarshalManifestPath("snapshot/data/20", 7), LegacyL0Deltalogs: []string{deltaPath}}}
			want := []int64{1}
			if mode == "source_commit" {
				file.SnapshotSource.SourceCommitTimestamp = 300
				want = []int64{1, 1, 2}
			}
			if mode == "zero_rows" {
				want = nil
			}
			options := importutilv2.Options{{Key: importutilv2.BackupFlag, Value: "true"}, {Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot}}
			manager := NewTaskManager()
			pre := NewPreImportTask(&datapb.PreImportRequest{TaskID: 1, Schema: schema, Options: options, ImportFiles: []*internalpb.ImportFile{file},
				PartitionIDs: []int64{10}, Vchannels: []string{"target"}, StorageConfig: &indexpb.StorageConfig{}}, manager, cm)
			defer pre.Cancel()
			manager.Add(pre)
			if mode == "preimport_admission" {
				ma.systemTotalMemory = 0
				defer func() { ma.systemTotalMemory = 1024 * 1024 * 1024 }()
				require.ErrorIs(t, conc.AwaitAll(pre.Execute()...), merr.ErrServiceResourceInsufficient)
				require.Equal(t, datapb.ImportTaskStateV2_Failed, manager.Get(1).GetState())
				require.Zero(t, ma.usedMemory)
				return
			}
			require.NoError(t, conc.AwaitAll(pre.Execute()...))
			require.EqualValues(t, len(want), manager.Get(1).(*PreImportTask).GetFileStats()[0].GetTotalRows())
			require.Zero(t, ma.usedMemory)
			if mode == "missing_between_phases" {
				require.NoError(t, cm.Remove(context.Background(), deltaPath))
			}
			imp := NewImportTask(&datapb.ImportRequest{TaskID: 2, Schema: schema, Options: options, Files: []*internalpb.ImportFile{file},
				Ts: 9999, IDRange: &datapb.IDRange{Begin: 100, End: 1000}, PartitionIDs: []int64{10}, Vchannels: []string{"target"},
				StorageConfig: &indexpb.StorageConfig{}}, manager, nil, cm)
			defer imp.Cancel()
			manager.Add(imp)
			if mode == "import_admission" {
				ma.systemTotalMemory = 0
				defer func() { ma.systemTotalMemory = 1024 * 1024 * 1024 }()
			}
			if mode == "cancel_import" {
				imp.Cancel()
			}
			err = conc.AwaitAll(imp.Execute()...)
			if mode == "missing_between_phases" || mode == "import_admission" || mode == "cancel_import" {
				require.Error(t, err)
				require.Equal(t, datapb.ImportTaskStateV2_Failed, manager.Get(2).GetState())
			} else {
				require.NoError(t, err)
				require.Equal(t, want, imported, "target commit timestamp must not participate in source delete ordering")
			}
			require.Zero(t, ma.usedMemory)
		})
	}
}

func TestSnapshotSourceTimestampErrorFailsBothImportPhases(t *testing.T) {
	type snapshotErrorReader struct{ importutilv2.Reader }
	paramtable.Init()
	pool := conc.NewPool[any](1)
	defer pool.Release()
	poolPatch := mockey.Mock(GetExecPool).Return(pool).Build()
	defer poolPatch.UnPatch()
	for _, phase := range []string{"preimport", "import"} {
		t.Run(phase, func(t *testing.T) {
			// Reader tests exercise the real timestamp check. Here inject its
			// typed read error to verify the real task does not publish success
			// or turn a corrupt source into an empty successful import.
			readErr := merr.WrapErrDataIntegrityMsg("raw row timestamp 400 above segment commit timestamp 300")
			file := &internalpb.ImportFile{Id: 1, SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 1, ManifestPath: "snapshot/segment/10", SourceCommitTimestamp: 300,
			}}
			fakeReader := &snapshotErrorReader{}
			readPatch := mockey.Mock((*snapshotErrorReader).Read).Return(nil, readErr).Build()
			defer readPatch.UnPatch()
			sizePatch := mockey.Mock((*snapshotErrorReader).Size).Return(int64(1), nil).Build()
			defer sizePatch.UnPatch()
			closed := false
			closePatch := mockey.Mock((*snapshotErrorReader).Close).To(func(_ *snapshotErrorReader) {
				closed = true
			}).Build()
			defer closePatch.UnPatch()
			factoryPatch := mockey.Mock(importutilv2.NewReader).To(func(_ context.Context, _ storage.ChunkManager,
				_ *schemapb.CollectionSchema, gotFile *internalpb.ImportFile, _ importutilv2.Options, _ int,
				_ *indexpb.StorageConfig, _ int64,
			) (importutilv2.Reader, error) {
				if gotFile != file {
					return nil, merr.WrapErrServiceInternalMsg("task did not retain its immutable snapshot descriptor")
				}
				return fakeReader, nil
			}).Build()
			defer factoryPatch.UnPatch()

			manager := NewTaskManager()
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			options := importutilv2.Options{{Key: importutilv2.BackupFlag, Value: "true"},
				{Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot}}
			var task Task
			if phase == "preimport" {
				task = NewPreImportTask(&datapb.PreImportRequest{TaskID: 1, Schema: schema, Options: options,
					ImportFiles: []*internalpb.ImportFile{file}, PartitionIDs: []int64{1}, Vchannels: []string{"v1"}}, manager, nil)
			} else {
				task = NewImportTask(&datapb.ImportRequest{TaskID: 1, Schema: schema, Options: options,
					Files: []*internalpb.ImportFile{file}, PartitionIDs: []int64{1}, Vchannels: []string{"v1"}}, manager, nil, nil)
			}
			defer task.Cancel()
			manager.Add(task)
			futures := task.Execute()
			require.Len(t, futures, 1)
			select {
			case <-futures[0].Inner():
			case <-time.After(10 * time.Second):
				t.Fatal("import phase did not finish after a terminal reader error")
			}
			_, err := futures[0].Await()
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Equal(t, datapb.ImportTaskStateV2_Failed, manager.Get(1).GetState())
			require.Contains(t, manager.Get(1).GetReason(), "raw row timestamp")
			require.True(t, closed, "task must close its reader on failure")
		})
	}
}

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

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}
