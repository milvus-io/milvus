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
	rand2 "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	s.scheduler = NewScheduler(s.manager, s.syncMgr, nil).(*scheduler)
}

func createInsertData(t *testing.T, schema *schemapb.CollectionSchema, rowCount int) *storage.InsertData {
	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	for _, field := range schema.GetFields() {
		if field.GetAutoID() && field.GetIsPrimaryKey() {
			continue
		}
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			binVecData := make([]byte, 0)
			total := rowCount * int(dim) / 8
			for i := 0; i < total; i++ {
				binVecData = append(binVecData, byte(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: int(dim)}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			floatVecData := make([]float32, 0)
			total := rowCount * int(dim)
			for i := 0; i < total; i++ {
				floatVecData = append(floatVecData, rand.Float32())
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: int(dim)}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			float16VecData := make([]byte, total)
			_, err = rand2.Read(float16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: int(dim)}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, strconv.Itoa(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
		case schemapb.DataType_Array:
			arrayData := make([]*schemapb.ScalarField, 0)
			for i := 0; i < rowCount; i++ {
				arrayData = append(arrayData, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(i), int32(i + 1), int32(i + 2)},
						},
					},
				})
			}
			insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
		default:
			panic(fmt.Sprintf("unexpected data type: %s", field.GetDataType().String()))
		}
	}
	return insertData
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
	}
	preimportTask := NewPreImportTask(preimportReq)
	s.manager.Add(preimportTask)

	slots := s.scheduler.Slots()
	s.Equal(paramtable.Get().DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt64()-1, slots)
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
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader}, nil)
	s.scheduler.cm = cm

	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
	}
	preimportTask := NewPreImportTask(preimportReq)
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
	type mockReader struct {
		io.Reader
		io.Closer
		io.ReaderAt
		io.Seeker
	}
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(1024, nil)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader}, nil)
	s.scheduler.cm = cm

	preimportReq := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{4},
		Vchannels:    []string{"ch-0"},
		Schema:       s.schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
	}
	preimportTask := NewPreImportTask(preimportReq)
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
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader}, nil)
	s.scheduler.cm = cm

	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task) *conc.Future[struct{}] {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, nil
		})
		return future
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
		AutoIDRange: &datapb.AutoIDRange{
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
	importTask := NewImportTask(importReq)
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
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader}, nil)
	s.scheduler.cm = cm

	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task) *conc.Future[struct{}] {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, errors.New("mock err")
		})
		return future
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
		AutoIDRange: &datapb.AutoIDRange{
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
	importTask := NewImportTask(importReq)
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
	data := createInsertData(s.T(), s.schema, s.numRows)
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
	preimportTask := NewPreImportTask(preimportReq)
	s.manager.Add(preimportTask)
	err := s.scheduler.readFileStat(s.reader, preimportTask, 0)
	s.NoError(err)
}

func (s *SchedulerSuite) TestScheduler_ImportFile() {
	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, task syncmgr.Task) *conc.Future[struct{}] {
		future := conc.Go(func() (struct{}, error) {
			return struct{}{}, nil
		})
		return future
	})
	var once sync.Once
	data := createInsertData(s.T(), s.schema, s.numRows)
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
		AutoIDRange: &datapb.AutoIDRange{
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
	importTask := NewImportTask(importReq)
	s.manager.Add(importTask)
	err := s.scheduler.importFile(s.reader, importTask)
	s.NoError(err)
}

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}
