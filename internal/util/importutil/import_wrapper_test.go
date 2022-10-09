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

package importutil

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const (
	TempFilesPath = "/tmp/milvus_test/import/"
)

type MockChunkManager struct {
	size       int64
	sizeErr    error
	readBuf    map[string][]byte
	readErr    error
	listResult map[string][]string
	listErr    error
}

func (mc *MockChunkManager) RootPath() string {
	return TempFilesPath
}

func (mc *MockChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	return "", nil
}

func (mc *MockChunkManager) Reader(ctx context.Context, filePath string) (storage.FileReader, error) {
	return nil, nil
}

func (mc *MockChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	return nil
}

func (mc *MockChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	return nil
}

func (mc *MockChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	return true, nil
}

func (mc *MockChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	if mc.readErr != nil {
		return nil, mc.readErr
	}

	val, ok := mc.readBuf[filePath]
	if !ok {
		return nil, errors.New("mock chunk manager: file path not found: " + filePath)
	}

	return val, nil
}

func (mc *MockChunkManager) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	if mc.listErr != nil {
		return nil, nil, mc.listErr
	}

	result, ok := mc.listResult[prefix]
	if ok {
		return result, nil, nil
	}

	return nil, nil, nil
}

func (mc *MockChunkManager) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	return nil, nil, nil
}

func (mc *MockChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, nil
}

func (mc *MockChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	if mc.sizeErr != nil {
		return 0, mc.sizeErr
	}

	return mc.size, nil
}

func (mc *MockChunkManager) Remove(ctx context.Context, filePath string) error {
	return nil
}

func (mc *MockChunkManager) MultiRemove(ctx context.Context, filePaths []string) error {
	return nil
}

func (mc *MockChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	return nil
}

type rowCounterTest struct {
	rowCount int
	callTime int
}

func createMockCallbackFunctions(t *testing.T, rowCounter *rowCounterTest) (AssignSegmentFunc, CreateBinlogsFunc, SaveSegmentFunc) {
	createBinlogFunc := func(fields map[storage.FieldID]storage.FieldData, segmentID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCounter.rowCount += count
		rowCounter.callTime++
		return nil, nil, nil
	}

	assignSegmentFunc := func(shardID int) (int64, string, error) {
		return 100, "ch", nil
	}

	saveSegmentFunc := func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog, segmentID int64, targetChName string, rowCount int64) error {
		return nil
	}

	return assignSegmentFunc, createBinlogFunc, saveSegmentFunc
}

func Test_NewImportWrapper(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, nil, 2, 1, nil, cm, nil, nil)
	assert.Nil(t, wrapper)

	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	schema.Fields = append(schema.Fields, sampleSchema().Fields...)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      106,
		Name:         common.RowIDFieldName,
		IsPrimaryKey: true,
		AutoID:       false,
		Description:  "int64",
		DataType:     schemapb.DataType_Int64,
	})
	wrapper = NewImportWrapper(ctx, schema, 2, 1, nil, cm, nil, nil)
	assert.NotNil(t, wrapper)

	assignSegFunc := func(shardID int) (int64, string, error) {
		return 0, "", nil
	}
	createBinFunc := func(fields map[storage.FieldID]storage.FieldData, segmentID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		return nil, nil, nil
	}
	saveBinFunc := func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog, segmentID int64, targetChName string, rowCount int64) error {
		return nil
	}

	err = wrapper.SetCallbackFunctions(assignSegFunc, createBinFunc, saveBinFunc)
	assert.Nil(t, err)
	err = wrapper.SetCallbackFunctions(assignSegFunc, createBinFunc, nil)
	assert.NotNil(t, err)
	err = wrapper.SetCallbackFunctions(assignSegFunc, nil, nil)
	assert.NotNil(t, err)
	err = wrapper.SetCallbackFunctions(nil, nil, nil)
	assert.NotNil(t, err)

	err = wrapper.Cancel()
	assert.Nil(t, err)
}

func Test_ImportWrapperRowBased(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t, nil)

	content := []byte(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_bool": false, "field_int8": 11, "field_int16": 102, "field_int32": 1002, "field_int64": 10002, "field_float": 3.15, "field_double": 2.56, "field_string": "hello world", "field_binary_vector": [253, 0], "field_float_vector": [2.1, 2.2, 2.3, 2.4]},
			{"field_bool": true, "field_int8": 12, "field_int16": 103, "field_int32": 1003, "field_int64": 10003, "field_float": 3.16, "field_double": 3.56, "field_string": "hello world", "field_binary_vector": [252, 0], "field_float_vector": [3.1, 3.2, 3.3, 3.4]},
			{"field_bool": false, "field_int8": 13, "field_int16": 104, "field_int32": 1004, "field_int64": 10004, "field_float": 3.17, "field_double": 4.56, "field_string": "hello world", "field_binary_vector": [251, 0], "field_float_vector": [4.1, 4.2, 4.3, 4.4]},
			{"field_bool": true, "field_int8": 14, "field_int16": 105, "field_int32": 1005, "field_int64": 10005, "field_float": 3.18, "field_double": 5.56, "field_string": "hello world", "field_binary_vector": [250, 0], "field_float_vector": [5.1, 5.2, 5.3, 5.4]}
		]
	}`)

	filePath := TempFilesPath + "rows_1.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
	assert.Nil(t, err)
	assert.Equal(t, 0, rowCounter.rowCount)

	err = wrapper.Import(files, DefaultImportOptions())
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// parse error
	content = []byte(`{
		"rows":[
			{"field_bool": true, "field_int8": false, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
		]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	importResult.State = commonpb.ImportState_ImportStarted
	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
	assert.NotNil(t, err)
	assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
	assert.NotNil(t, err)
}

func createSampleNumpyFiles(t *testing.T, cm storage.ChunkManager) []string {
	ctx := context.Background()
	files := make([]string, 0)

	filePath := "field_bool.npy"
	content, err := CreateNumpyData([]bool{true, false, true, true, true})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_int8.npy"
	content, err = CreateNumpyData([]int8{10, 11, 12, 13, 14})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_int16.npy"
	content, err = CreateNumpyData([]int16{100, 101, 102, 103, 104})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_int32.npy"
	content, err = CreateNumpyData([]int32{1000, 1001, 1002, 1003, 1004})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_int64.npy"
	content, err = CreateNumpyData([]int64{10000, 10001, 10002, 10003, 10004})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_float.npy"
	content, err = CreateNumpyData([]float32{3.14, 3.15, 3.16, 3.17, 3.18})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_double.npy"
	content, err = CreateNumpyData([]float64{5.1, 5.2, 5.3, 5.4, 5.5})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_string.npy"
	content, err = CreateNumpyData([]string{"a", "bb", "ccc", "dd", "e"})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_binary_vector.npy"
	content, err = CreateNumpyData([][2]uint8{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = "field_float_vector.npy"
	content, err = CreateNumpyData([][4]float32{{1, 2, 3, 4}, {3, 4, 5, 6}, {5, 6, 7, 8}, {7, 8, 9, 10}, {9, 10, 11, 12}})
	assert.Nil(t, err)
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	return files
}

func Test_ImportWrapperColumnBased_numpy(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t, nil)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	schema := sampleSchema()
	wrapper := NewImportWrapper(ctx, schema, 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files := createSampleNumpyFiles(t, cm)
	err = wrapper.Import(files, DefaultImportOptions())
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// parse error
	content := []byte(`{
		"field_bool": [true, false, true, true, true]
	}`)

	filePath := "rows_2.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	importResult.State = commonpb.ImportState_ImportStarted
	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, DefaultImportOptions())
	assert.NotNil(t, err)
	assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, DefaultImportOptions())
	assert.NotNil(t, err)
}

func perfSchema(dim int) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "ID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "Vector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: strconv.Itoa(dim)},
				},
			},
		},
	}

	return schema
}

func Test_ImportWrapperRowBased_perf(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t, nil)

	tr := timerecord.NewTimeRecorder("row-based parse performance")

	type Entity struct {
		ID     int64
		Vector []float32
	}

	type Entities struct {
		Rows []*Entity
	}

	// change these parameters to test different cases
	dim := 128
	rowCount := 10000
	shardNum := 2
	segmentSize := 512 // unit: MB

	// generate rows data
	entities := &Entities{
		Rows: make([]*Entity, 0),
	}

	for i := 0; i < rowCount; i++ {
		entity := &Entity{
			ID:     int64(i),
			Vector: make([]float32, 0, dim),
		}
		for k := 0; k < dim; k++ {
			entity.Vector = append(entity.Vector, float32(i)+3.1415926)
		}
		entities.Rows = append(entities.Rows, entity)
	}
	tr.Record("generate " + strconv.Itoa(rowCount) + " rows")

	// generate a json file
	filePath := "row_perf.json"
	func() {
		var b bytes.Buffer
		bw := bufio.NewWriter(&b)

		encoder := json.NewEncoder(bw)
		err = encoder.Encode(entities)
		assert.Nil(t, err)
		err = bw.Flush()
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, b.Bytes())
		assert.NoError(t, err)
	}()
	tr.Record("generate large json file " + filePath)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	schema := perfSchema(dim)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, DefaultImportOptions())
	assert.Nil(t, err)
	assert.Equal(t, rowCount, rowCounter.rowCount)

	tr.Record("parse large json file " + filePath)
}

func Test_ImportWrapperValidateColumnBasedFiles(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	shardNum := 2
	segmentSize := 512 // unit: MB

	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "ID",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "Age",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      103,
				Name:         "Vector",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "10"},
				},
			},
		},
	}

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)

	// file for PK is redundant
	files := []string{"ID.npy", "Age.npy", "Vector.npy"}
	err := wrapper.validateColumnBasedFiles(files, schema)
	assert.NotNil(t, err)

	// file for PK is not redundant
	schema.Fields[0].AutoID = false
	err = wrapper.validateColumnBasedFiles(files, schema)
	assert.Nil(t, err)

	// file missed
	files = []string{"Age.npy", "Vector.npy"}
	err = wrapper.validateColumnBasedFiles(files, schema)
	assert.NotNil(t, err)

	files = []string{"ID.npy", "Vector.npy"}
	err = wrapper.validateColumnBasedFiles(files, schema)
	assert.NotNil(t, err)

	// redundant file
	files = []string{"ID.npy", "Age.npy", "Vector.npy", "dummy.npy"}
	err = wrapper.validateColumnBasedFiles(files, schema)
	assert.NotNil(t, err)

	// correct input
	files = []string{"ID.npy", "Age.npy", "Vector.npy"}
	err = wrapper.validateColumnBasedFiles(files, schema)
	assert.Nil(t, err)
}

func Test_ImportWrapperFileValidation(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := &schemapb.CollectionSchema{
		Name:   "schema",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "bol",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Bool,
			},
		},
	}
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)

	// unsupported file type
	files := []string{"uid.txt"}
	rowBased, err := wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// file missed
	files = []string{"uid.npy"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// redundant file
	files = []string{"uid.npy", "b/bol.npy", "c/no.npy"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// duplicate files
	files = []string{"a/1.json", "b/1.json"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.True(t, rowBased)

	files = []string{"a/uid.npy", "uid.npy", "b/bol.npy"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// unsupported file for row-based
	files = []string{"a/uid.json", "b/bol.npy"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.True(t, rowBased)

	// unsupported file for column-based
	files = []string{"a/uid.npy", "b/bol.json"}
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// valid cases
	files = []string{"a/1.json", "b/2.json"}
	rowBased, err = wrapper.fileValidation(files)
	assert.Nil(t, err)
	assert.True(t, rowBased)

	files = []string{"a/uid.npy", "b/bol.npy"}
	rowBased, err = wrapper.fileValidation(files)
	assert.Nil(t, err)
	assert.False(t, rowBased)

	// empty file
	cm.size = 0
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// file size exceed MaxFileSize limit
	cm.size = MaxFileSize + 1
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// total files size exceed MaxTotalSizeInMemory limit
	cm.size = MaxFileSize - 1
	files = append(files, "3.npy")
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)

	// failed to get file size
	cm.sizeErr = errors.New("error")
	rowBased, err = wrapper.fileValidation(files)
	assert.NotNil(t, err)
	assert.False(t, rowBased)
}

func Test_ImportWrapperReportFailRowBased(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t, nil)

	content := []byte(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_bool": false, "field_int8": 11, "field_int16": 102, "field_int32": 1002, "field_int64": 10002, "field_float": 3.15, "field_double": 2.56, "field_string": "hello world", "field_binary_vector": [253, 0], "field_float_vector": [2.1, 2.2, 2.3, 2.4]},
			{"field_bool": true, "field_int8": 12, "field_int16": 103, "field_int32": 1003, "field_int64": 10003, "field_float": 3.16, "field_double": 3.56, "field_string": "hello world", "field_binary_vector": [252, 0], "field_float_vector": [3.1, 3.2, 3.3, 3.4]},
			{"field_bool": false, "field_int8": 13, "field_int16": 104, "field_int32": 1004, "field_int64": 10004, "field_float": 3.17, "field_double": 4.56, "field_string": "hello world", "field_binary_vector": [251, 0], "field_float_vector": [4.1, 4.2, 4.3, 4.4]},
			{"field_bool": true, "field_int8": 14, "field_int16": 105, "field_int32": 1005, "field_int64": 10005, "field_float": 3.18, "field_double": 5.56, "field_string": "hello world", "field_binary_vector": [250, 0], "field_float_vector": [5.1, 5.2, 5.3, 5.4]}
		]
	}`)

	filePath := "rows_1.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files := make([]string, 0)
	files = append(files, filePath)

	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}
	err = wrapper.Import(files, DefaultImportOptions())
	assert.NotNil(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperReportFailColumnBased_numpy(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t, nil)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	schema := sampleSchema()
	wrapper := NewImportWrapper(ctx, schema, 2, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}

	files := createSampleNumpyFiles(t, cm)

	err = wrapper.Import(files, DefaultImportOptions())
	assert.NotNil(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperIsBinlogImport(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)

	// empty paths
	paths := []string{}
	b := wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// paths count should be 2
	paths = []string{
		"path1",
		"path2",
		"path3",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// not path
	paths = []string{
		"path1.txt",
		"path2.jpg",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// success
	paths = []string{
		"/tmp",
		"/tmp",
	}
	b = wrapper.isBinlogImport(paths)
	assert.True(t, b)
}

func Test_ImportWrapperDoBinlogImport(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil)
	paths := []string{
		"/tmp",
		"/tmp",
	}
	wrapper.chunkManager = nil

	// failed to create new BinlogParser
	err := wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.NotNil(t, err)

	cm.listErr = errors.New("error")
	wrapper.chunkManager = cm

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	// failed to call parser.Parse()
	err = wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.NotNil(t, err)

	// Import() failed
	err = wrapper.Import(paths, DefaultImportOptions())
	assert.NotNil(t, err)

	cm.listErr = nil
	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	wrapper.importResult = &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}

	// succeed
	err = wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.Nil(t, err)
}

func Test_ImportWrapperSplitFieldsData(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{}

	idAllocator := newIDAllocator(ctx, t, nil)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}

	schema := &schemapb.CollectionSchema{
		Name:   "schema",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "flag",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Bool,
			},
		},
	}

	wrapper := NewImportWrapper(ctx, schema, 2, 1024*1024, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	// nil input
	err := wrapper.splitFieldsData(nil, 0)
	assert.NotNil(t, err)

	// split 100 rows to 4 blocks
	rowCount := 100
	input := initSegmentData(schema)
	for j := 0; j < rowCount; j++ {
		pkField := input[101].(*storage.Int64FieldData)
		pkField.Data = append(pkField.Data, int64(j))

		flagField := input[102].(*storage.BoolFieldData)
		flagField.Data = append(flagField.Data, true)
	}

	err = wrapper.splitFieldsData(input, 512)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(importResult.AutoIds))
	assert.Equal(t, 4, rowCounter.callTime)
	assert.Equal(t, rowCount, rowCounter.rowCount)

	// row count of fields are unequal
	schema.Fields[0].AutoID = false
	input = initSegmentData(schema)
	for j := 0; j < rowCount; j++ {
		pkField := input[101].(*storage.Int64FieldData)
		pkField.Data = append(pkField.Data, int64(j))
		if j%2 == 0 {
			continue
		}
		flagField := input[102].(*storage.BoolFieldData)
		flagField.Data = append(flagField.Data, true)
	}
	err = wrapper.splitFieldsData(input, 512)
	assert.NotNil(t, err)
}
