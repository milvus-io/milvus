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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
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

func Test_NewImportWrapper(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, nil, 2, 1, nil, cm, nil, nil, nil)
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
	wrapper = NewImportWrapper(ctx, schema, 2, 1, nil, cm, nil, nil, nil)
	assert.NotNil(t, wrapper)

	err = wrapper.Cancel()
	assert.Nil(t, err)
}

func Test_ImportWrapperRowBased(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t)

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

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, true, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)
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
	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, true, false)
	assert.NotNil(t, err)
	assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, true, false)
	assert.NotNil(t, err)
}

func Test_ImportWrapperColumnBased_json(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	content := []byte(`{
		"field_bool": [true, false, true, true, true],
		"field_int8": [10, 11, 12, 13, 14],
		"field_int16": [100, 101, 102, 103, 104],
		"field_int32": [1000, 1001, 1002, 1003, 1004],
		"field_int64": [10000, 10001, 10002, 10003, 10004],
		"field_float": [3.14, 3.15, 3.16, 3.17, 3.18],
		"field_double": [5.1, 5.2, 5.3, 5.4, 5.5],
		"field_string": ["a", "b", "c", "d", "e"],
		"field_binary_vector": [
			[254, 1],
			[253, 2],
			[252, 3],
			[251, 4],
			[250, 5]
		],
		"field_float_vector": [
			[1.1, 1.2, 1.3, 1.4],
			[2.1, 2.2, 2.3, 2.4],
			[3.1, 3.2, 3.3, 3.4],
			[4.1, 4.2, 4.3, 4.4],
			[5.1, 5.2, 5.3, 5.4]
		]
	}`)

	filePath := TempFilesPath + "columns_1.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// parse error
	content = []byte(`{
		"field_bool": [true, false, true, true, true]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	importResult.State = commonpb.ImportState_ImportStarted
	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
	assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
}

func Test_ImportWrapperColumnBased_StringKey(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	content := []byte(`{
		"uid": ["Dm4aWrbNzhmjwCTEnCJ9LDPO2N09sqysxgVfbH9Zmn3nBzmwsmk0eZN6x7wSAoPQ", "RP50U0d2napRjXu94a8oGikWgklvVsXFurp8RR4tHGw7N0gk1b7opm59k3FCpyPb", "oxhFkQitWPPw0Bjmj7UQcn4iwvS0CU7RLAC81uQFFQjWtOdiB329CPyWkfGSeYfE", "sxoEL4Mpk1LdsyXhbNm059UWJ3CvxURLCQczaVI5xtBD4QcVWTDFUW7dBdye6nbn", "g33Rqq2UQSHPRHw5FvuXxf5uGEhIAetxE6UuXXCJj0hafG8WuJr1ueZftsySCqAd"],
		"int_scalar": [9070353, 8505288, 4392660, 7927425, 9288807],
		"float_scalar": [0.9798043638085004, 0.937913432198687, 0.32381232630490264, 0.31074026464844895, 0.4953578200336135],
		"string_scalar": ["ShQ44OX0z8kGpRPhaXmfSsdH7JHq5DsZzu0e2umS1hrWG0uONH2RIIAdOECaaXir", "Ld4b0avxathBdNvCrtm3QsWO1pYktUVR7WgAtrtozIwrA8vpeactNhJ85CFGQnK5", "EmAlB0xdQcxeBtwlZJQnLgKodiuRinynoQtg0eXrjkq24dQohzSm7Bx3zquHd3kO", "fdY2beCvs1wSws0Gb9ySD92xwfEfJpX5DQgsWoISylBAoYOcXpRaqIJoXYS4g269", "6f8Iv1zQAGksj5XxMbbI5evTrYrB8fSFQ58jl0oU7Z4BpA81VsD2tlWqkhfoBNa7"],
		"bool_scalar": [true, false, true, false, false],
		"vectors": [
			[0.5040062902126952, 0.8297619818664708, 0.20248342801564806, 0.12834786423659314],
			[0.528232122836893, 0.6916116750653186, 0.41443762522548705, 0.26624344144792056],
			[0.7978693027281338, 0.12394906726785092, 0.42431962903815285, 0.4098707807351914],
			[0.3716157812069954, 0.006981281113265229, 0.9007003458552365, 0.22492634316191004],
			[0.5921374209648096, 0.04234832587925662, 0.7803878096531548, 0.1964045837884633]
		]
	}`)

	filePath := TempFilesPath + "columns_2.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, strKeySchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperColumnBased_numpy(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	content := []byte(`{
		"field_bool": [true, false, true, true, true],
		"field_int8": [10, 11, 12, 13, 14],
		"field_int16": [100, 101, 102, 103, 104],
		"field_int32": [1000, 1001, 1002, 1003, 1004],
		"field_int64": [10000, 10001, 10002, 10003, 10004],
		"field_float": [3.14, 3.15, 3.16, 3.17, 3.18],
		"field_double": [5.1, 5.2, 5.3, 5.4, 5.5],
		"field_string": ["a", "b", "c", "d", "e"]
	}`)

	files := make([]string, 0)

	filePath := TempFilesPath + "scalar_fields.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_binary_vector.npy"
	bin := [][2]uint8{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}
	content, err = CreateNumpyData(bin)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_float_vector.npy"
	flo := [][4]float32{{1, 2, 3, 4}, {3, 4, 5, 6}, {5, 6, 7, 8}, {7, 8, 9, 10}, {9, 10, 11, 12}}
	content, err = CreateNumpyData(flo)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	schema.Fields[4].AutoID = true
	wrapper := NewImportWrapper(ctx, schema, 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)

	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// parse error
	content = []byte(`{
		"field_bool": [true, false, true, true, true]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	importResult.State = commonpb.ImportState_ImportStarted
	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
	assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, false, false)
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
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

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
	filePath := TempFilesPath + "row_perf.json"
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

	// parse the json file
	parseCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		parseCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, true, false)
	assert.Nil(t, err)
	assert.Equal(t, rowCount, parseCount)

	tr.Record("parse large json file " + filePath)
}

func Test_ImportWrapperColumnBased_perf(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	tr := timerecord.NewTimeRecorder("column-based parse performance")

	type IDCol struct {
		ID []int64
	}

	type VectorCol struct {
		Vector [][]float32
	}

	// change these parameters to test different cases
	dim := 128
	rowCount := 10000
	shardNum := 2
	segmentSize := 512 // unit: MB

	// generate rows data
	ids := &IDCol{
		ID: make([]int64, 0, rowCount),
	}

	vectors := &VectorCol{
		Vector: make([][]float32, 0, rowCount),
	}

	for i := 0; i < rowCount; i++ {
		ids.ID = append(ids.ID, int64(i))

		vector := make([]float32, 0, dim)
		for k := 0; k < dim; k++ {
			vector = append(vector, float32(i)+3.1415926)
		}
		vectors.Vector = append(vectors.Vector, vector)
	}
	tr.Record("generate " + strconv.Itoa(rowCount) + " rows")

	// generate json files
	saveFileFunc := func(filePath string, data interface{}) error {
		var b bytes.Buffer
		bw := bufio.NewWriter(&b)

		encoder := json.NewEncoder(bw)
		err = encoder.Encode(data)
		assert.Nil(t, err)
		err = bw.Flush()
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, b.Bytes())
		assert.NoError(t, err)
		return nil
	}

	filePath1 := TempFilesPath + "ids.json"
	err = saveFileFunc(filePath1, ids)
	assert.Nil(t, err)
	tr.Record("generate large json file " + filePath1)

	filePath2 := TempFilesPath + "vectors.json"
	err = saveFileFunc(filePath2, vectors)
	assert.Nil(t, err)
	tr.Record("generate large json file " + filePath2)

	// parse the json file
	parseCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		parseCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath1)
	files = append(files, filePath2)
	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, rowCount, parseCount)

	tr.Record("parse large json files: " + filePath1 + "," + filePath2)
}

func Test_ImportWrapperFileValidation(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)

	// duplicate files
	files := []string{"1.npy", "1.npy"}
	err := wrapper.fileValidation(files, false)
	assert.NotNil(t, err)
	err = wrapper.fileValidation(files, true)
	assert.NotNil(t, err)

	// unsupported file name
	files[0] = "a/1.npy"
	files[1] = "b/1.npy"
	err = wrapper.fileValidation(files, true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)

	// unsupported file type
	files[0] = "1"
	files[1] = "1"
	err = wrapper.fileValidation(files, true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)

	// valid cases
	files[0] = "1.json"
	files[1] = "2.json"
	err = wrapper.fileValidation(files, true)
	assert.Nil(t, err)

	files[1] = "2.npy"
	err = wrapper.fileValidation(files, false)
	assert.Nil(t, err)

	// empty file
	cm.size = 0
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)
	err = wrapper.fileValidation(files, true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)

	// file size exceed MaxFileSize limit
	cm.size = MaxFileSize + 1
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)
	err = wrapper.fileValidation(files, true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)

	// total files size exceed MaxTotalSizeInMemory limit
	cm.size = MaxFileSize - 1
	files = append(files, "3.npy")
	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)

	// failed to get file size
	cm.sizeErr = errors.New("error")
	err = wrapper.fileValidation(files, false)
	assert.NotNil(t, err)
}

func Test_ImportWrapperReportFailRowBased(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t)

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

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)

	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}
	err = wrapper.Import(files, true, false)
	assert.NotNil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperReportFailColumnBased_json(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	content := []byte(`{
		"field_bool": [true, false, true, true, true],
		"field_int8": [10, 11, 12, 13, 14],
		"field_int16": [100, 101, 102, 103, 104],
		"field_int32": [1000, 1001, 1002, 1003, 1004],
		"field_int64": [10000, 10001, 10002, 10003, 10004],
		"field_float": [3.14, 3.15, 3.16, 3.17, 3.18],
		"field_double": [5.1, 5.2, 5.3, 5.4, 5.5],
		"field_string": ["a", "b", "c", "d", "e"],
		"field_binary_vector": [
			[254, 1],
			[253, 2],
			[252, 3],
			[251, 4],
			[250, 5]
		],
		"field_float_vector": [
			[1.1, 1.2, 1.3, 1.4],
			[2.1, 2.2, 2.3, 2.4],
			[3.1, 3.2, 3.3, 3.4],
			[4.1, 4.2, 4.3, 4.4],
			[5.1, 5.2, 5.3, 5.4]
		]
	}`)

	filePath := TempFilesPath + "columns_1.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)
	files := make([]string, 0)
	files = append(files, filePath)

	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperReportFailColumnBased_numpy(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, "")

	idAllocator := newIDAllocator(ctx, t)

	content := []byte(`{
		"field_bool": [true, false, true, true, true],
		"field_int8": [10, 11, 12, 13, 14],
		"field_int16": [100, 101, 102, 103, 104],
		"field_int32": [1000, 1001, 1002, 1003, 1004],
		"field_int64": [10000, 10001, 10002, 10003, 10004],
		"field_float": [3.14, 3.15, 3.16, 3.17, 3.18],
		"field_double": [5.1, 5.2, 5.3, 5.4, 5.5],
		"field_string": ["a", "b", "c", "d", "e"]
	}`)

	files := make([]string, 0)

	filePath := TempFilesPath + "scalar_fields.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_binary_vector.npy"
	bin := [][2]uint8{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}
	content, err = CreateNumpyData(bin)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_float_vector.npy"
	flo := [][4]float32{{1, 2, 3, 4}, {3, 4, 5, 6}, {5, 6, 7, 8}, {7, 8, 9, 10}, {9, 10, 11, 12}}
	content, err = CreateNumpyData(flo)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	rowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardNum int) error {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCount += count
		return nil
	}

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
	schema.Fields[4].AutoID = true
	wrapper := NewImportWrapper(ctx, schema, 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)

	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperIsBinlogImport(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)

	paths := []string{}
	b := wrapper.isBinlogImport(paths)
	assert.False(t, b)

	paths = []string{
		"path1",
		"path2",
		"path3",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

	paths = []string{
		"path1.txt",
		"path2.jpg",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

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

	idAllocator := newIDAllocator(ctx, t)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	wrapper := NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)
	paths := []string{
		"/tmp",
		"/tmp",
	}
	wrapper.chunkManager = nil

	// failed to create new BinlogParser
	err := wrapper.doBinlogImport(paths, 0)
	assert.NotNil(t, err)

	cm.listErr = errors.New("error")
	wrapper.chunkManager = cm
	wrapper.callFlushFunc = func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		return nil
	}

	// failed to call parser.Parse()
	err = wrapper.doBinlogImport(paths, 0)
	assert.NotNil(t, err)

	// Import() failed
	err = wrapper.Import(paths, false, false)
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
	err = wrapper.doBinlogImport(paths, 0)
	assert.Nil(t, err)
}
