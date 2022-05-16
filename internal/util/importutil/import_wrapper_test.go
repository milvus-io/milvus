package importutil

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const (
	TempFilesPath = "/tmp/milvus_test/import/"
)

type MockChunkManager struct {
	size int64
}

func (mc *MockChunkManager) Path(filePath string) (string, error) {
	return "", nil
}

func (mc *MockChunkManager) Reader(filePath string) (storage.FileReader, error) {
	return nil, nil
}

func (mc *MockChunkManager) Write(filePath string, content []byte) error {
	return nil
}

func (mc *MockChunkManager) MultiWrite(contents map[string][]byte) error {
	return nil
}

func (mc *MockChunkManager) Exist(filePath string) (bool, error) {
	return true, nil
}

func (mc *MockChunkManager) Read(filePath string) ([]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) MultiRead(filePaths []string) ([][]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) ListWithPrefix(prefix string) ([]string, error) {
	return nil, nil
}

func (mc *MockChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	return nil, nil, nil
}

func (mc *MockChunkManager) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) Mmap(filePath string) (*mmap.ReaderAt, error) {
	return nil, nil
}

func (mc *MockChunkManager) Size(filePath string) (int64, error) {
	return mc.size, nil
}

func (mc *MockChunkManager) Remove(filePath string) error {
	return nil
}

func (mc *MockChunkManager) MultiRemove(filePaths []string) error {
	return nil
}

func (mc *MockChunkManager) RemoveWithPrefix(prefix string) error {
	return nil
}

func Test_NewImportWrapper(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
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

func Test_ImportRowBased(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
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
	err = cm.Write(filePath, content)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
	err = cm.Write(filePath, content)
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

func Test_ImportColumnBased_json(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
	err = cm.Write(filePath, content)
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
	err = cm.Write(filePath, content)
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

func Test_ImportColumnBased_StringKey(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
	err = cm.Write(filePath, content)
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

func Test_ImportColumnBased_numpy(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
	err = cm.Write(filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_binary_vector.npy"
	bin := [][2]uint8{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}
	content, err = CreateNumpyData(bin)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(filePath, content)
	assert.NoError(t, err)
	files = append(files, filePath)

	filePath = TempFilesPath + "field_float_vector.npy"
	flo := [][4]float32{{1, 2, 3, 4}, {3, 4, 5, 6}, {5, 6, 7, 8}, {7, 8, 9, 10}, {9, 10, 11, 12}}
	content, err = CreateNumpyData(flo)
	assert.Nil(t, err)
	log.Debug("content", zap.Any("c", content))
	err = cm.Write(filePath, content)
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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, cm, flushFunc, importResult, reportFunc)

	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)

	// parse error
	content = []byte(`{
		"field_bool": [true, false, true, true, true]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	err = cm.Write(filePath, content)
	assert.NoError(t, err)

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

func Test_ImportRowBased_perf(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
		err = cm.Write(filePath, b.Bytes())
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

func Test_ImportColumnBased_perf(t *testing.T) {
	f := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	cm, err := f.NewVectorStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix("")

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
		err = cm.Write(filePath, b.Bytes())
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

func Test_FileValidation(t *testing.T) {
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
	var files = [2]string{"1.npy", "1.npy"}
	err := wrapper.fileValidation(files[:], false)
	assert.NotNil(t, err)
	err = wrapper.fileValidation(files[:], true)
	assert.NotNil(t, err)

	// unsupported file type
	files[0] = "1"
	files[1] = "1"
	err = wrapper.fileValidation(files[:], true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files[:], false)
	assert.NotNil(t, err)

	// valid cases
	files[0] = "1.json"
	files[1] = "2.json"
	err = wrapper.fileValidation(files[:], true)
	assert.Nil(t, err)

	files[1] = "2.npy"
	err = wrapper.fileValidation(files[:], false)
	assert.Nil(t, err)

	// empty file
	cm = &MockChunkManager{
		size: 0,
	}
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)
	err = wrapper.fileValidation(files[:], true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files[:], false)
	assert.NotNil(t, err)

	// file size exceed limit
	cm = &MockChunkManager{
		size: MaxFileSize + 1,
	}
	wrapper = NewImportWrapper(ctx, schema, int32(shardNum), int64(segmentSize), idAllocator, cm, nil, nil, nil)
	err = wrapper.fileValidation(files[:], true)
	assert.NotNil(t, err)

	err = wrapper.fileValidation(files[:], false)
	assert.NotNil(t, err)
}
