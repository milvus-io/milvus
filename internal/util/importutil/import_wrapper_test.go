package importutil

import (
	"context"
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

const (
	TempFilesPath = "/tmp/milvus_test/import/"
)

func Test_NewImportWrapper(t *testing.T) {
	ctx := context.Background()
	wrapper := NewImportWrapper(ctx, nil, 2, 1, nil, nil)
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
	wrapper = NewImportWrapper(ctx, schema, 2, 1, nil, nil)
	assert.NotNil(t, wrapper)

	err := wrapper.Cancel()
	assert.Nil(t, err)
}

func saveFile(t *testing.T, filePath string, content []byte) *os.File {
	fp, err := os.Create(filePath)
	assert.Nil(t, err)

	_, err = fp.Write(content)
	assert.Nil(t, err)

	return fp
}

func Test_ImportRowBased(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

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
	fp1 := saveFile(t, filePath, content)
	defer fp1.Close()

	rowCount := 0
	flushFunc := func(fields map[string]storage.FieldData) error {
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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, flushFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, true, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)

	// parse error
	content = []byte(`{
		"rows":[
			{"field_bool": true, "field_int8": false, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
		]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	fp2 := saveFile(t, filePath, content)
	defer fp2.Close()

	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, flushFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, true, false)
	assert.NotNil(t, err)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, true, false)
	assert.NotNil(t, err)

}

func Test_ImportColumnBased(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

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
	fp1 := saveFile(t, filePath, content)
	defer fp1.Close()

	rowCount := 0
	flushFunc := func(fields map[string]storage.FieldData) error {
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
	wrapper := NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, flushFunc)
	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.Nil(t, err)
	assert.Equal(t, 5, rowCount)

	// parse error
	content = []byte(`{
		"field_bool": [true, false, true, true, true]
	}`)

	filePath = TempFilesPath + "rows_2.json"
	fp2 := saveFile(t, filePath, content)
	defer fp2.Close()

	wrapper = NewImportWrapper(ctx, sampleSchema(), 2, 1, idAllocator, flushFunc)
	files = make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)

	// file doesn't exist
	files = make([]string, 0)
	files = append(files, "/dummy/dummy.json")
	err = wrapper.Import(files, false, false)
	assert.NotNil(t, err)
}
