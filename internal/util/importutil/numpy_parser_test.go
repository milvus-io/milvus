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
	"context"
	"math"
	"os"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

func createLocalChunkManager(t *testing.T) storage.ChunkManager {
	ctx := context.Background()
	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	return cm
}

func createNumpyParser(t *testing.T) *NumpyParser {
	ctx := context.Background()
	schema := sampleSchema()
	idAllocator := newIDAllocator(ctx, t, nil)

	cm := createLocalChunkManager(t)

	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		return nil
	}

	parser, err := NewNumpyParser(ctx, schema, idAllocator, 2, 100, cm, flushFunc, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)
	return parser
}

func findSchema(schema *schemapb.CollectionSchema, dt schemapb.DataType) *schemapb.FieldSchema {
	fields := schema.Fields
	for _, field := range fields {
		if field.GetDataType() == dt {
			return field
		}
	}
	return nil
}

func Test_NewNumpyParser(t *testing.T) {
	ctx := context.Background()

	parser, err := NewNumpyParser(ctx, nil, nil, 2, 100, nil, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, parser)

	schema := sampleSchema()
	parser, err = NewNumpyParser(ctx, schema, nil, 2, 100, nil, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, parser)

	idAllocator := newIDAllocator(ctx, t, nil)
	parser, err = NewNumpyParser(ctx, schema, idAllocator, 2, 100, nil, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, parser)

	cm := createLocalChunkManager(t)

	parser, err = NewNumpyParser(ctx, schema, idAllocator, 2, 100, cm, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, parser)

	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		return nil
	}
	parser, err = NewNumpyParser(ctx, schema, idAllocator, 2, 100, cm, flushFunc, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)
}

func Test_NumpyParserValidateFileNames(t *testing.T) {
	parser := createNumpyParser(t)

	// file has no corresponding field in collection
	err := parser.validateFileNames([]string{"dummy.npy"})
	assert.Error(t, err)

	// there is no file corresponding to field
	fileNames := []string{
		"FieldBool.npy",
		"FieldInt8.npy",
		"FieldInt16.npy",
		"FieldInt32.npy",
		"FieldInt64.npy",
		"FieldFloat.npy",
		"FieldDouble.npy",
		"FieldString.npy",
		"FieldJSON.npy",
		"FieldBinaryVector.npy",
	}
	err = parser.validateFileNames(fileNames)
	assert.Error(t, err)

	// valid
	fileNames = append(fileNames, "FieldFloatVector.npy")
	err = parser.validateFileNames(fileNames)
	assert.NoError(t, err)

	// has dynamic field
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		AutoID:             true,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "FieldInt64",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:   102,
				Name:      "FieldDynamic",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}
	fileNames = []string{"FieldInt64.npy"}
	err = parser.validateFileNames(fileNames)
	assert.NoError(t, err)

	fileNames = append(fileNames, "FieldDynamic.npy")
	err = parser.validateFileNames(fileNames)
	assert.NoError(t, err)
}

func Test_NumpyParserValidateHeader(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	parser := createNumpyParser(t)

	// nil input error
	err = parser.validateHeader(nil)
	assert.Error(t, err)

	t.Run("not a valid numpy array", func(t *testing.T) {
		filePath := TempFilesPath + "invalid.npy"
		err = CreateNumpyFile(filePath, "aaa")
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		columnReader := &NumpyColumnReader{
			fieldName: "invalid",
			reader:    adapter,
		}
		err = parser.validateHeader(columnReader)
		assert.Error(t, err)
	})

	validateHeader := func(data interface{}, fieldSchema *schemapb.FieldSchema) error {
		filePath := TempFilesPath + fieldSchema.GetName() + ".npy"

		err = CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		dim, _ := getFieldDimension(fieldSchema)
		columnReader := &NumpyColumnReader{
			fieldName: fieldSchema.GetName(),
			fieldID:   fieldSchema.GetFieldID(),
			dataType:  fieldSchema.GetDataType(),
			dimension: dim,
			file:      file,
			reader:    adapter,
		}
		err = parser.validateHeader(columnReader)
		return err
	}

	t.Run("veridate float vector numpy", func(t *testing.T) {
		// numpy file is not vectors
		data1 := []int32{1, 2, 3, 4}
		schema := findSchema(sampleSchema(), schemapb.DataType_FloatVector)
		err = validateHeader(data1, schema)
		assert.Error(t, err)

		// field data type is not float vector type
		data2 := []float32{1.1, 2.1, 3.1, 4.1}
		err = validateHeader(data2, schema)
		assert.Error(t, err)

		// dimension mismatch
		data3 := [][4]float32{{1.1, 2.1, 3.1, 4.1}, {5.2, 6.2, 7.2, 8.2}}
		schema = &schemapb.FieldSchema{
			FieldID:      111,
			Name:         "FieldFloatVector",
			IsPrimaryKey: false,
			Description:  "float_vector",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "99"},
			},
		}
		err = validateHeader(data3, schema)
		assert.Error(t, err)
	})

	t.Run("veridate binary vector numpy", func(t *testing.T) {
		// numpy file is not vectors
		data1 := []int32{1, 2, 3, 4}
		schema := findSchema(sampleSchema(), schemapb.DataType_BinaryVector)
		err = validateHeader(data1, schema)
		assert.Error(t, err)

		// field data type is not binary vector type
		data2 := []uint8{1, 2, 3, 4, 5, 6}
		err = validateHeader(data2, schema)
		assert.Error(t, err)

		// dimension mismatch
		data3 := [][2]uint8{{1, 2}, {3, 4}, {5, 6}}
		schema = &schemapb.FieldSchema{
			FieldID:      110,
			Name:         "FieldBinaryVector",
			IsPrimaryKey: false,
			Description:  "binary_vector",
			DataType:     schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "99"},
			},
		}
		err = validateHeader(data3, schema)
		assert.Error(t, err)
	})

	t.Run("veridate scalar numpy", func(t *testing.T) {
		// data type mismatch
		data1 := []int32{1, 2, 3, 4}
		schema := findSchema(sampleSchema(), schemapb.DataType_Int8)
		err = validateHeader(data1, schema)
		assert.Error(t, err)

		// illegal shape
		data2 := [][2]int8{{1, 2}, {3, 4}, {5, 6}}
		err = validateHeader(data2, schema)
		assert.Error(t, err)
	})
}

func Test_NumpyParserCreateReaders(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)
	parser := createNumpyParser(t)

	// no field match the filename
	t.Run("no field match the filename", func(t *testing.T) {
		filePath := TempFilesPath + "dummy.npy"
		files := []string{filePath}
		readers, err := parser.createReaders(files)
		assert.Error(t, err)
		assert.Empty(t, readers)
		defer closeReaders(readers)
	})

	// file doesn't exist
	t.Run("file doesnt exist", func(t *testing.T) {
		filePath := TempFilesPath + "FieldBool.npy"
		files := []string{filePath}
		readers, err := parser.createReaders(files)
		assert.Error(t, err)
		assert.Empty(t, readers)
		defer closeReaders(readers)
	})

	// not a numpy file
	t.Run("not a numpy file", func(t *testing.T) {
		ctx := context.Background()
		filePath := TempFilesPath + "FieldBool.npy"
		files := []string{filePath}
		err = cm.Write(ctx, filePath, []byte{1, 2, 3})
		readers, err := parser.createReaders(files)
		assert.Error(t, err)
		assert.Empty(t, readers)
		defer closeReaders(readers)
	})

	t.Run("succeed", func(t *testing.T) {
		files := createSampleNumpyFiles(t, cm)
		readers, err := parser.createReaders(files)
		assert.NoError(t, err)
		assert.Equal(t, len(files), len(readers))
		for i := 0; i < len(readers); i++ {
			reader := readers[i]
			schema := findSchema(sampleSchema(), reader.dataType)
			assert.NotNil(t, schema)
			assert.Equal(t, schema.GetName(), reader.fieldName)
			assert.Equal(t, schema.GetFieldID(), reader.fieldID)
			dim, _ := getFieldDimension(schema)
			assert.Equal(t, dim, reader.dimension)
		}
		defer closeReaders(readers)
	})

	t.Run("row count doesnt equal", func(t *testing.T) {
		files := createSampleNumpyFiles(t, cm)
		filePath := TempFilesPath + "FieldBool.npy"
		err = CreateNumpyFile(filePath, []bool{true})
		assert.NoError(t, err)

		readers, err := parser.createReaders(files)
		assert.Error(t, err)
		assert.Empty(t, readers)
		defer closeReaders(readers)
	})

	t.Run("velidate header failed", func(t *testing.T) {
		filePath := TempFilesPath + "FieldBool.npy"
		err = CreateNumpyFile(filePath, []int32{1, 2, 3, 4, 5})
		assert.NoError(t, err)
		files := []string{filePath}
		readers, err := parser.createReaders(files)
		assert.Error(t, err)
		assert.Empty(t, readers)
		closeReaders(readers)
	})
}

func Test_NumpyParserReadData(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)
	parser := createNumpyParser(t)

	t.Run("general cases", func(t *testing.T) {
		files := createSampleNumpyFiles(t, cm)
		readers, err := parser.createReaders(files)
		assert.NoError(t, err)
		assert.Equal(t, len(files), len(readers))
		defer closeReaders(readers)

		// each sample file has 5 rows, read the first 2 rows
		for _, reader := range readers {
			fieldData, err := parser.readData(reader, 2)
			assert.NoError(t, err)
			assert.Equal(t, 2, fieldData.RowNum())
		}

		// read the left rows
		for _, reader := range readers {
			fieldData, err := parser.readData(reader, 100)
			assert.NoError(t, err)
			assert.Equal(t, 3, fieldData.RowNum())
		}

		// unsupport data type
		columnReader := &NumpyColumnReader{
			fieldName: "dummy",
			dataType:  schemapb.DataType_None,
		}
		fieldData, err := parser.readData(columnReader, 2)
		assert.Error(t, err)
		assert.Nil(t, fieldData)
	})

	readEmptyFunc := func(filedName string, data interface{}) {
		filePath := TempFilesPath + filedName + ".npy"
		err = CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		readers, err := parser.createReaders([]string{filePath})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(readers))
		defer closeReaders(readers)

		// row count 0 is not allowed
		fieldData, err := parser.readData(readers[0], 0)
		assert.Error(t, err)
		assert.Nil(t, fieldData)

		// nothint to read
		_, err = parser.readData(readers[0], 2)
		assert.NoError(t, err)
	}

	readBatchFunc := func(filedName string, data interface{}, dataLen int, getValue func(k int) interface{}) {
		filePath := TempFilesPath + filedName + ".npy"
		err = CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		readers, err := parser.createReaders([]string{filePath})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(readers))
		defer closeReaders(readers)

		readPosition := 2
		fieldData, err := parser.readData(readers[0], readPosition)
		assert.NoError(t, err)
		assert.Equal(t, readPosition, fieldData.RowNum())
		for i := 0; i < readPosition; i++ {
			assert.Equal(t, getValue(i), fieldData.GetRow(i))
		}

		if dataLen > readPosition {
			fieldData, err = parser.readData(readers[0], dataLen+1)
			assert.NoError(t, err)
			assert.Equal(t, dataLen-readPosition, fieldData.RowNum())
			for i := readPosition; i < dataLen; i++ {
				assert.Equal(t, getValue(i), fieldData.GetRow(i-readPosition))
			}
		}
	}

	readErrorFunc := func(filedName string, data interface{}) {
		filePath := TempFilesPath + filedName + ".npy"
		err = CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		readers, err := parser.createReaders([]string{filePath})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(readers))
		defer closeReaders(readers)

		// encounter error
		fieldData, err := parser.readData(readers[0], 1000)
		assert.Error(t, err)
		assert.Nil(t, fieldData)
	}

	t.Run("read bool", func(t *testing.T) {
		readEmptyFunc("FieldBool", []bool{})

		data := []bool{true, false, true, false, false, true}
		readBatchFunc("FieldBool", data, len(data), func(k int) interface{} { return data[k] })
	})

	t.Run("read int8", func(t *testing.T) {
		readEmptyFunc("FieldInt8", []int8{})

		data := []int8{1, 3, 5, 7, 9, 4, 2, 6, 8}
		readBatchFunc("FieldInt8", data, len(data), func(k int) interface{} { return data[k] })
	})

	t.Run("read int16", func(t *testing.T) {
		readEmptyFunc("FieldInt16", []int16{})

		data := []int16{21, 13, 35, 47, 59, 34, 12}
		readBatchFunc("FieldInt16", data, len(data), func(k int) interface{} { return data[k] })
	})

	t.Run("read int32", func(t *testing.T) {
		readEmptyFunc("FieldInt32", []int32{})

		data := []int32{1, 3, 5, 7, 9, 4, 2, 6, 8}
		readBatchFunc("FieldInt32", data, len(data), func(k int) interface{} { return data[k] })
	})

	t.Run("read int64", func(t *testing.T) {
		readEmptyFunc("FieldInt64", []int64{})

		data := []int64{100, 200}
		readBatchFunc("FieldInt64", data, len(data), func(k int) interface{} { return data[k] })
	})

	t.Run("read float", func(t *testing.T) {
		readEmptyFunc("FieldFloat", []float32{})

		data := []float32{2.5, 32.2, 53.254, 3.45, 65.23421, 54.8978}
		readBatchFunc("FieldFloat", data, len(data), func(k int) interface{} { return data[k] })
		data = []float32{2.5, 32.2, float32(math.NaN())}
		readErrorFunc("FieldFloat", data)
	})

	t.Run("read double", func(t *testing.T) {
		readEmptyFunc("FieldDouble", []float64{})

		data := []float64{65.24454, 343.4365, 432.6556}
		readBatchFunc("FieldDouble", data, len(data), func(k int) interface{} { return data[k] })
		data = []float64{65.24454, math.Inf(1)}
		readErrorFunc("FieldDouble", data)
	})

	specialReadEmptyFunc := func(filedName string, data interface{}) {
		ctx := context.Background()
		filePath := TempFilesPath + filedName + ".npy"
		content, err := CreateNumpyData(data)
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, content)
		assert.NoError(t, err)

		readers, err := parser.createReaders([]string{filePath})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(readers))
		defer closeReaders(readers)

		// row count 0 is not allowed
		fieldData, err := parser.readData(readers[0], 0)
		assert.Error(t, err)
		assert.Nil(t, fieldData)
	}

	t.Run("read varchar", func(t *testing.T) {
		specialReadEmptyFunc("FieldString", []string{"aaa"})
	})

	t.Run("read JSON", func(t *testing.T) {
		specialReadEmptyFunc("FieldJSON", []string{"{\"x\": 1}"})
	})

	t.Run("read binary vector", func(t *testing.T) {
		specialReadEmptyFunc("FieldBinaryVector", [][2]uint8{{1, 2}, {3, 4}})
	})

	t.Run("read float vector", func(t *testing.T) {
		specialReadEmptyFunc("FieldFloatVector", [][4]float32{{1, 2, 3, 4}, {3, 4, 5, 6}})
		specialReadEmptyFunc("FieldFloatVector", [][4]float64{{1, 2, 3, 4}, {3, 4, 5, 6}})

		readErrorFunc("FieldFloatVector", [][4]float32{{1, 2, 3, float32(math.NaN())}, {3, 4, 5, 6}})
		readErrorFunc("FieldFloatVector", [][4]float64{{1, 2, 3, 4}, {3, 4, math.Inf(1), 6}})
	})
}

func Test_NumpyParserPrepareAppendFunctions(t *testing.T) {
	parser := createNumpyParser(t)

	// succeed
	appendFuncs, err := parser.prepareAppendFunctions()
	assert.NoError(t, err)
	assert.Equal(t, len(sampleSchema().Fields), len(appendFuncs))

	// schema has unsupported data type
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name: "schema",
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
				DataType:     schemapb.DataType_None,
			},
		},
	}
	appendFuncs, err = parser.prepareAppendFunctions()
	assert.Error(t, err)
	assert.Nil(t, appendFuncs)
}

func Test_NumpyParserCheckRowCount(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)
	parser := createNumpyParser(t)

	files := createSampleNumpyFiles(t, cm)
	readers, err := parser.createReaders(files)
	assert.NoError(t, err)
	defer closeReaders(readers)

	// succeed
	segmentData := make(map[storage.FieldID]storage.FieldData)
	for _, reader := range readers {
		fieldData, err := parser.readData(reader, 100)
		assert.NoError(t, err)
		segmentData[reader.fieldID] = fieldData
	}

	rowCount, primaryKey, err := parser.checkRowCount(segmentData)
	assert.NoError(t, err)
	assert.Equal(t, 5, rowCount)
	assert.NotNil(t, primaryKey)
	assert.Equal(t, "FieldInt64", primaryKey.GetName())

	// field data missed
	delete(segmentData, 102)
	rowCount, primaryKey, err = parser.checkRowCount(segmentData)
	assert.Error(t, err)
	assert.Zero(t, rowCount)
	assert.Nil(t, primaryKey)

	// primarykey missed
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      105,
				Name:         "FieldInt32",
				IsPrimaryKey: false,
				AutoID:       false,
				DataType:     schemapb.DataType_Int32,
			},
		},
	}

	segmentData[105] = &storage.Int32FieldData{
		Data: []int32{1, 2, 3, 4},
	}

	rowCount, primaryKey, err = parser.checkRowCount(segmentData)
	assert.Error(t, err)
	assert.Zero(t, rowCount)
	assert.Nil(t, primaryKey)

	// row count mismatch
	parser.collectionSchema.Fields = append(parser.collectionSchema.Fields, &schemapb.FieldSchema{
		FieldID:      106,
		Name:         "FieldInt64",
		IsPrimaryKey: true,
		AutoID:       false,
		DataType:     schemapb.DataType_Int64,
	})

	segmentData[106] = &storage.Int64FieldData{
		Data: []int64{1, 2, 4},
	}

	rowCount, primaryKey, err = parser.checkRowCount(segmentData)
	assert.Error(t, err)
	assert.Zero(t, rowCount)
	assert.Nil(t, primaryKey)

	// has dynamic field
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		AutoID:             true,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "FieldInt64",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:   102,
				Name:      "FieldDynamic",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}
	segmentData[101] = &storage.Int64FieldData{
		Data: []int64{1, 2, 4},
	}

	rowCount, primaryKey, err = parser.checkRowCount(segmentData)
	assert.NoError(t, err)
	assert.Equal(t, 3, rowCount)
	assert.NotNil(t, primaryKey)
}

func Test_NumpyParserSplitFieldsData(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)
	parser := createNumpyParser(t)

	segmentData := make(map[storage.FieldID]storage.FieldData)
	t.Run("segemnt data is empty", func(t *testing.T) {
		err = parser.splitFieldsData(segmentData, nil)
		assert.Error(t, err)
	})

	files := createSampleNumpyFiles(t, cm)
	readers, err := parser.createReaders(files)
	assert.NoError(t, err)
	defer closeReaders(readers)

	for _, reader := range readers {
		fieldData, err := parser.readData(reader, 100)
		assert.NoError(t, err)
		segmentData[reader.fieldID] = fieldData
	}

	shards := make([]map[storage.FieldID]storage.FieldData, 0, parser.shardNum)
	t.Run("shards number mismatch", func(t *testing.T) {
		err = parser.splitFieldsData(segmentData, shards)
		assert.Error(t, err)
	})

	t.Run("checkRowCount returns error", func(t *testing.T) {
		parser.collectionSchema = &schemapb.CollectionSchema{
			Name: "schema",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      105,
					Name:         "FieldInt32",
					IsPrimaryKey: false,
					AutoID:       false,
					DataType:     schemapb.DataType_Int32,
				},
			},
		}
		for i := 0; i < int(parser.shardNum); i++ {
			shards = append(shards, initSegmentData(parser.collectionSchema))
		}
		err = parser.splitFieldsData(segmentData, shards)
		assert.Error(t, err)
		parser.collectionSchema = sampleSchema()
	})

	t.Run("failed to alloc id", func(t *testing.T) {
		ctx := context.Background()
		parser.rowIDAllocator = newIDAllocator(ctx, t, errors.New("dummy error"))
		err = parser.splitFieldsData(segmentData, shards)
		assert.Error(t, err)
		parser.rowIDAllocator = newIDAllocator(ctx, t, nil)
	})

	t.Run("primary key auto-generated", func(t *testing.T) {
		schema := findSchema(parser.collectionSchema, schemapb.DataType_Int64)
		schema.AutoID = true

		shards = make([]map[storage.FieldID]storage.FieldData, 0, parser.shardNum)
		for i := 0; i < int(parser.shardNum); i++ {
			segmentData := initSegmentData(parser.collectionSchema)
			shards = append(shards, segmentData)
		}
		err = parser.splitFieldsData(segmentData, shards)
		assert.NoError(t, err)
		assert.NotEmpty(t, parser.autoIDRange)

		totalNum := 0
		for i := 0; i < int(parser.shardNum); i++ {
			totalNum += shards[i][106].RowNum()
		}
		assert.Equal(t, segmentData[106].RowNum(), totalNum)

		// target field data is nil
		shards[0][105] = nil
		err = parser.splitFieldsData(segmentData, shards)
		assert.Error(t, err)

		schema.AutoID = false
	})

	t.Run("has dynamic field", func(t *testing.T) {
		parser.collectionSchema = &schemapb.CollectionSchema{
			Name:               "schema",
			Description:        "schema",
			AutoID:             true,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      101,
					Name:         "FieldInt64",
					IsPrimaryKey: true,
					AutoID:       false,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:   102,
					Name:      "FieldDynamic",
					IsDynamic: true,
					DataType:  schemapb.DataType_JSON,
				},
			},
		}
		shards = make([]map[storage.FieldID]storage.FieldData, 0, parser.shardNum)
		for i := 0; i < int(parser.shardNum); i++ {
			segmentData := initSegmentData(parser.collectionSchema)
			shards = append(shards, segmentData)
		}
		segmentData = make(map[storage.FieldID]storage.FieldData)
		segmentData[101] = &storage.Int64FieldData{
			Data: []int64{1, 2, 4},
		}
		err = parser.splitFieldsData(segmentData, shards)
		assert.NoError(t, err)
	})
}

func Test_NumpyParserCalcRowCountPerBlock(t *testing.T) {
	parser := createNumpyParser(t)

	// succeed
	rowCount, err := parser.calcRowCountPerBlock()
	assert.NoError(t, err)
	assert.Greater(t, rowCount, int64(0))

	// failed to estimate row size
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      109,
				Name:         "FieldString",
				IsPrimaryKey: false,
				Description:  "string",
				DataType:     schemapb.DataType_VarChar,
			},
		},
	}
	rowCount, err = parser.calcRowCountPerBlock()
	assert.Error(t, err)
	assert.Zero(t, rowCount)

	// no field
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name: "schema",
	}
	rowCount, err = parser.calcRowCountPerBlock()
	assert.Error(t, err)
	assert.Zero(t, rowCount)
}

func Test_NumpyParserConsume(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)
	parser := createNumpyParser(t)

	files := createSampleNumpyFiles(t, cm)
	readers, err := parser.createReaders(files)
	assert.NoError(t, err)
	assert.Equal(t, len(sampleSchema().Fields), len(readers))

	// succeed
	err = parser.consume(readers)
	assert.NoError(t, err)
	closeReaders(readers)

	// row count mismatch
	parser.blockSize = 1000
	readers, err = parser.createReaders(files)
	assert.NoError(t, err)
	parser.readData(readers[0], 1)
	err = parser.consume(readers)
	assert.Error(t, err)

	// invalid schema
	parser.collectionSchema = &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      109,
				Name:         "dummy",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_None,
			},
		},
	}
	err = parser.consume(readers)
	assert.Error(t, err)
	closeReaders(readers)
}

func Test_NumpyParserParse(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	parser := createNumpyParser(t)
	parser.blockSize = 400

	t.Run("validate file name failed", func(t *testing.T) {
		files := []string{"dummy.npy"}
		err = parser.Parse(files)
		assert.Error(t, err)
	})

	t.Run("file doesnt exist", func(t *testing.T) {
		parser.collectionSchema = perfSchema(4)
		files := []string{"ID.npy", "Vector.npy"}
		err = parser.Parse(files)
		assert.Error(t, err)
		parser.collectionSchema = sampleSchema()
	})

	t.Run("succeed", func(t *testing.T) {
		cm := createLocalChunkManager(t)
		files := createSampleNumpyFiles(t, cm)

		totalRowCount := 0
		parser.callFlushFunc = func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
			assert.LessOrEqual(t, int32(shardID), parser.shardNum)
			rowCount := 0
			for _, fieldData := range fields {
				if rowCount == 0 {
					rowCount = fieldData.RowNum()
				} else {
					assert.Equal(t, rowCount, fieldData.RowNum())
				}
			}
			totalRowCount += rowCount
			return nil
		}
		err = parser.Parse(files)
		assert.NoError(t, err)
		assert.Equal(t, 5, totalRowCount)
	})
}

func Test_NumpyParserParse_perf(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	cm := createLocalChunkManager(t)

	tr := timerecord.NewTimeRecorder("numpy parse performance")

	// change the parameter to test performance
	rowCount := 10000
	dotValue := float32(3.1415926)
	const (
		dim = 128
	)

	idData := make([]int64, 0)
	vecData := make([][dim]float32, 0)
	for i := 0; i < rowCount; i++ {
		var row [dim]float32
		for k := 0; k < dim; k++ {
			row[k] = float32(i) + dotValue
		}
		vecData = append(vecData, row)
		idData = append(idData, int64(i))
	}

	tr.Record("generate large data")

	createNpyFile := func(t *testing.T, fielName string, data interface{}) string {
		filePath := TempFilesPath + fielName + ".npy"
		content, err := CreateNumpyData(data)
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, content)
		assert.NoError(t, err)
		return filePath
	}

	idFilePath := createNpyFile(t, "ID", idData)
	vecFilePath := createNpyFile(t, "Vector", vecData)

	tr.Record("generate large numpy files")

	shardNum := int32(3)
	totalRowCount := 0
	callFlushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		assert.LessOrEqual(t, int32(shardID), shardNum)
		rowCount := 0
		for _, fieldData := range fields {
			if rowCount == 0 {
				rowCount = fieldData.RowNum()
			} else {
				assert.Equal(t, rowCount, fieldData.RowNum())
			}
		}
		totalRowCount += rowCount
		return nil
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	updateProgress := func(percent int64) {
		assert.Greater(t, percent, int64(0))
	}
	parser, err := NewNumpyParser(ctx, perfSchema(dim), idAllocator, shardNum, 16*1024*1024, cm, callFlushFunc, updateProgress)
	assert.NoError(t, err)
	assert.NotNil(t, parser)
	parser.collectionSchema = perfSchema(dim)

	err = parser.Parse([]string{idFilePath, vecFilePath})
	assert.NoError(t, err)
	assert.Equal(t, rowCount, totalRowCount)

	tr.Record("parse large numpy files")
}
