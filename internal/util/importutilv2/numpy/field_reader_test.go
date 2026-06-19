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

package numpy

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func init() {
	paramtable.Init()
}

func encodeToGB2312(input string) ([]byte, error) {
	encoder := simplifiedchinese.GB18030.NewEncoder() // GB18030 is compatible with GB2312.
	var buf bytes.Buffer
	writer := transform.NewWriter(&buf, encoder)

	_, err := writer.Write([]byte(input))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func WriteNonUTF8Npy() io.Reader {
	// Use bytes.Buffer instead of writing to a file
	var buffer bytes.Buffer

	// Step 1: Write Magic Number and version
	buffer.Write([]byte{0x93, 'N', 'U', 'M', 'P', 'Y'})
	buffer.Write([]byte{1, 0}) // Numpy 1.0 version

	// Step 2: Construct the header (using '|S20' for 20-byte fixed-length strings)
	header := "{'descr': '|S20', 'fortran_order': False, 'shape': (6,), }"
	headerBytes := []byte(header)

	// Pad the header to align its length to a multiple of 64 bytes
	padding := 64 - (10+len(headerBytes)+1)%64
	headerBytes = append(headerBytes, make([]byte, padding)...)
	headerBytes = append(headerBytes, '\n')

	// Write the header length and the header itself
	binary.Write(&buffer, binary.LittleEndian, uint16(len(headerBytes)))
	buffer.Write(headerBytes)

	// Step 3: Write non-UTF-8 string data (e.g., Latin1 encoded)
	testStrings := []string{
		"hello, vector database",               // valid utf-8
		string([]byte{0xC3, 0xA9, 0xB5, 0xF1}), // Latin1 encoded "éµñ"
		string([]byte{0xB0, 0xE1, 0xF2, 0xBD}), // Random non-UTF-8 data
		string([]byte{0xD2, 0xA9, 0xC8, 0xFC}), // Another set of non-UTF-8 data
	}

	for _, str := range testStrings {
		data := make([]byte, 20)
		copy(data, str)
		buffer.Write(data)
	}

	// Step 4: Encode and write GB-2312 Chinese strings
	chineseStrings := []string{
		"你好，世界", // "Hello, World"
		"向量数据库", // "Data Storage"
	}

	for _, str := range chineseStrings {
		gb2312Data, err := encodeToGB2312(str)
		if err != nil {
			panic(err)
		}
		data := make([]byte, 20)
		copy(data, gb2312Data)
		buffer.Write(data)
	}

	// Step 5: Convert buffer to a reader to simulate file reading
	reader := bytes.NewReader(buffer.Bytes())
	return reader
}

func TestInvalidUTF8(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:    100,
		Name:       "str",
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
	}

	reader := WriteNonUTF8Npy()
	fr, err := NewFieldReader(reader, fieldSchema, common.DefaultTimezone)
	assert.NoError(t, err)

	_, _, err = fr.Next(int64(6))
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "contains invalid UTF-8 data"))
}

func WriteNormalNpy() io.Reader {
	// Use bytes.Buffer instead of writing to a file
	var buffer bytes.Buffer

	// Step 1: Write Magic Number and version
	buffer.Write([]byte{0x93, 'N', 'U', 'M', 'P', 'Y'})
	buffer.Write([]byte{1, 0}) // Numpy 1.0 version

	// Step 2: Construct the header (using '|S20' for 20-byte fixed-length strings)
	header := "{'descr': '<U17', 'fortran_order': False, 'shape': (5,), }"
	headerBytes := []byte(header)

	// Pad the header to align its length to a multiple of 64 bytes
	padding := 64 - (10+len(headerBytes)+1)%64
	headerBytes = append(headerBytes, make([]byte, padding)...)
	headerBytes = append(headerBytes, '\n')

	// Write the header length and the header itself
	binary.Write(&buffer, binary.LittleEndian, uint16(len(headerBytes)))
	buffer.Write(headerBytes)

	// Step 3: Write non-UTF-8 string data (e.g., Latin1 encoded)
	testStrings := []string{
		"this is varchar 0",
		"this is varchar 1",
		"this is varchar 2",
		"this is varchar 3",
		"this is varchar 4",
	}

	for _, str := range testStrings {
		data := make([]byte, 20)
		copy(data, str)
		buffer.Write(data)
	}

	// Step 4: Convert buffer to a reader to simulate file reading
	reader := bytes.NewReader(buffer.Bytes())
	return reader
}

func TestNormalRead(t *testing.T) {
	checkFn := func(nullable bool) {
		fieldSchema := &schemapb.FieldSchema{
			FieldID:    100,
			Name:       "str",
			DataType:   schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
			Nullable:   nullable,
		}

		reader := WriteNormalNpy()
		fr, err := NewFieldReader(reader, fieldSchema, common.DefaultTimezone)
		assert.NoError(t, err)

		data, validData, err := fr.Next(int64(6))
		assert.NoError(t, err)
		values, ok := data.([]string)
		assert.True(t, ok)
		assert.Equal(t, 5, len(values))
		if nullable {
			assert.NotNil(t, validData)
			valids, ok := validData.([]bool)
			assert.True(t, ok)
			assert.Equal(t, len(values), len(valids))
		} else {
			assert.Nil(t, validData)
		}
	}

	checkFn(false)
	checkFn(true)
}

func TestNullableVectorValidDataUsesLogicalRows(t *testing.T) {
	tests := []struct {
		name      string
		dataType  schemapb.DataType
		fieldData storage.FieldData
	}{
		{
			name:     "float vector",
			dataType: schemapb.DataType_FloatVector,
			fieldData: &storage.FloatVectorFieldData{
				Data: make([]float32, 2*dim),
				Dim:  dim,
			},
		},
		{
			name:     "float16 vector",
			dataType: schemapb.DataType_Float16Vector,
			fieldData: &storage.Float16VectorFieldData{
				Data: make([]byte, 2*dim*2),
				Dim:  dim,
			},
		},
		{
			name:     "bfloat16 vector",
			dataType: schemapb.DataType_BFloat16Vector,
			fieldData: &storage.BFloat16VectorFieldData{
				Data: make([]byte, 2*dim*2),
				Dim:  dim,
			},
		},
		{
			name:     "int8 vector",
			dataType: schemapb.DataType_Int8Vector,
			fieldData: &storage.Int8VectorFieldData{
				Data: make([]int8, 2*dim),
				Dim:  dim,
			},
		},
		{
			name:     "binary vector",
			dataType: schemapb.DataType_BinaryVector,
			fieldData: &storage.BinaryVectorFieldData{
				Data: make([]byte, 2*dim/8),
				Dim:  dim,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "nullable_vector",
				DataType: tt.dataType,
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "8"},
				},
			}
			reader, err := createReader(tt.fieldData, tt.dataType)
			assert.NoError(t, err)

			fr, err := NewFieldReader(reader, field, common.DefaultTimezone)
			assert.NoError(t, err)
			data, validData, err := fr.Next(2)
			assert.NoError(t, err)
			assert.Len(t, validData.([]bool), 2)

			dst, err := storage.NewFieldData(tt.dataType, field, 0)
			assert.NoError(t, err)
			assert.NoError(t, dst.AppendRows(data, validData))
			assert.Equal(t, 2, dst.RowNum())
		})
	}
}

type ErrReader struct {
	Err error
}

func (r *ErrReader) Read(p []byte) (n int, err error) {
	if r.Err != nil {
		return 0, r.Err
	} else {
		return 1, nil
	}
}

func TestNumpyFieldReaderError(t *testing.T) {
	errReader := &ErrReader{
		Err: merr.WrapErrImportFailed("failed"),
	}

	// read header error
	_, err := NewFieldReader(errReader, &schemapb.FieldSchema{
		FieldID:    100,
		Name:       "str",
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
	}, common.DefaultTimezone)
	assert.Error(t, err)

	// read values error
	tests := []struct {
		name        string
		fieldSchema *schemapb.FieldSchema
		timezone    string
	}{
		{
			name: "read bool error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "bool",
				DataType: schemapb.DataType_Bool,
			},
		},
		{
			name: "read int8 error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "int8",
				DataType: schemapb.DataType_Int8,
			},
		},
		{
			name: "read int16 error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "int16",
				DataType: schemapb.DataType_Int16,
			},
		},
		{
			name: "read int32 error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "int32",
				DataType: schemapb.DataType_Int32,
			},
		},
		{
			name: "read int64 error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "int64",
				DataType: schemapb.DataType_Int64,
			},
		},
		{
			name: "read float error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "float",
				DataType: schemapb.DataType_Float,
			},
		},
		{
			name: "read double error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "double",
				DataType: schemapb.DataType_Double,
			},
		},
		{
			name: "read varchar error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:    100,
				Name:       "varchar",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "32"}},
			},
		},
		{
			name: "read json error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "json",
				DataType: schemapb.DataType_JSON,
			},
		},
		{
			name: "read geometry error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "geometry",
				DataType: schemapb.DataType_Geometry,
			},
		},
		{
			name: "read geometry error",
			fieldSchema: &schemapb.FieldSchema{
				FieldID:  100,
				Name:     "geometry",
				DataType: schemapb.DataType_Geometry,
			},
			timezone: "Asia/Shanghai",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{tt.fieldSchema},
			}
			insertData, err := testutil.CreateInsertData(schema, 3)
			assert.NoError(t, err)
			fieldData := insertData.Data[tt.fieldSchema.FieldID]
			reader, err := createReader(fieldData, tt.fieldSchema.DataType)
			assert.NoError(t, err)
			tz := common.DefaultTimezone
			if len(tt.timezone) > 0 {
				tz = tt.timezone
			}
			fieldReader, err := NewFieldReader(reader, tt.fieldSchema, tz)
			assert.NoError(t, err)

			fieldReader.reader = errReader

			data, validData, err := fieldReader.Next(5)
			assert.Error(t, err)
			assert.Nil(t, data)
			assert.Nil(t, validData)
		})
	}
}

func TestReadFP16BF16VectorFromFloatNumpy(t *testing.T) {
	const (
		fieldID  = int64(100)
		rowCount = 2
	)
	makeField := func(dataType schemapb.DataType) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			FieldID:  fieldID,
			Name:     "vector",
			DataType: dataType,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "8"},
			},
		}
	}

	t.Run("float32 source", func(t *testing.T) {
		sourceSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{makeField(schemapb.DataType_FloatVector)}}
		insertData, err := testutil.CreateInsertData(sourceSchema, rowCount)
		assert.NoError(t, err)
		floatData := append([]float32(nil), insertData.Data[fieldID].GetDataRows().([]float32)...)
		reader, err := createReader(insertData.Data[fieldID], schemapb.DataType_FloatVector)
		assert.NoError(t, err)

		for _, tt := range []struct {
			name     string
			dataType schemapb.DataType
			expected []byte
		}{
			{"float16", schemapb.DataType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes(floatData)},
			{"bfloat16", schemapb.DataType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes(floatData)},
		} {
			t.Run(tt.name, func(t *testing.T) {
				fr, err := NewFieldReader(reader, makeField(tt.dataType), common.DefaultTimezone)
				assert.NoError(t, err)
				data, validData, err := fr.Next(rowCount)
				assert.NoError(t, err)
				assert.Nil(t, validData)
				assert.Equal(t, tt.expected, data.([]byte))
			})
			reader, err = createReader(insertData.Data[fieldID], schemapb.DataType_FloatVector)
			assert.NoError(t, err)
		}
	})
	t.Run("float64 source", func(t *testing.T) {
		rows := [][dim]float64{
			{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
			{0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6},
		}
		floatData := make([]float32, 0, rowCount*dim)
		for _, row := range rows {
			for _, value := range row {
				floatData = append(floatData, float32(value))
			}
		}
		buf := new(bytes.Buffer)
		err := npyio.Write(buf, rows)
		assert.NoError(t, err)

		for _, tt := range []struct {
			name     string
			dataType schemapb.DataType
			expected []byte
		}{
			{"float16", schemapb.DataType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes(floatData)},
			{"bfloat16", schemapb.DataType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes(floatData)},
		} {
			t.Run(tt.name, func(t *testing.T) {
				fr, err := NewFieldReader(bytes.NewReader(buf.Bytes()), makeField(tt.dataType), common.DefaultTimezone)
				assert.NoError(t, err)
				data, validData, err := fr.Next(rowCount)
				assert.NoError(t, err)
				assert.Nil(t, validData)
				assert.Equal(t, tt.expected, data.([]byte))
			})
		}
	})

	t.Run("unsupported source type", func(t *testing.T) {
		sourceSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{makeField(schemapb.DataType_Int8Vector)}}
		insertData, err := testutil.CreateInsertData(sourceSchema, rowCount)
		assert.NoError(t, err)
		reader, err := createReader(insertData.Data[fieldID], schemapb.DataType_Int8Vector)
		assert.NoError(t, err)

		fr, err := NewFieldReader(reader, makeField(schemapb.DataType_Float16Vector), common.DefaultTimezone)
		assert.Error(t, err)
		assert.Nil(t, fr)
		assert.Contains(t, err.Error(), "expected element type")
	})
}

func TestNullableFP16BF16VectorFromFloatNumpyUsesLogicalRows(t *testing.T) {
	const (
		fieldID  = int64(100)
		rowCount = 2
	)
	makeField := func(dataType schemapb.DataType, nullable bool, defaultValue *schemapb.ValueField) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			FieldID:      fieldID,
			Name:         "vector",
			DataType:     dataType,
			Nullable:     nullable,
			DefaultValue: defaultValue,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "8"},
			},
		}
	}
	writeRows := func(t *testing.T, rows any) []byte {
		buf := new(bytes.Buffer)
		assert.NoError(t, npyio.Write(buf, rows))
		return buf.Bytes()
	}

	float32Rows := [][dim]float32{
		{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
		{0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6},
	}
	float64Rows := [][dim]float64{
		{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
		{0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6},
	}

	for _, tt := range []struct {
		name       string
		sourceData []byte
		field      *schemapb.FieldSchema
	}{
		{
			name:       "nullable float32 to float16",
			sourceData: writeRows(t, float32Rows),
			field:      makeField(schemapb.DataType_Float16Vector, true, nil),
		},
		{
			name:       "nullable float64 to float16",
			sourceData: writeRows(t, float64Rows),
			field:      makeField(schemapb.DataType_Float16Vector, true, nil),
		},
		{
			name:       "nullable float32 to bfloat16",
			sourceData: writeRows(t, float32Rows),
			field:      makeField(schemapb.DataType_BFloat16Vector, true, nil),
		},
		{
			name:       "nullable float64 to bfloat16",
			sourceData: writeRows(t, float64Rows),
			field:      makeField(schemapb.DataType_BFloat16Vector, true, nil),
		},
		{
			name:       "default float32 to float16",
			sourceData: writeRows(t, float32Rows),
			field:      makeField(schemapb.DataType_Float16Vector, false, &schemapb.ValueField{}),
		},
		{
			name:       "default float64 to bfloat16",
			sourceData: writeRows(t, float64Rows),
			field:      makeField(schemapb.DataType_BFloat16Vector, false, &schemapb.ValueField{}),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fr, err := NewFieldReader(bytes.NewReader(tt.sourceData), tt.field, common.DefaultTimezone)
			assert.NoError(t, err)

			data, validData, err := fr.Next(rowCount)

			assert.NoError(t, err)
			assert.Len(t, data.([]byte), rowCount*dim*2)
			assert.Equal(t, []bool{true, true}, validData.([]bool))
		})
	}
}

func TestNumpyValidateHeaderError(t *testing.T) {
	tests := []struct {
		name      string
		numpyType schemapb.DataType
		fieldType schemapb.DataType
	}{
		{
			name:      "bool numpy int8 field",
			numpyType: schemapb.DataType_Bool,
			fieldType: schemapb.DataType_Int8,
		},
		{
			name:      "bool numpy int16 field",
			numpyType: schemapb.DataType_Bool,
			fieldType: schemapb.DataType_Int16,
		},
		{
			name:      "float numpy int32 field",
			numpyType: schemapb.DataType_Float,
			fieldType: schemapb.DataType_Int32,
		},
		{
			name:      "double numpy int64 field",
			numpyType: schemapb.DataType_Double,
			fieldType: schemapb.DataType_Int64,
		},
		{
			name:      "int8 numpy float field",
			numpyType: schemapb.DataType_Int8,
			fieldType: schemapb.DataType_Float,
		},
		{
			name:      "bool numpy double field",
			numpyType: schemapb.DataType_Bool,
			fieldType: schemapb.DataType_Double,
		},
		{
			name:      "bool numpy varchar field",
			numpyType: schemapb.DataType_Bool,
			fieldType: schemapb.DataType_VarChar,
		},
		{
			name:      "bin vector numpy float vector field",
			numpyType: schemapb.DataType_BinaryVector,
			fieldType: schemapb.DataType_FloatVector,
		},
		{
			name:      "float vector numpy bin vector field",
			numpyType: schemapb.DataType_FloatVector,
			fieldType: schemapb.DataType_BinaryVector,
		},
		{
			name:      "bin vector numpy int8 vector field",
			numpyType: schemapb.DataType_BinaryVector,
			fieldType: schemapb.DataType_Int8Vector,
		},
		{
			name:      "int8 vector numpy float16 vector field",
			numpyType: schemapb.DataType_Int8Vector,
			fieldType: schemapb.DataType_Float16Vector,
		},
		{
			name:      "int8 vector dim error",
			numpyType: schemapb.DataType_Int8Vector,
			fieldType: schemapb.DataType_Int8Vector,
		},
		{
			name:      "bin vector dim error",
			numpyType: schemapb.DataType_BinaryVector,
			fieldType: schemapb.DataType_BinaryVector,
		},
		{
			name:      "float vector dim error",
			numpyType: schemapb.DataType_FloatVector,
			fieldType: schemapb.DataType_FloatVector,
		},
		{
			name:      "bfloat16 vector dim error",
			numpyType: schemapb.DataType_BFloat16Vector,
			fieldType: schemapb.DataType_BFloat16Vector,
		},
		{
			name:      "float vector shape error",
			numpyType: schemapb.DataType_Float,
			fieldType: schemapb.DataType_FloatVector,
		},
		{
			name:      "int8 vector shape error",
			numpyType: schemapb.DataType_Int8,
			fieldType: schemapb.DataType_Int8Vector,
		},
		{
			name:      "sparse vector not support",
			numpyType: schemapb.DataType_Int8,
			fieldType: schemapb.DataType_SparseFloatVector,
		},
	}

	fieldID := int64(100)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  fieldID,
						Name:     "dummy",
						DataType: tt.numpyType,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "max_length",
								Value: "256",
							},
							{
								Key:   "dim",
								Value: "16",
							},
						},
					},
				},
			}
			insertData, err := testutil.CreateInsertData(schema, 3)
			assert.NoError(t, err)
			fieldData := insertData.Data[fieldID]
			reader, err := createReader(fieldData, tt.numpyType)
			assert.NoError(t, err)

			schema.Fields[0].DataType = tt.fieldType
			schema.Fields[0].TypeParams[1].Value = "32"
			fieldReader, err := NewFieldReader(reader, schema.Fields[0], common.DefaultTimezone)
			assert.Error(t, err)
			assert.Nil(t, fieldReader)
		})
	}
}
