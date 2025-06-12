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

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

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
	fr, err := NewFieldReader(reader, fieldSchema)
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
		fr, err := NewFieldReader(reader, fieldSchema)
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
	})
	assert.Error(t, err)

	// read values error
	tests := []struct {
		name        string
		fieldSchema *schemapb.FieldSchema
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
			fieldReader, err := NewFieldReader(reader, tt.fieldSchema)
			assert.NoError(t, err)

			fieldReader.reader = errReader

			data, validData, err := fieldReader.Next(5)
			assert.Error(t, err)
			assert.Nil(t, data)
			assert.Nil(t, validData)
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
			fieldReader, err := NewFieldReader(reader, schema.Fields[0])
			assert.Error(t, err)
			assert.Nil(t, fieldReader)
		})
	}
}
