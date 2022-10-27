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
	"os"
	"testing"

	"github.com/sbinet/npyio/npy"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

func Test_NewNumpyParser(t *testing.T) {
	ctx := context.Background()

	parser := NewNumpyParser(ctx, nil, nil)
	assert.Nil(t, parser)
}

func Test_NumpyParserValidate(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	schema := sampleSchema()
	flushFunc := func(field storage.FieldData) error {
		return nil
	}

	adapter := &NumpyAdapter{npyReader: &npy.Reader{}}

	t.Run("not support DataType_String", func(t *testing.T) {
		// string type is not supported
		p := NewNumpyParser(ctx, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      109,
					Name:         "field_string",
					IsPrimaryKey: false,
					Description:  "string",
					DataType:     schemapb.DataType_String,
				},
			},
		}, flushFunc)
		err = p.validate(adapter, "dummy")
		assert.NotNil(t, err)
		err = p.validate(adapter, "field_string")
		assert.NotNil(t, err)
	})

	// reader is nil
	parser := NewNumpyParser(ctx, schema, flushFunc)
	err = parser.validate(nil, "")
	assert.NotNil(t, err)

	t.Run("validate scalar", func(t *testing.T) {
		filePath := TempFilesPath + "scalar_1.npy"
		data1 := []float64{0, 1, 2, 3, 4, 5}
		err := CreateNumpyFile(filePath, data1)
		assert.Nil(t, err)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_double")
		assert.Nil(t, err)
		assert.Equal(t, len(data1), parser.columnDesc.elementCount)

		err = parser.validate(adapter, "")
		assert.NotNil(t, err)

		// data type mismatch
		filePath = TempFilesPath + "scalar_2.npy"
		data2 := []int64{0, 1, 2, 3, 4, 5}
		err = CreateNumpyFile(filePath, data2)
		assert.Nil(t, err)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_double")
		assert.NotNil(t, err)

		// shape mismatch
		filePath = TempFilesPath + "scalar_2.npy"
		data3 := [][2]float64{{1, 1}}
		err = CreateNumpyFile(filePath, data3)
		assert.Nil(t, err)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_double")
		assert.NotNil(t, err)
	})

	t.Run("validate binary vector", func(t *testing.T) {
		filePath := TempFilesPath + "binary_vector_1.npy"
		data1 := [][2]uint8{{0, 1}, {2, 3}, {4, 5}}
		err := CreateNumpyFile(filePath, data1)
		assert.Nil(t, err)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_binary_vector")
		assert.Nil(t, err)
		assert.Equal(t, len(data1)*len(data1[0]), parser.columnDesc.elementCount)

		// data type mismatch
		filePath = TempFilesPath + "binary_vector_2.npy"
		data2 := [][2]uint16{{0, 1}, {2, 3}, {4, 5}}
		err = CreateNumpyFile(filePath, data2)
		assert.Nil(t, err)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.NotNil(t, err)
		assert.Nil(t, adapter)

		// shape mismatch
		filePath = TempFilesPath + "binary_vector_3.npy"
		data3 := []uint8{1, 2, 3}
		err = CreateNumpyFile(filePath, data3)
		assert.Nil(t, err)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file3.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_binary_vector")
		assert.NotNil(t, err)

		// shape[1] mismatch
		filePath = TempFilesPath + "binary_vector_4.npy"
		data4 := [][3]uint8{{0, 1, 2}, {2, 3, 4}, {4, 5, 6}}
		err = CreateNumpyFile(filePath, data4)
		assert.Nil(t, err)

		file4, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file4.Close()

		adapter, err = NewNumpyAdapter(file4)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_binary_vector")
		assert.NotNil(t, err)

		// dimension mismatch
		p := NewNumpyParser(ctx, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  109,
					Name:     "field_binary_vector",
					DataType: schemapb.DataType_BinaryVector,
				},
			},
		}, flushFunc)

		err = p.validate(adapter, "field_binary_vector")
		assert.NotNil(t, err)
	})

	t.Run("validate float vector", func(t *testing.T) {
		filePath := TempFilesPath + "float_vector.npy"
		data1 := [][4]float32{{0, 0, 0, 0}, {1, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}}
		err := CreateNumpyFile(filePath, data1)
		assert.Nil(t, err)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_float_vector")
		assert.Nil(t, err)
		assert.Equal(t, len(data1)*len(data1[0]), parser.columnDesc.elementCount)

		// data type mismatch
		filePath = TempFilesPath + "float_vector_2.npy"
		data2 := [][4]int32{{0, 1, 2, 3}}
		err = CreateNumpyFile(filePath, data2)
		assert.Nil(t, err)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)

		// shape mismatch
		filePath = TempFilesPath + "float_vector_3.npy"
		data3 := []float32{1, 2, 3}
		err = CreateNumpyFile(filePath, data3)
		assert.Nil(t, err)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file3.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)

		// shape[1] mismatch
		filePath = TempFilesPath + "float_vector_4.npy"
		data4 := [][3]float32{{0, 0, 0}, {1, 1, 1}}
		err = CreateNumpyFile(filePath, data4)
		assert.Nil(t, err)

		file4, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file4.Close()

		adapter, err = NewNumpyAdapter(file4)
		assert.Nil(t, err)
		assert.NotNil(t, adapter)

		err = parser.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)

		// dimension mismatch
		p := NewNumpyParser(ctx, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  109,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
				},
			},
		}, flushFunc)

		err = p.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)
	})
}

func Test_NumpyParserParse(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	schema := sampleSchema()

	checkFunc := func(data interface{}, fieldName string, callback func(field storage.FieldData) error) {

		filePath := TempFilesPath + fieldName + ".npy"
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		func() {
			file, err := os.Open(filePath)
			assert.Nil(t, err)
			defer file.Close()

			parser := NewNumpyParser(ctx, schema, callback)
			err = parser.Parse(file, fieldName, false)
			assert.Nil(t, err)
		}()

		// validation failed
		func() {
			file, err := os.Open(filePath)
			assert.Nil(t, err)
			defer file.Close()

			parser := NewNumpyParser(ctx, schema, callback)
			err = parser.Parse(file, "dummy", false)
			assert.NotNil(t, err)
		}()

		// read data error
		func() {
			parser := NewNumpyParser(ctx, schema, callback)
			err = parser.Parse(&MockReader{}, fieldName, false)
			assert.NotNil(t, err)
		}()
	}

	t.Run("parse scalar bool", func(t *testing.T) {
		data := []bool{true, false, true, false, true}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_bool", flushFunc)
	})

	t.Run("parse scalar int8", func(t *testing.T) {
		data := []int8{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_int8", flushFunc)
	})

	t.Run("parse scalar int16", func(t *testing.T) {
		data := []int16{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_int16", flushFunc)
	})

	t.Run("parse scalar int32", func(t *testing.T) {
		data := []int32{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_int32", flushFunc)
	})

	t.Run("parse scalar int64", func(t *testing.T) {
		data := []int64{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_int64", flushFunc)
	})

	t.Run("parse scalar float", func(t *testing.T) {
		data := []float32{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_float", flushFunc)
	})

	t.Run("parse scalar double", func(t *testing.T) {
		data := []float64{1, 2, 3, 4, 5}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_double", flushFunc)
	})

	t.Run("parse scalar varchar", func(t *testing.T) {
		data := []string{"abcd", "sdb", "ok", "milvus"}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				assert.Equal(t, data[i], field.GetRow(i))
			}

			return nil
		}
		checkFunc(data, "field_string", flushFunc)
	})

	t.Run("parse binary vector", func(t *testing.T) {
		data := [][2]uint8{{1, 2}, {3, 4}, {5, 6}}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				row := field.GetRow(i).([]uint8)
				for k := 0; k < len(row); k++ {
					assert.Equal(t, data[i][k], row[k])
				}
			}

			return nil
		}
		checkFunc(data, "field_binary_vector", flushFunc)
	})

	t.Run("parse binary vector with float32", func(t *testing.T) {
		data := [][4]float32{{1.1, 2.1, 3.1, 4.1}, {5.2, 6.2, 7.2, 8.2}}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				row := field.GetRow(i).([]float32)
				for k := 0; k < len(row); k++ {
					assert.Equal(t, data[i][k], row[k])
				}
			}

			return nil
		}
		checkFunc(data, "field_float_vector", flushFunc)
	})

	t.Run("parse binary vector with float64", func(t *testing.T) {
		data := [][4]float64{{1.1, 2.1, 3.1, 4.1}, {5.2, 6.2, 7.2, 8.2}}
		flushFunc := func(field storage.FieldData) error {
			assert.NotNil(t, field)
			assert.Equal(t, len(data), field.RowNum())

			for i := 0; i < len(data); i++ {
				row := field.GetRow(i).([]float32)
				for k := 0; k < len(row); k++ {
					assert.Equal(t, float32(data[i][k]), row[k])
				}
			}

			return nil
		}
		checkFunc(data, "field_float_vector", flushFunc)
	})
}

func Test_NumpyParserParse_perf(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	tr := timerecord.NewTimeRecorder("numpy parse performance")

	// change the parameter to test performance
	rowCount := 10000
	dotValue := float32(3.1415926)
	const (
		dim = 128
	)

	schema := perfSchema(dim)

	data := make([][dim]float32, 0)
	for i := 0; i < rowCount; i++ {
		var row [dim]float32
		for k := 0; k < dim; k++ {
			row[k] = float32(i) + dotValue
		}
		data = append(data, row)
	}

	tr.Record("generate large data")

	flushFunc := func(field storage.FieldData) error {
		assert.Equal(t, len(data), field.RowNum())
		return nil
	}

	filePath := TempFilesPath + "perf.npy"
	err = CreateNumpyFile(filePath, data)
	assert.Nil(t, err)

	tr.Record("generate large numpy file " + filePath)

	file, err := os.Open(filePath)
	assert.Nil(t, err)
	defer file.Close()

	parser := NewNumpyParser(ctx, schema, flushFunc)
	err = parser.Parse(file, "Vector", false)
	assert.Nil(t, err)

	tr.Record("parse large numpy files: " + filePath)
}
