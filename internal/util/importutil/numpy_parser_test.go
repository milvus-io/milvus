package importutil

import (
	"context"
	"os"
	"testing"

	"github.com/sbinet/npyio/npy"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

func Test_NewNumpyParser(t *testing.T) {
	ctx := context.Background()

	parser := NewNumpyParser(ctx, nil, nil)
	assert.Nil(t, parser)
}

func Test_ConvertNumpyType(t *testing.T) {
	checkFunc := func(inputs []string, output schemapb.DataType) {
		for i := 0; i < len(inputs); i++ {
			dt, err := convertNumpyType(inputs[i])
			assert.Nil(t, err)
			assert.Equal(t, output, dt)
		}
	}

	checkFunc([]string{"b1", "<b1", "|b1", "bool"}, schemapb.DataType_Bool)
	checkFunc([]string{"i1", "<i1", "|i1", ">i1", "int8"}, schemapb.DataType_Int8)
	checkFunc([]string{"i2", "<i2", "|i2", ">i2", "int16"}, schemapb.DataType_Int16)
	checkFunc([]string{"i4", "<i4", "|i4", ">i4", "int32"}, schemapb.DataType_Int32)
	checkFunc([]string{"i8", "<i8", "|i8", ">i8", "int64"}, schemapb.DataType_Int64)
	checkFunc([]string{"f4", "<f4", "|f4", ">f4", "float32"}, schemapb.DataType_Float)
	checkFunc([]string{"f8", "<f8", "|f8", ">f8", "float64"}, schemapb.DataType_Double)

	dt, err := convertNumpyType("dummy")
	assert.NotNil(t, err)
	assert.Equal(t, schemapb.DataType_None, dt)
}

func Test_Validate(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	schema := sampleSchema()
	flushFunc := func(field storage.FieldData) error {
		return nil
	}

	adapter := &NumpyAdapter{npyReader: &npy.Reader{}}

	{
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
	}

	// reader is nil
	parser := NewNumpyParser(ctx, schema, flushFunc)
	err = parser.validate(nil, "")
	assert.NotNil(t, err)

	// validate scalar data
	func() {
		filePath := TempFilesPath + "scalar_1.npy"
		data1 := []float64{0, 1, 2, 3, 4, 5}
		CreateNumpyFile(filePath, data1)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_double")
		assert.Nil(t, err)
		assert.Equal(t, len(data1), parser.columnDesc.elementCount)

		err = parser.validate(adapter, "")
		assert.NotNil(t, err)

		// data type mismatch
		filePath = TempFilesPath + "scalar_2.npy"
		data2 := []int64{0, 1, 2, 3, 4, 5}
		CreateNumpyFile(filePath, data2)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_double")
		assert.NotNil(t, err)

		// shape mismatch
		filePath = TempFilesPath + "scalar_2.npy"
		data3 := [][2]float64{{1, 1}}
		CreateNumpyFile(filePath, data3)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_double")
		assert.NotNil(t, err)
	}()

	// validate binary vector data
	func() {
		filePath := TempFilesPath + "binary_vector_1.npy"
		data1 := [][2]uint8{{0, 1}, {2, 3}, {4, 5}}
		CreateNumpyFile(filePath, data1)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_binary_vector")
		assert.Nil(t, err)
		assert.Equal(t, len(data1)*len(data1[0]), parser.columnDesc.elementCount)

		// data type mismatch
		filePath = TempFilesPath + "binary_vector_2.npy"
		data2 := [][2]uint16{{0, 1}, {2, 3}, {4, 5}}
		CreateNumpyFile(filePath, data2)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_binary_vector")
		assert.NotNil(t, err)

		// shape mismatch
		filePath = TempFilesPath + "binary_vector_3.npy"
		data3 := []uint8{1, 2, 3}
		CreateNumpyFile(filePath, data3)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file3.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_binary_vector")
		assert.NotNil(t, err)

		// shape[1] mismatch
		filePath = TempFilesPath + "binary_vector_4.npy"
		data4 := [][3]uint8{{0, 1, 2}, {2, 3, 4}, {4, 5, 6}}
		CreateNumpyFile(filePath, data4)

		file4, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file4.Close()

		adapter, err = NewNumpyAdapter(file4)
		assert.Nil(t, err)

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
	}()

	// validate float vector data
	func() {
		filePath := TempFilesPath + "float_vector.npy"
		data1 := [][4]float32{{0, 0, 0, 0}, {1, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}}
		CreateNumpyFile(filePath, data1)

		file1, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file1.Close()

		adapter, err := NewNumpyAdapter(file1)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_float_vector")
		assert.Nil(t, err)
		assert.Equal(t, len(data1)*len(data1[0]), parser.columnDesc.elementCount)

		// data type mismatch
		filePath = TempFilesPath + "float_vector_2.npy"
		data2 := [][4]int32{{0, 1, 2, 3}}
		CreateNumpyFile(filePath, data2)

		file2, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file2.Close()

		adapter, err = NewNumpyAdapter(file2)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)

		// shape mismatch
		filePath = TempFilesPath + "float_vector_3.npy"
		data3 := []float32{1, 2, 3}
		CreateNumpyFile(filePath, data3)

		file3, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file3.Close()

		adapter, err = NewNumpyAdapter(file3)
		assert.Nil(t, err)

		err = parser.validate(adapter, "field_float_vector")
		assert.NotNil(t, err)

		// shape[1] mismatch
		filePath = TempFilesPath + "float_vector_4.npy"
		data4 := [][3]float32{{0, 0, 0}, {1, 1, 1}}
		CreateNumpyFile(filePath, data4)

		file4, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file4.Close()

		adapter, err = NewNumpyAdapter(file4)
		assert.Nil(t, err)

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
	}()
}

func Test_Parse(t *testing.T) {
	ctx := context.Background()
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	schema := sampleSchema()

	checkFunc := func(data interface{}, fieldName string, callback func(field storage.FieldData) error) {

		filePath := TempFilesPath + fieldName + ".npy"
		CreateNumpyFile(filePath, data)

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

	// scalar bool
	data1 := []bool{true, false, true, false, true}
	flushFunc := func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data1), field.RowNum())

		for i := 0; i < len(data1); i++ {
			assert.Equal(t, data1[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data1, "field_bool", flushFunc)

	// scalar int8
	data2 := []int8{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data2), field.RowNum())

		for i := 0; i < len(data2); i++ {
			assert.Equal(t, data2[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data2, "field_int8", flushFunc)

	// scalar int16
	data3 := []int16{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data3), field.RowNum())

		for i := 0; i < len(data3); i++ {
			assert.Equal(t, data3[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data3, "field_int16", flushFunc)

	// scalar int32
	data4 := []int32{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data4), field.RowNum())

		for i := 0; i < len(data4); i++ {
			assert.Equal(t, data4[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data4, "field_int32", flushFunc)

	// scalar int64
	data5 := []int64{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data5), field.RowNum())

		for i := 0; i < len(data5); i++ {
			assert.Equal(t, data5[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data5, "field_int64", flushFunc)

	// scalar float
	data6 := []float32{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data6), field.RowNum())

		for i := 0; i < len(data6); i++ {
			assert.Equal(t, data6[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data6, "field_float", flushFunc)

	// scalar double
	data7 := []float64{1, 2, 3, 4, 5}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data7), field.RowNum())

		for i := 0; i < len(data7); i++ {
			assert.Equal(t, data7[i], field.GetRow(i))
		}

		return nil
	}
	checkFunc(data7, "field_double", flushFunc)

	// binary vector
	data8 := [][2]uint8{{1, 2}, {3, 4}, {5, 6}}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data8), field.RowNum())

		for i := 0; i < len(data8); i++ {
			row := field.GetRow(i).([]uint8)
			for k := 0; k < len(row); k++ {
				assert.Equal(t, data8[i][k], row[k])
			}
		}

		return nil
	}
	checkFunc(data8, "field_binary_vector", flushFunc)

	// double vector(element can be float32 or float64)
	data9 := [][4]float32{{1.1, 2.1, 3.1, 4.1}, {5.2, 6.2, 7.2, 8.2}}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data9), field.RowNum())

		for i := 0; i < len(data9); i++ {
			row := field.GetRow(i).([]float32)
			for k := 0; k < len(row); k++ {
				assert.Equal(t, data9[i][k], row[k])
			}
		}

		return nil
	}
	checkFunc(data9, "field_float_vector", flushFunc)

	data10 := [][4]float64{{1.1, 2.1, 3.1, 4.1}, {5.2, 6.2, 7.2, 8.2}}
	flushFunc = func(field storage.FieldData) error {
		assert.NotNil(t, field)
		assert.Equal(t, len(data10), field.RowNum())

		for i := 0; i < len(data10); i++ {
			row := field.GetRow(i).([]float32)
			for k := 0; k < len(row); k++ {
				assert.Equal(t, float32(data10[i][k]), row[k])
			}
		}

		return nil
	}
	checkFunc(data10, "field_float_vector", flushFunc)
}

func Test_Parse_perf(t *testing.T) {
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
	CreateNumpyFile(filePath, data)

	tr.Record("generate large numpy file " + filePath)

	file, err := os.Open(filePath)
	assert.Nil(t, err)
	defer file.Close()

	parser := NewNumpyParser(ctx, schema, flushFunc)
	err = parser.Parse(file, "Vector", false)
	assert.Nil(t, err)

	tr.Record("parse large numpy files: " + filePath)
}
