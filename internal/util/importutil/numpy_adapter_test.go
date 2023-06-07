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
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/sbinet/npyio/npy"
	"github.com/stretchr/testify/assert"
)

type MockReader struct {
}

func (r *MockReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func Test_CreateNumpyFile(t *testing.T) {
	// directory doesn't exist
	data1 := []float32{1, 2, 3, 4, 5}
	err := CreateNumpyFile("/dummy_not_exist/dummy.npy", data1)
	assert.Error(t, err)

	// invalid data type
	data2 := make(map[string]int)
	err = CreateNumpyFile("/tmp/dummy.npy", data2)
	assert.Error(t, err)
}

func Test_CreateNumpyData(t *testing.T) {
	// directory doesn't exist
	data1 := []float32{1, 2, 3, 4, 5}
	buf, err := CreateNumpyData(data1)
	assert.NotNil(t, buf)
	assert.NoError(t, err)

	// invalid data type
	data2 := make(map[string]int)
	buf, err = CreateNumpyData(data2)
	assert.Error(t, err)
	assert.Nil(t, buf)
}

func Test_ConvertNumpyType(t *testing.T) {
	checkFunc := func(inputs []string, output schemapb.DataType) {
		for i := 0; i < len(inputs); i++ {
			dt, err := convertNumpyType(inputs[i])
			assert.NoError(t, err)
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
	assert.Error(t, err)
	assert.Equal(t, schemapb.DataType_None, dt)
}

func Test_StringLen(t *testing.T) {
	len, utf, err := stringLen("S1")
	assert.Equal(t, 1, len)
	assert.False(t, utf)
	assert.NoError(t, err)

	len, utf, err = stringLen("2S")
	assert.Equal(t, 2, len)
	assert.False(t, utf)
	assert.NoError(t, err)

	len, utf, err = stringLen("<U3")
	assert.Equal(t, 3, len)
	assert.True(t, utf)
	assert.NoError(t, err)

	len, utf, err = stringLen(">4U")
	assert.Equal(t, 4, len)
	assert.True(t, utf)
	assert.NoError(t, err)

	len, utf, err = stringLen("dummy")
	assert.Error(t, err)
	assert.Equal(t, 0, len)
	assert.False(t, utf)
}

func Test_NumpyAdapterSetByteOrder(t *testing.T) {
	adapter := &NumpyAdapter{
		reader:    nil,
		npyReader: &npy.Reader{},
	}
	assert.Nil(t, adapter.Reader())
	assert.NotNil(t, adapter.NpyReader())

	adapter.npyReader.Header.Descr.Type = "<i8"
	adapter.setByteOrder()
	assert.Equal(t, binary.LittleEndian, adapter.order)

	adapter.npyReader.Header.Descr.Type = ">i8"
	adapter.setByteOrder()
	assert.Equal(t, binary.BigEndian, adapter.order)
}

func Test_NumpyAdapterReadError(t *testing.T) {
	// reader size is zero
	// t.Run("test size is zero", func(t *testing.T) {
	// 	adapter := &NumpyAdapter{
	// 		reader:    nil,
	// 		npyReader: nil,
	// 	}
	// 	_, err := adapter.ReadBool(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadUint8(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadInt8(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadInt16(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadInt32(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadInt64(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadFloat32(0)
	// 	assert.Error(t, err)
	// 	_, err = adapter.ReadFloat64(0)
	// 	assert.Error(t, err)
	// })

	createAdatper := func(dt schemapb.DataType) *NumpyAdapter {
		adapter := &NumpyAdapter{
			reader: &MockReader{},
			npyReader: &npy.Reader{
				Header: npy.Header{},
			},
			dataType: dt,
			order:    binary.BigEndian,
		}
		adapter.npyReader.Header.Descr.Shape = []int{1}
		return adapter
	}

	t.Run("test read bool", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Bool)
		data, err = adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1})
		data, err = adapter.ReadBool(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read uint8", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err := adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter.npyReader.Header.Descr.Type = "u1"
		data, err = adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1})
		data, err = adapter.ReadUint8(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read int8", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Int8)
		data, err = adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1})
		data, err = adapter.ReadInt8(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read int16", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Int16)
		data, err = adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1, 2})
		data, err = adapter.ReadInt16(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read int32", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Int32)
		data, err = adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1, 2, 3, 4})
		data, err = adapter.ReadInt32(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read int64", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Int64)
		data, err = adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8})
		data, err = adapter.ReadInt64(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read float", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Float)
		data, err = adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1, 2, 3, 4})
		data, err = adapter.ReadFloat32(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read double", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_Double)
		data, err = adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8})
		data, err = adapter.ReadFloat64(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})

	t.Run("test read varchar", func(t *testing.T) {
		// type mismatch
		adapter := createAdatper(schemapb.DataType_None)
		data, err := adapter.ReadString(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// reader is nil, cannot read
		adapter = createAdatper(schemapb.DataType_VarChar)
		adapter.reader = nil
		adapter.npyReader.Header.Descr.Type = "S3"
		data, err = adapter.ReadString(1)
		assert.Nil(t, data)
		assert.Error(t, err)

		// read one element from reader
		adapter.reader = strings.NewReader("abc")
		data, err = adapter.ReadString(1)
		assert.NotEmpty(t, data)
		assert.NoError(t, err)

		// nothing to read
		data, err = adapter.ReadString(1)
		assert.Nil(t, data)
		assert.NoError(t, err)
	})
}

func Test_NumpyAdapterRead(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	t.Run("test read bool", func(t *testing.T) {
		filePath := TempFilesPath + "bool.npy"
		data := []bool{true, false, true, false}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadBool(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadBool(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadBool(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)

		// incorrect type read
		resu1, err := adapter.ReadUint8(len(data))
		assert.Error(t, err)
		assert.Nil(t, resu1)

		resi1, err := adapter.ReadInt8(len(data))
		assert.Error(t, err)
		assert.Nil(t, resi1)

		resi2, err := adapter.ReadInt16(len(data))
		assert.Error(t, err)
		assert.Nil(t, resi2)

		resi4, err := adapter.ReadInt32(len(data))
		assert.Error(t, err)
		assert.Nil(t, resi4)

		resi8, err := adapter.ReadInt64(len(data))
		assert.Error(t, err)
		assert.Nil(t, resi8)

		resf4, err := adapter.ReadFloat32(len(data))
		assert.Error(t, err)
		assert.Nil(t, resf4)

		resf8, err := adapter.ReadFloat64(len(data))
		assert.Error(t, err)
		assert.Nil(t, resf8)
	})

	t.Run("test read uint8", func(t *testing.T) {
		filePath := TempFilesPath + "uint8.npy"
		data := []uint8{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadUint8(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadUint8(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadUint8(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)

		// incorrect type read
		resb, err := adapter.ReadBool(len(data))
		assert.Error(t, err)
		assert.Nil(t, resb)
	})

	t.Run("test read int8", func(t *testing.T) {
		filePath := TempFilesPath + "int8.npy"
		data := []int8{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadInt8(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadInt8(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadInt8(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int16", func(t *testing.T) {
		filePath := TempFilesPath + "int16.npy"
		data := []int16{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadInt16(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadInt16(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadInt16(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int32", func(t *testing.T) {
		filePath := TempFilesPath + "int32.npy"
		data := []int32{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadInt32(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadInt32(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadInt32(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int64", func(t *testing.T) {
		filePath := TempFilesPath + "int64.npy"
		data := []int64{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadInt64(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadInt64(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadInt64(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read float", func(t *testing.T) {
		filePath := TempFilesPath + "float.npy"
		data := []float32{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadFloat32(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadFloat32(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadFloat32(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read double", func(t *testing.T) {
		filePath := TempFilesPath + "double.npy"
		data := []float64{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadFloat64(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadFloat64(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadFloat64(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read ascii characters with ansi", func(t *testing.T) {
		npyReader := &npy.Reader{
			Header: npy.Header{},
		}

		data := make([]byte, 0)
		values := []string{"ab", "ccc", "d"}
		maxLen := 0
		for _, str := range values {
			if len(str) > maxLen {
				maxLen = len(str)
			}
		}
		for _, str := range values {
			for i := 0; i < maxLen; i++ {
				if i < len(str) {
					data = append(data, str[i])
				} else {
					data = append(data, 0)
				}
			}
		}

		npyReader.Header.Descr.Shape = append(npyReader.Header.Descr.Shape, len(values))

		adapter := &NumpyAdapter{
			reader:       strings.NewReader(string(data)),
			npyReader:    npyReader,
			readPosition: 0,
			dataType:     schemapb.DataType_VarChar,
		}

		// count should greater than 0
		res, err := adapter.ReadString(0)
		assert.Error(t, err)
		assert.Nil(t, res)

		// maxLen is zero
		npyReader.Header.Descr.Type = "S0"
		res, err = adapter.ReadString(1)
		assert.Error(t, err)
		assert.Nil(t, res)

		npyReader.Header.Descr.Type = "S" + strconv.FormatInt(int64(maxLen), 10)

		res, err = adapter.ReadString(len(values) + 1)
		assert.NoError(t, err)
		assert.Equal(t, len(values), len(res))
		for i := 0; i < len(res); i++ {
			assert.Equal(t, values[i], res[i])
		}
	})

	t.Run("test read ascii characters with utf32", func(t *testing.T) {
		filePath := TempFilesPath + "varchar1.npy"
		data := []string{"a ", "bbb", " c", "dd", "eeee", "fff"}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)

		// partly read
		res, err := adapter.ReadString(len(data) - 1)
		assert.NoError(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		// read the left data
		res, err = adapter.ReadString(len(data))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		// nothing to read
		res, err = adapter.ReadString(len(data))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read non-ascii characters with utf32", func(t *testing.T) {
		filePath := TempFilesPath + "varchar2.npy"
		data := []string{"で と ど ", " 马克bbb", "$(한)삼각*"}
		err := CreateNumpyFile(filePath, data)
		assert.NoError(t, err)

		file, err := os.Open(filePath)
		assert.NoError(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.NoError(t, err)
		res, err := adapter.ReadString(len(data))
		assert.NoError(t, err)
		assert.Equal(t, len(data), len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}
	})
}

func Test_DecodeUtf32(t *testing.T) {
	// wrong input
	res, err := decodeUtf32([]byte{1, 2}, binary.LittleEndian)
	assert.Error(t, err)
	assert.Empty(t, res)

	// this string contains ascii characters and unicode characters
	str := "ad◤三百🎵ゐ↙"

	// utf32 littleEndian of str
	src := []byte{97, 0, 0, 0, 100, 0, 0, 0, 228, 37, 0, 0, 9, 78, 0, 0, 126, 118, 0, 0, 181, 243, 1, 0, 144, 48, 0, 0, 153, 33, 0, 0}
	res, err = decodeUtf32(src, binary.LittleEndian)
	assert.NoError(t, err)
	assert.Equal(t, str, res)

	// utf32 bigEndian of str
	src = []byte{0, 0, 0, 97, 0, 0, 0, 100, 0, 0, 37, 228, 0, 0, 78, 9, 0, 0, 118, 126, 0, 1, 243, 181, 0, 0, 48, 144, 0, 0, 33, 153}
	res, err = decodeUtf32(src, binary.BigEndian)
	assert.NoError(t, err)
	assert.Equal(t, str, res)
}
