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
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
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
	assert.NotNil(t, err)

	// invalid data type
	data2 := make(map[string]int)
	err = CreateNumpyFile("/tmp/dummy.npy", data2)
	assert.NotNil(t, err)
}

func Test_CreateNumpyData(t *testing.T) {
	// directory doesn't exist
	data1 := []float32{1, 2, 3, 4, 5}
	buf, err := CreateNumpyData(data1)
	assert.NotNil(t, buf)
	assert.Nil(t, err)

	// invalid data type
	data2 := make(map[string]int)
	buf, err = CreateNumpyData(data2)
	assert.NotNil(t, err)
	assert.Nil(t, buf)
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

func Test_StringLen(t *testing.T) {
	len, utf, err := stringLen("S1")
	assert.Equal(t, 1, len)
	assert.False(t, utf)
	assert.Nil(t, err)

	len, utf, err = stringLen("2S")
	assert.Equal(t, 2, len)
	assert.False(t, utf)
	assert.Nil(t, err)

	len, utf, err = stringLen("<U3")
	assert.Equal(t, 3, len)
	assert.True(t, utf)
	assert.Nil(t, err)

	len, utf, err = stringLen(">4U")
	assert.Equal(t, 4, len)
	assert.True(t, utf)
	assert.Nil(t, err)

	len, utf, err = stringLen("dummy")
	assert.NotNil(t, err)
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
	adapter := &NumpyAdapter{
		reader:    nil,
		npyReader: nil,
	}

	// reader size is zero
	t.Run("test size is zero", func(t *testing.T) {
		_, err := adapter.ReadBool(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadUint8(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt8(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt16(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt32(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt64(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadFloat32(0)
		assert.NotNil(t, err)
		_, err = adapter.ReadFloat64(0)
		assert.NotNil(t, err)
	})

	adapter = &NumpyAdapter{
		reader:    &MockReader{},
		npyReader: &npy.Reader{},
	}

	t.Run("test read bool", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "bool"
		data, err := adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read uint8", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "u1"
		data, err := adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read int8", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "i1"
		data, err := adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read int16", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "i2"
		data, err := adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read int32", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "i4"
		data, err := adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read int64", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "i8"
		data, err := adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read float", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "f4"
		data, err := adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read double", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "f8"
		data, err := adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})

	t.Run("test read varchar", func(t *testing.T) {
		adapter.npyReader.Header.Descr.Type = "U3"
		data, err := adapter.ReadString(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadString(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	})
}

func Test_NumpyAdapterRead(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	t.Run("test read bool", func(t *testing.T) {
		filePath := TempFilesPath + "bool.npy"
		data := []bool{true, false, true, false}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadBool(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadBool(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadBool(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)

		// incorrect type read
		resu1, err := adapter.ReadUint8(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resu1)

		resi1, err := adapter.ReadInt8(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resi1)

		resi2, err := adapter.ReadInt16(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resi2)

		resi4, err := adapter.ReadInt32(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resi4)

		resi8, err := adapter.ReadInt64(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resi8)

		resf4, err := adapter.ReadFloat32(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resf4)

		resf8, err := adapter.ReadFloat64(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resf8)
	})

	t.Run("test read uint8", func(t *testing.T) {
		filePath := TempFilesPath + "uint8.npy"
		data := []uint8{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadUint8(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadUint8(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadUint8(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)

		// incorrect type read
		resb, err := adapter.ReadBool(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, resb)
	})

	t.Run("test read int8", func(t *testing.T) {
		filePath := TempFilesPath + "int8.npy"
		data := []int8{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadInt8(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadInt8(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadInt8(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int16", func(t *testing.T) {
		filePath := TempFilesPath + "int16.npy"
		data := []int16{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadInt16(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadInt16(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadInt16(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int32", func(t *testing.T) {
		filePath := TempFilesPath + "int32.npy"
		data := []int32{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadInt32(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadInt32(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadInt32(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read int64", func(t *testing.T) {
		filePath := TempFilesPath + "int64.npy"
		data := []int64{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadInt64(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadInt64(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadInt64(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read float", func(t *testing.T) {
		filePath := TempFilesPath + "float.npy"
		data := []float32{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadFloat32(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadFloat32(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadFloat32(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read double", func(t *testing.T) {
		filePath := TempFilesPath + "double.npy"
		data := []float64{1, 2, 3, 4, 5, 6}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)

		res, err := adapter.ReadFloat64(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadFloat64(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadFloat64(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read ascii characters", func(t *testing.T) {
		filePath := TempFilesPath + "varchar1.npy"
		data := []string{"a", "bbb", "c", "dd", "eeee", "fff"}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)
		res, err := adapter.ReadString(len(data) - 1)
		assert.Nil(t, err)
		assert.Equal(t, len(data)-1, len(res))

		for i := 0; i < len(res); i++ {
			assert.Equal(t, data[i], res[i])
		}

		res, err = adapter.ReadString(len(data))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(res))
		assert.Equal(t, data[len(data)-1], res[0])

		res, err = adapter.ReadString(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("test read non-ascii", func(t *testing.T) {
		filePath := TempFilesPath + "varchar2.npy"
		data := []string{"a三百", "马克bbb"}
		err := CreateNumpyFile(filePath, data)
		assert.Nil(t, err)

		file, err := os.Open(filePath)
		assert.Nil(t, err)
		defer file.Close()

		adapter, err := NewNumpyAdapter(file)
		assert.Nil(t, err)
		res, err := adapter.ReadString(len(data))
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})
}
