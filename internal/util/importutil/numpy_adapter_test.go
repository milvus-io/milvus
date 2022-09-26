package importutil

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

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

func Test_SetByteOrder(t *testing.T) {
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

func Test_ReadError(t *testing.T) {
	adapter := &NumpyAdapter{
		reader:    nil,
		npyReader: nil,
	}

	// reader is nil
	{
		_, err := adapter.ReadBool(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadUint8(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt8(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt16(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt32(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadInt64(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadFloat32(1)
		assert.NotNil(t, err)
		_, err = adapter.ReadFloat64(1)
		assert.NotNil(t, err)
	}

	adapter = &NumpyAdapter{
		reader:    &MockReader{},
		npyReader: &npy.Reader{},
	}

	{
		adapter.npyReader.Header.Descr.Type = "bool"
		data, err := adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadBool(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "u1"
		data, err := adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadUint8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "i1"
		data, err := adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt8(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "i2"
		data, err := adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt16(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "i4"
		data, err := adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "i8"
		data, err := adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadInt64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "f4"
		data, err := adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadFloat32(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}

	{
		adapter.npyReader.Header.Descr.Type = "f8"
		data, err := adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)

		adapter.npyReader.Header.Descr.Type = "dummy"
		data, err = adapter.ReadFloat64(1)
		assert.Nil(t, data)
		assert.NotNil(t, err)
	}
}

func Test_Read(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	{
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
	}

	{
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
	}

	{
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
	}

	{
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
	}

	{
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
	}

	{
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
	}

	{
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
	}

	{
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
	}
}
