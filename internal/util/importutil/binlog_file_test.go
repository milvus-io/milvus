// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package importutil

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func createBinlogBuf(t *testing.T, dataType schemapb.DataType, data interface{}) []byte {
	w := storage.NewInsertBinlogWriter(dataType, 10, 20, 30, 40)
	assert.NotNil(t, w)
	defer w.Close()

	dim := 0
	if dataType == schemapb.DataType_BinaryVector {
		vectors := data.([][]byte)
		if len(vectors) > 0 {
			dim = len(vectors[0]) * 8
		}
	} else if dataType == schemapb.DataType_FloatVector {
		vectors := data.([][]float32)
		if len(vectors) > 0 {
			dim = len(vectors[0])
		}
	}

	evt, err := w.NextInsertEventWriter(dim)
	assert.Nil(t, err)
	assert.NotNil(t, evt)

	evt.SetEventTimestamp(100, 200)
	w.SetEventTimeStamp(1000, 2000)

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.([]bool))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]bool))
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.([]int8))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]int8))
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.([]int16))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]int16)) * 2
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.([]int32))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]int32)) * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.([]int64))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]int64)) * 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.([]float32))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]float32)) * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.([]float64))
		assert.Nil(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]float64)) * 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_VarChar:
		values := data.([]string)
		sizeTotal := 0
		for _, val := range values {
			err = evt.AddOneStringToPayload(val)
			assert.Nil(t, err)
			sizeTotal += binary.Size(val)
		}
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_BinaryVector:
		vectors := data.([][]byte)
		for i := 0; i < len(vectors); i++ {
			err = evt.AddBinaryVectorToPayload(vectors[i], dim)
			assert.Nil(t, err)
		}
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(vectors) * dim / 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_FloatVector:
		vectors := data.([][]float32)
		for i := 0; i < len(vectors); i++ {
			err = evt.AddFloatVectorToPayload(vectors[i], dim)
			assert.Nil(t, err)
		}
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(vectors) * dim * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	default:
		assert.True(t, false)
		return nil
	}

	err = w.Finish()
	assert.Nil(t, err)

	buf, err := w.GetBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, buf)

	return buf
}

func Test_NewBinlogFile(t *testing.T) {
	// nil chunkManager
	file, err := NewBinlogFile(nil)
	assert.NotNil(t, err)
	assert.Nil(t, file)

	// succeed
	file, err = NewBinlogFile(&MockChunkManager{})
	assert.Nil(t, err)
	assert.NotNil(t, file)
}

func Test_BinlogFileOpen(t *testing.T) {
	chunkManager := &MockChunkManager{
		readBuf: nil,
		readErr: nil,
	}

	// read succeed
	chunkManager.readBuf = map[string][]byte{
		"dummy": createBinlogBuf(t, schemapb.DataType_Bool, []bool{true}),
	}
	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile.reader)

	dt := binlogFile.DataType()
	assert.Equal(t, schemapb.DataType_Bool, dt)

	// failed to read
	err = binlogFile.Open("")
	assert.NotNil(t, err)

	chunkManager.readErr = errors.New("error")
	err = binlogFile.Open("dummy")
	assert.NotNil(t, err)

	// failed to create new BinlogReader
	chunkManager.readBuf["dummy"] = []byte{}
	chunkManager.readErr = nil
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.NotNil(t, err)
	assert.Nil(t, binlogFile.reader)

	dt = binlogFile.DataType()
	assert.Equal(t, schemapb.DataType_None, dt)

	// nil reader protect
	dataBool, err := binlogFile.ReadBool()
	assert.Nil(t, dataBool)
	assert.NotNil(t, err)

	dataInt8, err := binlogFile.ReadInt8()
	assert.Nil(t, dataInt8)
	assert.NotNil(t, err)

	dataInt16, err := binlogFile.ReadInt16()
	assert.Nil(t, dataInt16)
	assert.NotNil(t, err)

	dataInt32, err := binlogFile.ReadInt32()
	assert.Nil(t, dataInt32)
	assert.NotNil(t, err)

	dataInt64, err := binlogFile.ReadInt64()
	assert.Nil(t, dataInt64)
	assert.NotNil(t, err)

	dataFloat, err := binlogFile.ReadFloat()
	assert.Nil(t, dataFloat)
	assert.NotNil(t, err)

	dataDouble, err := binlogFile.ReadDouble()
	assert.Nil(t, dataDouble)
	assert.NotNil(t, err)

	dataVarchar, err := binlogFile.ReadVarchar()
	assert.Nil(t, dataVarchar)
	assert.NotNil(t, err)

	dataBinaryVector, dim, err := binlogFile.ReadBinaryVector()
	assert.Nil(t, dataBinaryVector)
	assert.Equal(t, 0, dim)
	assert.NotNil(t, err)

	dataFloatVector, dim, err := binlogFile.ReadFloatVector()
	assert.Nil(t, dataFloatVector)
	assert.Equal(t, 0, dim)
	assert.NotNil(t, err)
}

func Test_BinlogFileBool(t *testing.T) {
	source := []bool{true, false, true, false}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Bool, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Bool, binlogFile.DataType())

	data, err := binlogFile.ReadBool()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadInt8()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadBool()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileInt8(t *testing.T) {
	source := []int8{2, 4, 6, 8}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Int8, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Int8, binlogFile.DataType())

	data, err := binlogFile.ReadInt8()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadInt16()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadInt8()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileInt16(t *testing.T) {
	source := []int16{2, 4, 6, 8}

	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Int16, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Int16, binlogFile.DataType())

	data, err := binlogFile.ReadInt16()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadInt32()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadInt16()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileInt32(t *testing.T) {
	source := []int32{2, 4, 6, 8}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Int32, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Int32, binlogFile.DataType())

	data, err := binlogFile.ReadInt32()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadInt64()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadInt32()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileInt64(t *testing.T) {
	source := []int64{2, 4, 6, 8}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Int64, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Int64, binlogFile.DataType())

	data, err := binlogFile.ReadInt64()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadFloat()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadInt64()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileFloat(t *testing.T) {
	source := []float32{2, 4, 6, 8}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Float, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Float, binlogFile.DataType())

	data, err := binlogFile.ReadFloat()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadDouble()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadFloat()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileDouble(t *testing.T) {
	source := []float64{2, 4, 6, 8}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_Double, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_Double, binlogFile.DataType())

	data, err := binlogFile.ReadDouble()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, err := binlogFile.ReadVarchar()
	assert.Zero(t, len(d))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, err = binlogFile.ReadDouble()
	assert.Zero(t, len(data))
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileVarchar(t *testing.T) {
	source := []string{"a", "bb", "罗伯特", "d"}
	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_VarChar, source),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_VarChar, binlogFile.DataType())

	data, err := binlogFile.ReadVarchar()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, len(source), len(data))
	for i := 0; i < len(source); i++ {
		assert.Equal(t, source[i], data[i])
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	d, dim, err := binlogFile.ReadBinaryVector()
	assert.Zero(t, len(d))
	assert.Zero(t, dim)
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileBinaryVector(t *testing.T) {
	vectors := make([][]byte, 0)
	vectors = append(vectors, []byte{1, 3, 5, 7})
	vectors = append(vectors, []byte{2, 4, 6, 8})
	dim := len(vectors[0]) * 8
	vecCount := len(vectors)

	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_BinaryVector, vectors),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_BinaryVector, binlogFile.DataType())

	data, d, err := binlogFile.ReadBinaryVector()
	assert.Nil(t, err)
	assert.Equal(t, dim, d)
	assert.NotNil(t, data)
	assert.Equal(t, vecCount*dim/8, len(data))
	for i := 0; i < vecCount; i++ {
		for j := 0; j < dim/8; j++ {
			assert.Equal(t, vectors[i][j], data[i*dim/8+j])
		}
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	dt, d, err := binlogFile.ReadFloatVector()
	assert.Zero(t, len(dt))
	assert.Zero(t, d)
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, d, err = binlogFile.ReadBinaryVector()
	assert.Zero(t, len(data))
	assert.Zero(t, d)
	assert.NotNil(t, err)

	binlogFile.Close()
}

func Test_BinlogFileFloatVector(t *testing.T) {
	vectors := make([][]float32, 0)
	vectors = append(vectors, []float32{1, 3, 5, 7})
	vectors = append(vectors, []float32{2, 4, 6, 8})
	dim := len(vectors[0])
	vecCount := len(vectors)

	chunkManager := &MockChunkManager{
		readBuf: map[string][]byte{
			"dummy": createBinlogBuf(t, schemapb.DataType_FloatVector, vectors),
		},
	}

	binlogFile, err := NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	assert.NotNil(t, binlogFile)

	// correct reading
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)
	assert.Equal(t, schemapb.DataType_FloatVector, binlogFile.DataType())

	data, d, err := binlogFile.ReadFloatVector()
	assert.Nil(t, err)
	assert.Equal(t, dim, d)
	assert.NotNil(t, data)
	assert.Equal(t, vecCount*dim, len(data))
	for i := 0; i < vecCount; i++ {
		for j := 0; j < dim; j++ {
			assert.Equal(t, vectors[i][j], data[i*dim+j])
		}
	}

	binlogFile.Close()

	// wrong data type reading
	binlogFile, err = NewBinlogFile(chunkManager)
	assert.Nil(t, err)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	dt, err := binlogFile.ReadBool()
	assert.Zero(t, len(dt))
	assert.NotNil(t, err)

	binlogFile.Close()

	// wrong log type
	chunkManager.readBuf["dummy"] = createDeltalogBuf(t, []int64{1}, false)
	err = binlogFile.Open("dummy")
	assert.Nil(t, err)

	data, d, err = binlogFile.ReadFloatVector()
	assert.Zero(t, len(data))
	assert.Zero(t, d)
	assert.NotNil(t, err)

	binlogFile.Close()
}
