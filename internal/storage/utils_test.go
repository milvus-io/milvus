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

package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/kv"

	"github.com/stretchr/testify/assert"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
)

type mockLessHeaderDataKV struct {
	kv.BaseKV
}

func (kv *mockLessHeaderDataKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	header := &baseEventHeader{}

	headerSize := binary.Size(header)
	mockSize := headerSize - 1

	ret := make([]byte, mockSize)
	_, _ = rand.Read(ret)
	return ret, nil
}

func (kv *mockLessHeaderDataKV) GetSize(key string) (int64, error) {
	return 0, errors.New("less header")
}

func newMockLessHeaderDataKV() *mockLessHeaderDataKV {
	return &mockLessHeaderDataKV{}
}

type mockWrongHeaderDataKV struct {
	kv.BaseKV
}

func (kv *mockWrongHeaderDataKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	header := &baseEventHeader{}

	header.EventLength = -1
	header.NextPosition = -1

	buffer := bytes.Buffer{}
	_ = binary.Write(&buffer, common.Endian, header)

	return buffer.Bytes(), nil
}

func (kv *mockWrongHeaderDataKV) GetSize(key string) (int64, error) {
	return 0, errors.New("wrong header")
}

func newMockWrongHeaderDataKV() kv.DataKV {
	return &mockWrongHeaderDataKV{}
}

func TestGetBinlogSize(t *testing.T) {
	memoryKV := memkv.NewMemoryKV()
	defer memoryKV.Close()

	key := "TestGetBinlogSize"

	var size int64
	var err error

	// key not in memoryKV
	size, err = GetBinlogSize(memoryKV, key)
	assert.Error(t, err)
	assert.Zero(t, size)

	// normal binlog key, for example, index binlog
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_FLAT"
	datas := []*Blob{
		{
			Key:   "ivf1",
			Value: []byte{1, 2, 3},
		},
		{
			Key:   "ivf2",
			Value: []byte{4, 5, 6},
		},
		{
			Key:   "large",
			Value: []byte(funcutil.RandomString(maxLengthPerRowOfIndexFile + 1)),
		},
	}

	codec := NewIndexFileBinlogCodec()

	serializedBlobs, err := codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.Nil(t, err)

	for _, blob := range serializedBlobs {
		err = memoryKV.Save(blob.Key, string(blob.Value))
		assert.Nil(t, err)

		size, err = GetBinlogSize(memoryKV, blob.Key)
		assert.Nil(t, err)
		assert.Equal(t, size, int64(len(blob.Value)))
	}
}

// cover case that failed to read event header
func TestGetBinlogSize_less_header(t *testing.T) {
	mockKV := newMockLessHeaderDataKV()

	key := "TestGetBinlogSize_less_header"

	_, err := GetBinlogSize(mockKV, key)
	assert.Error(t, err)
}

// cover case that file not in binlog format
func TestGetBinlogSize_not_in_binlog_format(t *testing.T) {
	mockKV := newMockWrongHeaderDataKV()

	key := "TestGetBinlogSize_not_in_binlog_format"

	_, err := GetBinlogSize(mockKV, key)
	assert.Error(t, err)
}

func TestEstimateMemorySize(t *testing.T) {
	memoryKV := memkv.NewMemoryKV()
	defer memoryKV.Close()

	key := "TestEstimateMemorySize"

	var size int64
	var err error

	// key not in memoryKV
	_, err = EstimateMemorySize(memoryKV, key)
	assert.Error(t, err)

	// normal binlog key, for example, index binlog
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_FLAT"
	datas := []*Blob{
		{
			Key:   "ivf1",
			Value: []byte{1, 2, 3},
		},
		{
			Key:   "ivf2",
			Value: []byte{4, 5, 6},
		},
		{
			Key:   "large",
			Value: []byte(funcutil.RandomString(maxLengthPerRowOfIndexFile + 1)),
		},
	}

	codec := NewIndexFileBinlogCodec()

	serializedBlobs, err := codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.Nil(t, err)

	for _, blob := range serializedBlobs {
		err = memoryKV.Save(blob.Key, string(blob.Value))
		assert.Nil(t, err)

		buf := bytes.NewBuffer(blob.Value)

		_, _ = readMagicNumber(buf)
		desc, _ := ReadDescriptorEvent(buf)

		size, err = EstimateMemorySize(memoryKV, blob.Key)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%v", desc.Extras[originalSizeKey]), fmt.Sprintf("%v", size))
	}
}

// cover case that failed to read event header
func TestEstimateMemorySize_less_header(t *testing.T) {
	mockKV := newMockLessHeaderDataKV()

	key := "TestEstimateMemorySize_less_header"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

// cover case that file not in binlog format
func TestEstimateMemorySize_not_in_binlog_format(t *testing.T) {
	mockKV := newMockWrongHeaderDataKV()

	key := "TestEstimateMemorySize_not_in_binlog_format"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

type mockFailedToGetDescDataKV struct {
	kv.BaseKV
}

func (kv *mockFailedToGetDescDataKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	header := &eventHeader{}
	header.EventLength = 20
	headerSize := binary.Size(header)

	if end-start > int64(headerSize) {
		return nil, errors.New("mock failed to get desc data")
	}

	buf := bytes.Buffer{}
	_ = binary.Write(&buf, common.Endian, header)
	return buf.Bytes(), nil
}

func (kv *mockFailedToGetDescDataKV) GetSize(key string) (int64, error) {
	return 0, nil
}

func newMockFailedToGetDescDataKV() *mockFailedToGetDescDataKV {
	return &mockFailedToGetDescDataKV{}
}

// cover case that failed to get descriptor event content
func TestEstimateMemorySize_failed_to_load_desc(t *testing.T) {
	mockKV := newMockFailedToGetDescDataKV()

	key := "TestEstimateMemorySize_failed_to_load_desc"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

type mockLessDescDataKV struct {
	kv.BaseKV
}

func (kv *mockLessDescDataKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	header := &baseEventHeader{}
	header.EventLength = 20

	buffer := bytes.Buffer{}
	_ = binary.Write(&buffer, common.Endian, header)

	// no event data
	return buffer.Bytes(), nil

	/*
		desc := &descriptorEvent{}
		desc.ExtraLength = 2
		desc.ExtraBytes = []byte{1, 2}
		buffer := bytes.Buffer{}
		_ = binary.Write(&buffer, common.Endian, desc)
		// extra not in json format
		return buffer.Bytes(), nil
	*/
}

func (kv *mockLessDescDataKV) GetSize(key string) (int64, error) {
	return 0, nil
}

func newMockLessDescDataKV() *mockLessDescDataKV {
	return &mockLessDescDataKV{}
}

func TestEstimateMemorySize_less_desc_data(t *testing.T) {
	mockKV := newMockLessDescDataKV()

	key := "TestEstimateMemorySize_less_desc_data"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

type mockOriginalSizeDataKV struct {
	kv.BaseKV
	impl func(key string, start, end int64) ([]byte, error)
}

func (kv *mockOriginalSizeDataKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	if kv.impl != nil {
		return kv.impl(key, start, end)
	}
	return nil, nil
}

func (kv *mockOriginalSizeDataKV) GetSize(key string) (int64, error) {
	return 0, nil
}

func newMockOriginalSizeDataKV() *mockOriginalSizeDataKV {
	return &mockOriginalSizeDataKV{}
}

func TestEstimateMemorySize_no_original_size(t *testing.T) {
	mockKV := newMockOriginalSizeDataKV()
	mockKV.impl = func(key string, start, end int64) ([]byte, error) {
		desc := &descriptorEvent{}
		desc.descriptorEventHeader.EventLength = 20
		desc.descriptorEventData = *newDescriptorEventData()
		extra := make(map[string]interface{})
		extra["key"] = "value"
		extraBytes, _ := json.Marshal(extra)
		desc.ExtraBytes = extraBytes
		desc.ExtraLength = int32(len(extraBytes))
		buf := bytes.Buffer{}
		_ = desc.descriptorEventHeader.Write(&buf)
		_ = desc.descriptorEventData.Write(&buf)
		return buf.Bytes(), nil
	}

	key := "TestEstimateMemorySize_no_original_size"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

func TestEstimateMemorySize_cannot_convert_original_size_to_int(t *testing.T) {
	mockKV := newMockOriginalSizeDataKV()
	mockKV.impl = func(key string, start, end int64) ([]byte, error) {
		desc := &descriptorEvent{}
		desc.descriptorEventHeader.EventLength = 20
		desc.descriptorEventData = *newDescriptorEventData()
		extra := make(map[string]interface{})
		extra[originalSizeKey] = "value"
		extraBytes, _ := json.Marshal(extra)
		desc.ExtraBytes = extraBytes
		desc.ExtraLength = int32(len(extraBytes))
		buf := bytes.Buffer{}
		_ = desc.descriptorEventHeader.Write(&buf)
		_ = desc.descriptorEventData.Write(&buf)
		return buf.Bytes(), nil
	}

	key := "TestEstimateMemorySize_cannot_convert_original_size_to_int"

	_, err := EstimateMemorySize(mockKV, key)
	assert.Error(t, err)
}

//////////////////////////////////////////////////////////////////////////////////////////////////

func TestCheckTsField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	assert.False(t, checkTsField(data))

	data.Data[common.TimeStampField] = &BoolFieldData{}
	assert.False(t, checkTsField(data))

	data.Data[common.TimeStampField] = &Int64FieldData{}
	assert.True(t, checkTsField(data))
}

func TestCheckRowIDField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	assert.False(t, checkRowIDField(data))

	data.Data[common.RowIDField] = &BoolFieldData{}
	assert.False(t, checkRowIDField(data))

	data.Data[common.RowIDField] = &Int64FieldData{}
	assert.True(t, checkRowIDField(data))
}

func TestCheckNumRows(t *testing.T) {
	assert.True(t, checkNumRows())

	f1 := &Int64FieldData{
		NumRows: nil,
		Data:    []int64{1, 2, 3},
	}
	f2 := &Int64FieldData{
		NumRows: nil,
		Data:    []int64{1, 2, 3},
	}
	f3 := &Int64FieldData{
		NumRows: nil,
		Data:    []int64{1, 2, 3, 4},
	}

	assert.True(t, checkNumRows(f1, f2))
	assert.False(t, checkNumRows(f1, f3))
	assert.False(t, checkNumRows(f2, f3))
	assert.False(t, checkNumRows(f1, f2, f3))
}

func TestSortFieldDataList(t *testing.T) {
	f1 := &Int16FieldData{
		NumRows: nil,
		Data:    []int16{1, 2, 3},
	}
	f2 := &Int32FieldData{
		NumRows: nil,
		Data:    []int32{4, 5, 6},
	}
	f3 := &Int64FieldData{
		NumRows: nil,
		Data:    []int64{7, 8, 9},
	}

	ls := fieldDataList{
		IDs:   []FieldID{1, 3, 2},
		datas: []FieldData{f1, f3, f2},
	}

	assert.Equal(t, 3, ls.Len())
	sortFieldDataList(ls)
	assert.ElementsMatch(t, []FieldID{1, 2, 3}, ls.IDs)
	assert.ElementsMatch(t, []FieldData{f1, f2, f3}, ls.datas)
}

func TestTransferColumnBasedInsertDataToRowBased(t *testing.T) {
	var err error

	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	// no ts
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	tss := &Int64FieldData{
		Data: []int64{1, 2, 3},
	}
	data.Data[common.TimeStampField] = tss

	// no row ids
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	rowIdsF := &Int64FieldData{
		Data: []int64{1, 2, 3, 4},
	}
	data.Data[common.RowIDField] = rowIdsF

	// row num mismatch
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	data.Data[common.RowIDField] = &Int64FieldData{
		Data: []int64{1, 2, 3},
	}

	f1 := &BoolFieldData{
		Data: []bool{true, false, true},
	}
	f2 := &Int8FieldData{
		Data: []int8{0, 0xf, 0x1f},
	}
	f3 := &Int16FieldData{
		Data: []int16{0, 0xff, 0x1fff},
	}
	f4 := &Int32FieldData{
		Data: []int32{0, 0xffff, 0x1fffffff},
	}
	f5 := &Int64FieldData{
		Data: []int64{0, 0xffffffff, 0x1fffffffffffffff},
	}
	f6 := &FloatFieldData{
		Data: []float32{0, 0, 0},
	}
	f7 := &DoubleFieldData{
		Data: []float64{0, 0, 0},
	}
	// maybe we cannot support string now, no matter what the length of string is fixed or not.
	// f8 := &StringFieldData{
	// 	Data: []string{"1", "2", "3"},
	// }
	f9 := &BinaryVectorFieldData{
		Dim:  8,
		Data: []byte{1, 2, 3},
	}
	f10 := &FloatVectorFieldData{
		Dim:  1,
		Data: []float32{0, 0, 0},
	}

	data.Data[101] = f1
	data.Data[102] = f2
	data.Data[103] = f3
	data.Data[104] = f4
	data.Data[105] = f5
	data.Data[106] = f6
	data.Data[107] = f7
	// data.Data[108] = f8
	data.Data[109] = f9
	data.Data[110] = f10

	utss, rowIds, rows, err := TransferColumnBasedInsertDataToRowBased(data)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []uint64{1, 2, 3}, utss)
	assert.ElementsMatch(t, []int64{1, 2, 3}, rowIds)
	assert.Equal(t, 3, len(rows))
	// b := []byte("1")[0]
	if common.Endian == binary.LittleEndian {
		// low byte in high address

		assert.ElementsMatch(t,
			[]byte{
				1,    // true
				0,    // 0
				0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 1, // "1"
				1,          // 1
				0, 0, 0, 0, // 0
			},
			rows[0].Value)
		assert.ElementsMatch(t,
			[]byte{
				0,       // false
				0xf,     // 0xf
				0, 0xff, // 0xff
				0, 0, 0xff, 0xff, // 0xffff
				0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // 0xffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 2, // "2"
				2,          // 2
				0, 0, 0, 0, // 0
			},
			rows[1].Value)
		assert.ElementsMatch(t,
			[]byte{
				1,          // false
				0x1f,       // 0x1f
				0xff, 0x1f, // 0x1fff
				0xff, 0xff, 0xff, 0x1f, // 0x1fffffff
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, // 0x1fffffffffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 3, // "3"
				3,          // 3
				0, 0, 0, 0, // 0
			},
			rows[2].Value)
	}
}
