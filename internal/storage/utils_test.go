// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

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
	_ = binary.Write(&buffer, binary.LittleEndian, header)

	return buffer.Bytes(), nil
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
	assert.NoError(t, err)
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
	defer codec.Close()

	serializedBlobs, err := codec.Serialize(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas)
	assert.Nil(t, err)

	for _, blob := range serializedBlobs {
		err = memoryKV.Save(blob.Key, string(blob.Value))
		assert.Nil(t, err)

		size, err = GetBinlogSize(memoryKV, blob.Key)
		assert.Nil(t, err)
		assert.Equal(t, size+int64(binary.Size(MagicNumber)), int64(len(blob.Value)))
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
	defer codec.Close()

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
	_ = binary.Write(&buf, binary.LittleEndian, header)
	return buf.Bytes(), nil
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
	_ = binary.Write(&buffer, binary.LittleEndian, header)

	// no event data
	return buffer.Bytes(), nil

	/*
		desc := &descriptorEvent{}
		desc.ExtraLength = 2
		desc.ExtraBytes = []byte{1, 2}
		buffer := bytes.Buffer{}
		_ = binary.Write(&buffer, binary.LittleEndian, desc)
		// extra not in json format
		return buffer.Bytes(), nil
	*/
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
