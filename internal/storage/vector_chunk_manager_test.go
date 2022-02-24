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
	"context"
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func initMeta() *etcdpb.CollectionMeta {
	meta := &etcdpb.CollectionMeta{
		ID:            1,
		CreateTime:    1,
		SegmentIDs:    []int64{0, 1},
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      0,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      1,
					Name:         "Ts",
					IsPrimaryKey: false,
					Description:  "Ts",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      101,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "description_3",
					DataType:     schemapb.DataType_Int8,
				},
				{
					FieldID:      108,
					Name:         "field_binary_vector",
					IsPrimaryKey: false,
					Description:  "description_10",
					DataType:     schemapb.DataType_BinaryVector,
				},
				{
					FieldID:      109,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "description_11",
					DataType:     schemapb.DataType_FloatVector,
				},
			},
		},
	}
	return meta
}

func initBinlogFile(schema *etcdpb.CollectionMeta) []*Blob {
	insertCodec := NewInsertCodec(schema)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			1: &Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			101: &Int8FieldData{
				NumRows: []int64{2},
				Data:    []int8{3, 4},
			},
			108: &BinaryVectorFieldData{
				NumRows: []int64{2},
				Data:    []byte{0, 255},
				Dim:     8,
			},
			109: &FloatVectorFieldData{
				NumRows: []int64{2},
				Data:    []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666},
				Dim:     8,
			},
		},
	}

	blobs, _, err := insertCodec.Serialize(1, 1, insertData)
	if err != nil {
		return nil
	}
	return blobs
}

func buildVectorChunkManager(localPath string, localCacheEnable bool) (*VectorChunkManager, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())

	bucketName := "vector-chunk-manager"

	rcm, err := newMinIOChunkManager(ctx, bucketName)
	if err != nil {
		return nil, cancel, err
	}
	lcm := NewLocalChunkManager(RootPath(localPath))

	meta := initMeta()
	vcm := NewVectorChunkManager(lcm, rcm, meta, localCacheEnable)

	return vcm, cancel, nil
}

var Params paramtable.BaseTable
var localPath = "/tmp/milvus/test_data/"

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	err := os.RemoveAll(localPath)
	if err != nil {
		return
	}
	os.Exit(exitCode)
}

func TestVectorChunkManager_GetPath(t *testing.T) {
	vcm, cancel, err := buildVectorChunkManager(localPath, true)
	assert.NotNil(t, vcm)
	assert.NoError(t, err)

	key := "1"
	err = vcm.Write(key, []byte{1})
	assert.Nil(t, err)
	pathGet, err := vcm.GetPath(key)
	assert.Nil(t, err)
	pathJoin := path.Join(localPath, key)
	assert.Equal(t, pathGet, pathJoin)

	vcm.localCacheEnable = false
	err = vcm.remoteChunkManager.Write(key, []byte{1})
	assert.Nil(t, err)
	pathGet, err = vcm.GetPath(key)
	assert.Nil(t, err)
	assert.Equal(t, pathGet, key)

	err = vcm.RemoveWithPrefix(localPath)
	assert.NoError(t, err)
	cancel()
}

func TestVectorChunkManager_GetSize(t *testing.T) {
	vcm, cancel, err := buildVectorChunkManager(localPath, true)
	assert.NotNil(t, vcm)
	assert.NoError(t, err)

	key := "1"
	err = vcm.Write(key, []byte{1})
	assert.Nil(t, err)
	sizeGet, err := vcm.GetSize(key)
	assert.Nil(t, err)
	assert.EqualValues(t, sizeGet, 1)

	vcm.localCacheEnable = false
	err = vcm.remoteChunkManager.Write(key, []byte{1})
	assert.Nil(t, err)
	sizeGet, err = vcm.GetSize(key)
	assert.Nil(t, err)
	assert.EqualValues(t, sizeGet, 1)

	err = vcm.RemoveWithPrefix(localPath)
	assert.NoError(t, err)
	cancel()
}

func TestVectorChunkManager_Write(t *testing.T) {
	vcm, cancel, err := buildVectorChunkManager(localPath, false)
	assert.NoError(t, err)
	assert.NotNil(t, vcm)

	key := "1"
	err = vcm.Write(key, []byte{1})
	assert.Error(t, err)

	vcm.localCacheEnable = true
	err = vcm.Write(key, []byte{1})
	assert.Nil(t, err)

	exist := vcm.Exist(key)
	assert.True(t, exist)

	contents := map[string][]byte{
		"key_1": {111},
		"key_2": {222},
	}
	err = vcm.MultiWrite(contents)
	assert.NoError(t, err)

	exist = vcm.Exist("key_1")
	assert.True(t, exist)
	exist = vcm.Exist("key_2")
	assert.True(t, exist)

	err = vcm.RemoveWithPrefix(localPath)
	assert.NoError(t, err)
	cancel()
}

func TestVectorChunkManager_Remove(t *testing.T) {
	localCaches := []bool{true, false}
	for _, localCache := range localCaches {
		vcm, cancel, err := buildVectorChunkManager(localPath, localCache)
		assert.NoError(t, err)
		assert.NotNil(t, vcm)

		key := "1"
		err = vcm.remoteChunkManager.Write(key, []byte{1})
		assert.Nil(t, err)

		err = vcm.Remove(key)
		assert.Nil(t, err)

		exist := vcm.Exist(key)
		assert.False(t, exist)

		contents := map[string][]byte{
			"key_1": {111},
			"key_2": {222},
		}
		err = vcm.remoteChunkManager.MultiWrite(contents)
		assert.NoError(t, err)

		err = vcm.MultiRemove([]string{"key_1", "key_2"})
		assert.NoError(t, err)

		exist = vcm.Exist("key_1")
		assert.False(t, exist)
		exist = vcm.Exist("key_2")
		assert.False(t, exist)

		err = vcm.RemoveWithPrefix(localPath)
		assert.NoError(t, err)
		cancel()
	}
}

type mockFailedChunkManager struct {
	fail bool
	ChunkManager
}

func (m *mockFailedChunkManager) Remove(key string) error {
	if m.fail {
		return errors.New("remove error")
	}
	return nil
}

func (m *mockFailedChunkManager) RemoveWithPrefix(prefix string) error {
	if m.fail {
		return errors.New("remove with prefix error")
	}
	return nil
}
func (m *mockFailedChunkManager) MultiRemove(key []string) error {
	if m.fail {
		return errors.New("multi remove error")
	}
	return nil
}

func TestVectorChunkManager_Remove_Fail(t *testing.T) {
	vcm := &VectorChunkManager{
		localChunkManager: &mockFailedChunkManager{fail: true},
	}
	assert.Error(t, vcm.Remove("test"))
	assert.Error(t, vcm.MultiRemove([]string{"test"}))
	assert.Error(t, vcm.RemoveWithPrefix("test"))

	vcm = &VectorChunkManager{
		localChunkManager:  &mockFailedChunkManager{fail: false},
		remoteChunkManager: &mockFailedChunkManager{fail: true},
	}
	assert.Error(t, vcm.Remove("test"))
	assert.Error(t, vcm.MultiRemove([]string{"test"}))
	assert.Error(t, vcm.RemoveWithPrefix("test"))
}

func TestVectorChunkManager_Read(t *testing.T) {
	localCaches := []bool{true, false}
	for _, localCache := range localCaches {
		vcm, cancel, err := buildVectorChunkManager(localPath, localCache)
		assert.NotNil(t, vcm)
		assert.NoError(t, err)

		content, err := vcm.Read("9999")
		assert.Error(t, err)
		assert.Nil(t, content)

		vcm.localCacheEnable = true

		content, err = vcm.Read("9999")
		assert.Error(t, err)
		assert.Nil(t, content)

		meta := initMeta()
		binlogs := initBinlogFile(meta)
		assert.NotNil(t, binlogs)
		for _, binlog := range binlogs {
			err := vcm.remoteChunkManager.Write(binlog.Key, binlog.Value)
			assert.Nil(t, err)
		}

		content, err = vcm.Read("108")
		assert.Nil(t, err)
		assert.Equal(t, []byte{0, 255}, content)

		content, err = vcm.Read("109")
		assert.Nil(t, err)
		floatResult := make([]float32, 0)
		for i := 0; i < len(content)/4; i++ {
			singleData := typeutil.BytesToFloat32(content[i*4 : i*4+4])
			floatResult = append(floatResult, singleData)
		}
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

		contents, err := vcm.MultiRead([]string{"108", "109"})
		assert.Nil(t, err)
		assert.Equal(t, []byte{0, 255}, contents[0])

		floatResult = make([]float32, 0)
		for i := 0; i < len(content)/4; i++ {
			singleData := typeutil.BytesToFloat32(contents[1][i*4 : i*4+4])
			floatResult = append(floatResult, singleData)
		}
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

		floatResult = make([]float32, 0)
		for i := 0; i < len(content)/4; i++ {
			singleData := typeutil.BytesToFloat32(contents[1][i*4 : i*4+4])
			floatResult = append(floatResult, singleData)
		}
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

		content, err = vcm.ReadAt("109", 8*4, 8*4)
		assert.Nil(t, err)

		floatResult = make([]float32, 0)
		for i := 0; i < len(content)/4; i++ {
			singleData := typeutil.BytesToFloat32(content[i*4 : i*4+4])
			floatResult = append(floatResult, singleData)
		}
		assert.Equal(t, []float32{0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

		content, err = vcm.ReadAt("9999", 0, 8*4)
		assert.Error(t, err)
		assert.Nil(t, content)

		vcm.localCacheEnable = false
		content, err = vcm.ReadAt("109", 8*4, 8*4)
		assert.Nil(t, err)
		assert.Equal(t, 32, len(content))

		content, err = vcm.ReadAt("109", 9999, 8*4)
		assert.Error(t, err)
		assert.Nil(t, content)

		content, err = vcm.ReadAt("9999", 0, 8*4)
		assert.Error(t, err)
		assert.Nil(t, content)

		err = vcm.RemoveWithPrefix(localPath)
		assert.NoError(t, err)
		cancel()
	}
}
