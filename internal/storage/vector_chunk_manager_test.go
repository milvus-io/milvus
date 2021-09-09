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
	"context"
	"os"
	"path"
	"strconv"
	"testing"

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func newMinIOKVClient(ctx context.Context, bucketName string) (*miniokv.MinIOKV, error) {
	endPoint, _ := Params.Load("_MinioAddress")
	accessKeyID, _ := Params.Load("minio.accessKeyID")
	secretAccessKey, _ := Params.Load("minio.secretAccessKey")
	useSSLStr, _ := Params.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	option := &miniokv.Option{
		Address:           endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	client, err := miniokv.NewMinIOKV(ctx, option)
	return client, err
}

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

func buildVectorChunkManager(t *testing.T, localPath string, localCacheEnable bool) (*VectorChunkManager, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	bucketName := "vector-chunk-manager"
	minIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)

	rcm := NewMinioChunkManager(minIOKV)
	lcm := NewLocalChunkManager(localPath)

	meta := initMeta()
	vcm := NewVectorChunkManager(lcm, rcm, meta, localCacheEnable)
	assert.NotNil(t, vcm)

	var allCancel context.CancelFunc = func() {
		cancel()
		minIOKV.RemoveWithPrefix("")
	}
	return vcm, allCancel
}

var Params paramtable.BaseTable
var localPath string = "/tmp/milvus/test_data"

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.RemoveAll(localPath)
	os.Exit(exitCode)
}

func TestVectorChunkManager_GetPath(t *testing.T) {
	vcm, cancel := buildVectorChunkManager(t, localPath, true)
	defer cancel()
	assert.NotNil(t, vcm)

	key := "1"
	err := vcm.Write(key, []byte{1})
	assert.Nil(t, err)
	pathGet, err := vcm.GetPath(key)
	assert.Nil(t, err)
	pathJoin := path.Join(localPath, key)
	assert.Equal(t, pathGet, pathJoin)

	vcm.localCacheEnable = false
	vcm.remoteChunkManager.Write(key, []byte{1})
	pathGet, err = vcm.GetPath(key)
	assert.Nil(t, err)
	assert.Equal(t, pathGet, key)
}

func TestVectorChunkManager_Write(t *testing.T) {
	vcm, cancel := buildVectorChunkManager(t, localPath, false)
	defer cancel()
	assert.NotNil(t, vcm)

	key := "1"
	err := vcm.Write(key, []byte{1})
	assert.Error(t, err)

	vcm.localCacheEnable = true
	err = vcm.Write(key, []byte{1})
	assert.Nil(t, err)

	exist := vcm.Exist(key)
	assert.True(t, exist)
}

func TestVectorChunkManager_Read(t *testing.T) {
	meta := initMeta()
	vcm, cancel := buildVectorChunkManager(t, localPath, false)
	defer cancel()
	assert.NotNil(t, vcm)

	content, err := vcm.Read("9999")
	assert.Error(t, err)
	assert.Nil(t, content)

	vcm.localCacheEnable = true

	content, err = vcm.Read("9999")
	assert.Error(t, err)
	assert.Nil(t, content)

	binlogs := initBinlogFile(meta)
	assert.NotNil(t, binlogs)
	for _, binlog := range binlogs {
		vcm.remoteChunkManager.Write(binlog.Key, binlog.Value)
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

	content = make([]byte, 8*4)
	byteLen, err := vcm.ReadAt("109", content, 8*4)
	assert.Nil(t, err)
	assert.Equal(t, 32, byteLen)

	floatResult = make([]float32, 0)
	for i := 0; i < len(content)/4; i++ {
		singleData := typeutil.BytesToFloat32(content[i*4 : i*4+4])
		floatResult = append(floatResult, singleData)
	}
	assert.Equal(t, []float32{0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

	byteLen, err = vcm.ReadAt("9999", content, 0)
	assert.Error(t, err)
	assert.Equal(t, -1, byteLen)

	vcm.localCacheEnable = false
	byteLen, err = vcm.ReadAt("109", content, 8*4)
	assert.Nil(t, err)
	assert.Equal(t, 32, byteLen)

	byteLen, err = vcm.ReadAt("109", content, 9999)
	assert.Error(t, err)
	assert.Equal(t, 0, byteLen)

	byteLen, err = vcm.ReadAt("9999", content, 0)
	assert.Error(t, err)
	assert.Equal(t, -1, byteLen)
}
