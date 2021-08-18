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

var Params paramtable.BaseTable

func TestVectorChunkManager(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	minIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer minIOKV.RemoveWithPrefix("")

	rcm := NewMinioChunkManager(minIOKV)

	localPath := "/tmp/milvus/data"

	lcm := NewLocalChunkManager(localPath)

	meta := initMeta()
	vcm := NewVectorChunkManager(lcm, rcm, meta, false)
	assert.NotNil(t, vcm)

	binlogs := initBinlogFile(meta)
	assert.NotNil(t, binlogs)
	for _, binlog := range binlogs {
		rcm.Write(binlog.Key, binlog.Value)
	}

	content, err := vcm.Read("108")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0, 255}, content)

	content, err = vcm.Read("109")
	assert.Nil(t, err)

	floatResult := make([]float32, 0)
	for i := 0; i < len(content)/4; i++ {
		singleData := typeutil.ByteToFloat32(content[i*4 : i*4+4])
		floatResult = append(floatResult, singleData)
	}
	assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

	content = make([]byte, 8*4)
	byteLen, err := vcm.ReadAt("109", content, 8*4)
	assert.Nil(t, err)
	assert.Equal(t, 32, byteLen)

	floatResult = make([]float32, 0)
	for i := 0; i < len(content)/4; i++ {
		singleData := typeutil.ByteToFloat32(content[i*4 : i*4+4])
		floatResult = append(floatResult, singleData)
	}
	assert.Equal(t, []float32{0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

	os.Remove(path.Join(localPath, "108"))
	os.Remove(path.Join(localPath, "109"))
}

func TestVectorChunkManagerWithLocalCache(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	minIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer minIOKV.RemoveWithPrefix("")

	rcm := NewMinioChunkManager(minIOKV)

	localPath := "/tmp/milvus/data"

	lcm := NewLocalChunkManager(localPath)

	meta := initMeta()
	vcm := NewVectorChunkManager(lcm, rcm, meta, true)
	assert.NotNil(t, vcm)

	binlogs := initBinlogFile(meta)
	assert.NotNil(t, binlogs)
	for _, binlog := range binlogs {
		rcm.Write(binlog.Key, binlog.Value)
	}

	content, err := vcm.Read("108")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0, 255}, content)

	content, err = vcm.Read("109")
	assert.Nil(t, err)

	floatResult := make([]float32, 0)
	for i := 0; i < len(content)/4; i++ {
		singleData := typeutil.ByteToFloat32(content[i*4 : i*4+4])
		floatResult = append(floatResult, singleData)
	}
	assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

	content = make([]byte, 8*4)
	byteLen, err := vcm.ReadAt("109", content, 8*4)
	assert.Nil(t, err)
	assert.Equal(t, 32, byteLen)

	floatResult = make([]float32, 0)
	for i := 0; i < len(content)/4; i++ {
		singleData := typeutil.ByteToFloat32(content[i*4 : i*4+4])
		floatResult = append(floatResult, singleData)
	}
	assert.Equal(t, []float32{0, 111, 222, 333, 444, 555, 777, 666}, floatResult)

	os.Remove(path.Join(localPath, "108"))
	os.Remove(path.Join(localPath, "109"))
}

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
