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

package indexnode

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestIndexNode(t *testing.T) {
	ctx := context.Background()

	indexID := UniqueID(999)
	indexBuildID1 := UniqueID(54321)
	indexBuildID2 := UniqueID(12345)
	floatVectorFieldID := UniqueID(101)
	binaryVectorFieldID := UniqueID(102)
	tsFieldID := UniqueID(1)
	collectionID := UniqueID(201)
	floatVectorFieldName := "float_vector"
	binaryVectorFieldName := "binary_vector"
	metaPath1 := "FloatVector"
	metaPath2 := "BinaryVector"
	metaPath3 := "FloatVectorDeleted"
	floatVectorBinlogPath := "float_vector_binlog"
	binaryVectorBinlogPath := "binary_vector_binlog"

	in, err := NewIndexNode(ctx)
	assert.Nil(t, err)
	Params.Init()

	err = in.Register()
	assert.Nil(t, err)
	err = in.Init()
	assert.Nil(t, err)

	err = in.Start()
	assert.Nil(t, err)

	t.Run("CreateIndex FloatVector", func(t *testing.T) {
		var insertCodec storage.InsertCodec
		defer insertCodec.Close()

		insertCodec.Schema = &etcdpb.CollectionMeta{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      floatVectorFieldID,
						Name:         floatVectorFieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
					},
				},
			},
		}
		data := make(map[UniqueID]storage.FieldData)
		tsData := make([]int64, nb)
		for i := 0; i < nb; i++ {
			tsData[i] = int64(i + 100)
		}
		data[tsFieldID] = &storage.Int64FieldData{
			NumRows: []int64{nb},
			Data:    tsData,
		}
		data[floatVectorFieldID] = &storage.FloatVectorFieldData{
			NumRows: []int64{nb},
			Data:    generateFloatVectors(),
			Dim:     dim,
		}
		insertData := storage.InsertData{
			Data: data,
			Infos: []storage.BlobInfo{
				{
					Length: 10,
				},
			},
		}
		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
		assert.Nil(t, err)
		kvs := make(map[string]string, len(binLogs))
		paths := make([]string, 0, len(binLogs))
		for i, blob := range binLogs {
			key := path.Join(floatVectorBinlogPath, strconv.Itoa(i))
			paths = append(paths, key)
			kvs[key] = string(blob.Value[:])
		}
		err = in.kv.MultiSave(kvs)
		assert.Nil(t, err)

		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: indexBuildID1,
			State:        commonpb.IndexState_InProgress,
			Version:      1,
		}

		value := proto.MarshalTextString(indexMeta)
		err = in.etcdKV.Save(metaPath1, value)
		assert.Nil(t, err)
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: indexBuildID1,
			IndexName:    "FloatVector",
			IndexID:      indexID,
			Version:      1,
			MetaPath:     metaPath1,
			DataPaths:    paths,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "8",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "index_type",
					Value: "IVF_SQ8",
				},
				{
					Key:   "params",
					Value: "{\"nlist\": 128}",
				},
				{
					Key:   "metric_type",
					Value: "L2",
				},
			},
		}

		status, err := in.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		value, err = in.etcdKV.Load(metaPath1)
		assert.Nil(t, err)
		indexMetaTmp := indexpb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMetaTmp)
		assert.Nil(t, err)
		if indexMetaTmp.State != commonpb.IndexState_Finished {
			time.Sleep(10 * time.Second)
			value, err = in.etcdKV.Load(metaPath1)
			assert.Nil(t, err)
			indexMetaTmp2 := indexpb.IndexMeta{}
			err = proto.UnmarshalText(value, &indexMetaTmp2)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.IndexState_Finished, indexMetaTmp2.State)
			defer in.kv.MultiRemove(indexMetaTmp2.IndexFilePaths)
		}
		defer in.kv.MultiRemove(indexMetaTmp.IndexFilePaths)
		defer func() {
			for k := range kvs {
				in.kv.Remove(k)
			}
		}()
	})
	t.Run("CreateIndex BinaryVector", func(t *testing.T) {
		var insertCodec storage.InsertCodec
		defer insertCodec.Close()

		insertCodec.Schema = &etcdpb.CollectionMeta{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      binaryVectorFieldID,
						Name:         binaryVectorFieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_BinaryVector,
					},
				},
			},
		}
		data := make(map[UniqueID]storage.FieldData)
		tsData := make([]int64, nb)
		for i := 0; i < nb; i++ {
			tsData[i] = int64(i + 100)
		}
		data[tsFieldID] = &storage.Int64FieldData{
			NumRows: []int64{nb},
			Data:    tsData,
		}
		data[binaryVectorFieldID] = &storage.BinaryVectorFieldData{
			NumRows: []int64{nb},
			Data:    generateBinaryVectors(),
			Dim:     dim,
		}
		insertData := storage.InsertData{
			Data: data,
			Infos: []storage.BlobInfo{
				{
					Length: 10,
				},
			},
		}
		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
		assert.Nil(t, err)
		kvs := make(map[string]string, len(binLogs))
		paths := make([]string, 0, len(binLogs))
		for i, blob := range binLogs {
			key := path.Join(binaryVectorBinlogPath, strconv.Itoa(i))
			paths = append(paths, key)
			kvs[key] = string(blob.Value[:])
		}
		err = in.kv.MultiSave(kvs)
		assert.Nil(t, err)

		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: indexBuildID2,
			State:        commonpb.IndexState_InProgress,
			Version:      1,
		}

		value := proto.MarshalTextString(indexMeta)
		err = in.etcdKV.Save(metaPath2, value)
		assert.Nil(t, err)
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: indexBuildID2,
			IndexName:    "BinaryVector",
			IndexID:      indexID,
			Version:      1,
			MetaPath:     metaPath2,
			DataPaths:    paths,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "8",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "index_type",
					Value: "BIN_FLAT",
				},
				{
					Key:   "metric_type",
					Value: "JACCARD",
				},
			},
		}

		status, err := in.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		value, err = in.etcdKV.Load(metaPath2)
		assert.Nil(t, err)
		indexMetaTmp := indexpb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMetaTmp)
		assert.Nil(t, err)
		if indexMetaTmp.State != commonpb.IndexState_Finished {
			time.Sleep(10 * time.Second)
			value, err = in.etcdKV.Load(metaPath2)
			assert.Nil(t, err)
			indexMetaTmp2 := indexpb.IndexMeta{}
			err = proto.UnmarshalText(value, &indexMetaTmp2)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.IndexState_Finished, indexMetaTmp2.State)
			defer in.kv.MultiRemove(indexMetaTmp2.IndexFilePaths)
		}
		defer in.kv.MultiRemove(indexMetaTmp.IndexFilePaths)
		defer func() {
			for k := range kvs {
				in.kv.Remove(k)
			}
		}()
	})

	t.Run("Create Deleted_Index", func(t *testing.T) {
		var insertCodec storage.InsertCodec
		defer insertCodec.Close()

		insertCodec.Schema = &etcdpb.CollectionMeta{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      floatVectorFieldID,
						Name:         floatVectorFieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
					},
				},
			},
		}
		data := make(map[UniqueID]storage.FieldData)
		tsData := make([]int64, nb)
		for i := 0; i < nb; i++ {
			tsData[i] = int64(i + 100)
		}
		data[tsFieldID] = &storage.Int64FieldData{
			NumRows: []int64{nb},
			Data:    tsData,
		}
		data[floatVectorFieldID] = &storage.FloatVectorFieldData{
			NumRows: []int64{nb},
			Data:    generateFloatVectors(),
			Dim:     dim,
		}
		insertData := storage.InsertData{
			Data: data,
			Infos: []storage.BlobInfo{
				{
					Length: 10,
				},
			},
		}
		binLogs, _, err := insertCodec.Serialize(999, 888, &insertData)
		assert.Nil(t, err)
		kvs := make(map[string]string, len(binLogs))
		paths := make([]string, 0, len(binLogs))
		for i, blob := range binLogs {
			key := path.Join(floatVectorBinlogPath, strconv.Itoa(i))
			paths = append(paths, key)
			kvs[key] = string(blob.Value[:])
		}
		err = in.kv.MultiSave(kvs)
		assert.Nil(t, err)

		indexMeta := &indexpb.IndexMeta{
			IndexBuildID: indexBuildID1,
			State:        commonpb.IndexState_InProgress,
			Version:      1,
			MarkDeleted:  true,
		}

		value := proto.MarshalTextString(indexMeta)
		err = in.etcdKV.Save(metaPath3, value)
		assert.Nil(t, err)
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: indexBuildID1,
			IndexName:    "FloatVector",
			IndexID:      indexID,
			Version:      1,
			MetaPath:     metaPath3,
			DataPaths:    paths,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "8",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "index_type",
					Value: "IVF_SQ8",
				},
				{
					Key:   "params",
					Value: "{\"nlist\": 128}",
				},
				{
					Key:   "metric_type",
					Value: "L2",
				},
			},
		}

		status, err := in.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		value, err = in.etcdKV.Load(metaPath3)
		assert.Nil(t, err)
		indexMetaTmp := indexpb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMetaTmp)
		assert.Nil(t, err)
		if indexMetaTmp.State != commonpb.IndexState_Finished {
			time.Sleep(10 * time.Second)
			value, err = in.etcdKV.Load(metaPath3)
			assert.Nil(t, err)
			indexMetaTmp2 := indexpb.IndexMeta{}
			err = proto.UnmarshalText(value, &indexMetaTmp2)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.IndexState_Finished, indexMetaTmp2.State)
			defer in.kv.MultiRemove(indexMetaTmp2.IndexFilePaths)
		}
		defer in.kv.MultiRemove(indexMetaTmp.IndexFilePaths)
		defer func() {
			for k := range kvs {
				in.kv.Remove(k)
			}
		}()
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		resp, err := in.GetComponentStates(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, internalpb.StateCode_Healthy, resp.State.StateCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := in.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := in.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = in.Stop()
	assert.Nil(t, err)
}
