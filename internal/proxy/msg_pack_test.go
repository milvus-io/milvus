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

package proxy

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestRepackInsertData(t *testing.T) {
	nb := 10
	hash := generateHashKeys(nb)
	prefix := "TestRepackInsertData"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	ctx := context.Background()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	cache := NewMockCache(t)
	cache.On("GetPartitionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(int64(1), nil)
	globalMetaCache = cache

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	t.Run("create collection", func(t *testing.T) {
		resp, err := rc.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)

		resp, err = rc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)
	})

	fieldData := generateFieldData(schemapb.DataType_Int64, testInt64Field, nb)
	insertMsg := &BaseInsertTask{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hash,
		},
		InsertRequest: msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				MsgID:    0,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			NumRows:        uint64(nb),
			FieldsData:     []*schemapb.FieldData{fieldData},
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}
	insertMsg.Timestamps = make([]uint64, nb)
	for index := range insertMsg.Timestamps {
		insertMsg.Timestamps[index] = insertMsg.BeginTimestamp
	}
	insertMsg.RowIDs = make([]UniqueID, nb)
	for index := range insertMsg.RowIDs {
		insertMsg.RowIDs[index] = int64(index)
	}

	ids, err := parsePrimaryFieldData2IDs(fieldData)
	assert.NoError(t, err)
	result := &milvuspb.MutationResult{
		IDs: ids,
	}

	t.Run("assign segmentID failed", func(t *testing.T) {
		fakeSegAllocator, err := newSegIDAssigner(ctx, &mockDataCoord2{expireTime: Timestamp(2500)}, getLastTick1)
		assert.NoError(t, err)
		_ = fakeSegAllocator.Start()
		defer fakeSegAllocator.Close()

		_, err = repackInsertData(ctx, []string{"test_dml_channel"}, insertMsg,
			result, idAllocator, fakeSegAllocator)
		assert.Error(t, err)
	})

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("repack insert data success", func(t *testing.T) {
		_, err = repackInsertData(ctx, []string{"test_dml_channel"}, insertMsg, result, idAllocator, segAllocator)
		assert.NoError(t, err)
	})
}

func TestRepackInsertDataWithPartitionKey(t *testing.T) {
	nb := 10
	hash := generateHashKeys(nb)
	prefix := "TestRepackInsertData"
	collectionName := prefix + funcutil.GenRandomStr()

	ctx := context.Background()
	dbName := GetCurDBNameFromContextOrDefault(ctx)

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	err := InitMetaCache(ctx, rc, nil, nil)
	assert.NoError(t, err)

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	_ = segAllocator.Start()
	defer segAllocator.Close()

	fieldName2Types := map[string]schemapb.DataType{
		testInt64Field:    schemapb.DataType_Int64,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector}

	t.Run("create collection with partition key", func(t *testing.T) {
		schema := ConstructCollectionSchemaWithPartitionKey(collectionName, fieldName2Types, testInt64Field, testVarCharField, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		resp, err := rc.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			NumPartitions:  100,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		assert.NoError(t, err)
	})

	fieldNameToDatas := make(map[string]*schemapb.FieldData)
	fieldDatas := make([]*schemapb.FieldData, 0)
	for name, dataType := range fieldName2Types {
		data := generateFieldData(dataType, name, nb)
		fieldNameToDatas[name] = data
		fieldDatas = append(fieldDatas, data)
	}

	insertMsg := &BaseInsertTask{
		BaseMsg: msgstream.BaseMsg{
			HashValues: hash,
		},
		InsertRequest: msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				MsgID:    0,
				SourceID: paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			NumRows:        uint64(nb),
			FieldsData:     fieldDatas,
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}
	insertMsg.Timestamps = make([]uint64, nb)
	for index := range insertMsg.Timestamps {
		insertMsg.Timestamps[index] = insertMsg.BeginTimestamp
	}
	insertMsg.RowIDs = make([]UniqueID, nb)
	for index := range insertMsg.RowIDs {
		insertMsg.RowIDs[index] = int64(index)
	}

	ids, err := parsePrimaryFieldData2IDs(fieldNameToDatas[testInt64Field])
	assert.NoError(t, err)
	result := &milvuspb.MutationResult{
		IDs: ids,
	}

	t.Run("repack insert data success", func(t *testing.T) {
		partitionKeys := generateFieldData(schemapb.DataType_VarChar, testVarCharField, nb)
		_, err = repackInsertDataWithPartitionKey(ctx, []string{"test_dml_channel"}, partitionKeys,
			insertMsg, result, idAllocator, segAllocator)
		assert.NoError(t, err)
	})
}
