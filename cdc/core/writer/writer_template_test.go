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

package writer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type AssertPosition struct {
	lock      sync.Mutex
	positions map[int64]map[string]*commonpb.KeyDataPair
	saveNum   int
}

func NewAssertPosition() *AssertPosition {
	return &AssertPosition{
		positions: map[int64]map[string]*commonpb.KeyDataPair{},
	}
}

func (a *AssertPosition) savePosition(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.saveNum++
	if _, ok := a.positions[collectionID]; !ok {
		a.positions[collectionID] = map[string]*commonpb.KeyDataPair{}
	}
	a.positions[collectionID][pChannelName] = position
}

func (a *AssertPosition) clear() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.positions = map[int64]map[string]*commonpb.KeyDataPair{}
	a.saveNum = 0
}

func TestWriterTemplateCreateCollection(t *testing.T) {
	mockMilvusFactory := mocks.NewMilvusClientFactory(t)
	mockMilvusClient := mocks.NewMilvusClientApi(t)
	factoryOption := writer.MilvusFactoryOption(mockMilvusFactory)
	writerCallback := mocks.NewWriteCallback(t)
	call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
	defer call.Unset()

	handler, err := writer.NewMilvusDataHandler(addressOption, userOption, tlsOption, timeoutOption, ignorePartition, factoryOption)
	assert.NoError(t, err)

	cdcWriter := writer.NewCDCWriterTemplate(
		writer.HandlerOption(handler),
		writer.BufferOption(10*time.Second, 1024, nil),
		writer.ErrorProtectOption(1, time.Second),
	)

	t.Run("msg type error", func(t *testing.T) {
		err = cdcWriter.Write(context.Background(), &model.CDCData{
			Msg: &api.CreateCollectionMsg{
				CreateCollectionRequest: pb.CreateCollectionRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_TimeTick,
					},
				},
			},
		}, writerCallback)
		assert.Error(t, err)
	})

	var shardNum int32 = 5
	level := commonpb.ConsistencyLevel_Session
	kv := &commonpb.KeyValuePair{Key: "foo", Value: "111"}

	options := []client.CreateCollectionOption{
		client.WithCollectionProperty(kv.GetKey(), kv.GetValue()),
		client.WithConsistencyLevel(entity.ConsistencyLevel(level)),
	}
	pbSchema := &schemapb.CollectionSchema{
		Name:        "coll",
		Description: "coll-des",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "first",
				Description:  "first-desc",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_VarChar,
			},
			{
				FieldID:     101,
				Name:        "second",
				Description: "second-desc",
				DataType:    schemapb.DataType_Double,
			},
		},
	}
	pbSchemaByte, _ := json.Marshal(pbSchema)

	data := &model.CDCData{
		Msg: &api.CreateCollectionMsg{
			CreateCollectionRequest: pb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateCollection,
				},
				CollectionName: "coll",
				CollectionID:   1001,
				Schema:         pbSchemaByte,
			},
		},
		Extra: map[string]any{
			model.ShardNumKey:             shardNum,
			model.ConsistencyLevelKey:     level,
			model.CollectionPropertiesKey: []*commonpb.KeyValuePair{kv},
		},
	}

	t.Run("success", func(t *testing.T) {
		createCall := mockMilvusClient.On("CreateCollection", mock.Anything, mock.Anything, shardNum, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				assert.Len(t, args, 5)

				entitySchema := args[1].(*entity.Schema)
				assert.Equal(t, entitySchema.CollectionName, "coll")
				assert.Equal(t, entitySchema.Description, "coll-des")
				assert.True(t, entitySchema.AutoID)
				assert.Len(t, entitySchema.Fields, 2)
				assert.EqualValues(t, 100, entitySchema.Fields[0].ID)
				assert.EqualValues(t, 101, entitySchema.Fields[1].ID)

				createRequest1 := &milvuspb.CreateCollectionRequest{}
				for _, option := range options {
					option(createRequest1)
				}
				createRequest2 := &milvuspb.CreateCollectionRequest{}
				for _, option := range args[3:] {
					option.(client.CreateCollectionOption)(createRequest2)
				}
				assert.EqualValues(t, createRequest1, createRequest2)
			}).
			Return(nil)
		defer createCall.Unset()
		successCallbackCall := writerCallback.On("OnSuccess", int64(1001), mock.Anything).Return()
		defer successCallbackCall.Unset()
		err = cdcWriter.Write(context.Background(), data, writerCallback)
		assert.NoError(t, err)
	})

	t.Run("create error", func(t *testing.T) {
		createCall := mockMilvusClient.On("CreateCollection", mock.Anything, mock.Anything, shardNum, mock.Anything, mock.Anything).
			Return(errors.New("create error"))
		defer createCall.Unset()
		failCallbackCall := writerCallback.On("OnFail", data, mock.Anything).Return()
		defer failCallbackCall.Unset()
		err = cdcWriter.Write(context.Background(), data, writerCallback)
		assert.NoError(t, err)

		// trigger error protect
		err = cdcWriter.Write(context.Background(), data, writerCallback)
		assert.Error(t, err)
	})
}

func TestWriterTemplateInsertDeleteDrop(t *testing.T) {
	mockMilvusFactory := mocks.NewMilvusClientFactory(t)
	factoryOption := writer.MilvusFactoryOption(mockMilvusFactory)

	assertPosition := NewAssertPosition()
	newWriter := func() writer.CDCWriter {
		handler, err := writer.NewMilvusDataHandler(addressOption, userOption, tlsOption, timeoutOption, ignorePartition, factoryOption)
		assert.NoError(t, err)
		return writer.NewCDCWriterTemplate(
			writer.HandlerOption(handler),
			writer.BufferOption(5*time.Second, 10*1024*1024, assertPosition.savePosition),
			writer.ErrorProtectOption(5, time.Second),
		)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	binaryVector := []byte{255, 255, 255, 0}
	generateInsertData := func(collectionID int64, collectionName string, channelName string, partitionName string, msgID string) *model.CDCData {
		return &model.CDCData{
			Msg: &api.InsertMsg{
				InsertRequest: pb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					CollectionName: collectionName,
					CollectionID:   collectionID,
					PartitionName:  partitionName,
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Bool,
							FieldName: "ok",
							FieldId:   101,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_BoolData{
										BoolData: &schemapb.BoolArray{
											Data: []bool{true},
										},
									},
								},
							},
						},
						{
							Type:      schemapb.DataType_String,
							FieldName: "ok",
							FieldId:   102,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{
											Data: []string{"true"},
										},
									},
								},
							},
						},
						{
							Type:      schemapb.DataType_BinaryVector,
							FieldName: "ok",
							FieldId:   102,
							Field: &schemapb.FieldData_Vectors{
								Vectors: &schemapb.VectorField{
									Dim: 32,
									Data: &schemapb.VectorField_BinaryVector{
										BinaryVector: binaryVector,
									},
								},
							},
						},
					},
				},
				BaseMsg: api.BaseMsg{
					EndTimestamp: 1000,
					MsgPosition: &pb.MsgPosition{
						ChannelName: channelName,
						MsgID:       []byte(msgID),
					},
				},
			},
		}
	}

	generateDeleteData := func(collectionID int64, collectionName string, channelName string, partitionName string, msgID string, ids []int64) *model.CDCData {
		return &model.CDCData{
			Msg: &api.DeleteMsg{
				DeleteRequest: pb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Delete,
					},
					CollectionName: collectionName,
					CollectionID:   collectionID,
					PartitionName:  partitionName,
					PrimaryKeys: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: ids,
							},
						},
					},
				},
				BaseMsg: api.BaseMsg{
					EndTimestamp: 1000,
					MsgPosition: &pb.MsgPosition{
						ChannelName: channelName,
						MsgID:       []byte(msgID),
					},
				},
			},
		}
	}

	generateDropData := func(collectionID int64, collectionName string, channelName, msgID, channelName2, msgID2 string) *model.CDCData {
		return &model.CDCData{
			Msg: &api.DropCollectionMsg{
				DropCollectionRequest: pb.DropCollectionRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_DropCollection,
					},
					CollectionName: collectionName,
					CollectionID:   collectionID,
				},
				BaseMsg: api.BaseMsg{
					EndTimestamp: 1000,
					MsgPosition: &pb.MsgPosition{
						ChannelName: channelName,
						MsgID:       []byte(msgID),
					},
				},
			},
			Extra: map[string]any{
				model.DropCollectionMsgsKey: []*api.DropCollectionMsg{
					{
						BaseMsg: api.BaseMsg{
							EndTimestamp: 2000,
							MsgPosition: &pb.MsgPosition{
								ChannelName: channelName2,
								MsgID:       []byte(msgID2),
							},
						},
					},
				},
			},
		}
	}

	t.Run("insert success", func(t *testing.T) {
		defer assertPosition.clear()
		writerCallback := mocks.NewWriteCallback(t)
		mockMilvusClient := mocks.NewMilvusClientApi(t)
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()

		successCallbackCall := writerCallback.On("OnSuccess", mock.Anything, mock.Anything).Return()
		defer successCallbackCall.Unset()
		insertCall := mockMilvusClient.On("Insert", mock.Anything, mock.Anything, "", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			collectionName := args[1].(string)
			if collectionName == "coll" {
				boolColumn := args[3].(*entity.ColumnBool)
				assert.Len(t, boolColumn.Data(), 3)
				stringColumn := args[4].(*entity.ColumnString)
				assert.Len(t, stringColumn.Data(), 3)
				byteVectorColumn := args[5].(*entity.ColumnBinaryVector)
				assert.Len(t, byteVectorColumn.Data(), 3)
			}
		}).Return(nil, nil)
		defer insertCall.Unset()
		cdcWriter := newWriter()

		err := cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "a", "part", "a"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "b", "part", "b"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1002), "coll2", "a", "part", "c"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "a", "part", "d"), writerCallback)
		assert.NoError(t, err)
		cdcWriter.Flush(context.Background())
		time.Sleep(time.Second)
		writerCallback.AssertCalled(t, "OnSuccess", int64(1001), mock.Anything) // 3
		writerCallback.AssertCalled(t, "OnSuccess", int64(1002), mock.Anything) // 1
		writerCallback.AssertNumberOfCalls(t, "OnSuccess", 4)
		mockMilvusClient.AssertNumberOfCalls(t, "Insert", 2)
		assertPosition.lock.Lock()
		defer assertPosition.lock.Unlock()
		assert.Equal(t, 3, assertPosition.saveNum)
		position := assertPosition.positions[int64(1001)]["a"]
		assert.Equal(t, "a", position.Key)
		assert.Equal(t, "d", string(position.Data))

		position = assertPosition.positions[int64(1001)]["b"]
		assert.Equal(t, "b", position.Key)
		assert.Equal(t, "b", string(position.Data))

		position = assertPosition.positions[int64(1002)]["a"]
		assert.Equal(t, "a", position.Key)
		assert.Equal(t, "c", string(position.Data))
	})

	t.Run("delete success", func(t *testing.T) {
		defer assertPosition.clear()

		writerCallback := mocks.NewWriteCallback(t)
		mockMilvusClient := mocks.NewMilvusClientApi(t)
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()
		successCallbackCall := writerCallback.On("OnSuccess", mock.Anything, mock.Anything).Return()
		defer successCallbackCall.Unset()

		deleteCall := mockMilvusClient.On("DeleteByPks", mock.Anything, mock.Anything, "", mock.Anything).Run(func(args mock.Arguments) {
			collectionName := args[1].(string)
			if collectionName == "col1" {
				boolColumn := args[3].(*entity.ColumnInt64)
				assert.Len(t, boolColumn.Data(), 9)
				assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 10, 11, 12}, boolColumn.Data())
			}
		}).Return(nil)
		defer deleteCall.Unset()
		cdcWriter := newWriter()

		err := cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "a", "part", "a", []int64{1, 2, 3}), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "b", "part", "b", []int64{4, 5, 6}), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateDeleteData(int64(1002), "col2", "a", "part", "c", []int64{7, 8, 9}), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "a", "part", "d", []int64{10, 11, 12}), writerCallback)
		assert.NoError(t, err)
		cdcWriter.Flush(context.Background())
		time.Sleep(time.Second)
		writerCallback.AssertCalled(t, "OnSuccess", int64(1001), mock.Anything) // 3
		writerCallback.AssertCalled(t, "OnSuccess", int64(1002), mock.Anything) // 1
		writerCallback.AssertNumberOfCalls(t, "OnSuccess", 4)
		mockMilvusClient.AssertNumberOfCalls(t, "DeleteByPks", 2)
		assertPosition.lock.Lock()
		defer assertPosition.lock.Unlock()
		assert.Equal(t, 3, assertPosition.saveNum)
		position := assertPosition.positions[int64(1001)]["a"]
		assert.Equal(t, "a", position.Key)
		assert.Equal(t, "d", string(position.Data))

		position = assertPosition.positions[int64(1001)]["b"]
		assert.Equal(t, "b", position.Key)
		assert.Equal(t, "b", string(position.Data))

		position = assertPosition.positions[int64(1002)]["a"]
		assert.Equal(t, "a", position.Key)
		assert.Equal(t, "c", string(position.Data))
	})

	t.Run("drop success", func(t *testing.T) {
		defer assertPosition.clear()

		writerCallback := mocks.NewWriteCallback(t)
		mockMilvusClient := mocks.NewMilvusClientApi(t)
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()
		successCallbackCall := writerCallback.On("OnSuccess", mock.Anything, mock.Anything).Return()
		defer successCallbackCall.Unset()
		insertCall := mockMilvusClient.On("Insert", mock.Anything, mock.Anything, "", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		defer insertCall.Unset()
		deleteCall := mockMilvusClient.On("DeleteByPks", mock.Anything, mock.Anything, "", mock.Anything).Return(nil)
		defer deleteCall.Unset()
		dropCall := mockMilvusClient.On("DropCollection", mock.Anything, mock.Anything).Return(nil)
		defer dropCall.Unset()
		cdcWriter := newWriter()

		err := cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "a", "part", "a"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "b", "part", "b"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "coll", "b", "part", "c", []int64{4, 5, 6}), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "a", "part", "d"), writerCallback)
		assert.NoError(t, err)
		err = cdcWriter.Write(context.Background(), generateDropData(int64(1001), "coll", "a", "e", "b", "f"), writerCallback)
		assert.NoError(t, err)

		time.Sleep(time.Second)
		writerCallback.AssertCalled(t, "OnSuccess", int64(1001), mock.Anything)
		writerCallback.AssertNumberOfCalls(t, "OnSuccess", 5)
		mockMilvusClient.AssertNumberOfCalls(t, "Insert", 2)
		mockMilvusClient.AssertNumberOfCalls(t, "DeleteByPks", 1)
		mockMilvusClient.AssertNumberOfCalls(t, "DropCollection", 1)
		assertPosition.lock.Lock()
		defer assertPosition.lock.Unlock()
		assert.Equal(t, 2, assertPosition.saveNum)
		position := assertPosition.positions[int64(1001)]["a"]
		assert.Equal(t, "a", position.Key)
		assert.Equal(t, "e", string(position.Data))

		position = assertPosition.positions[int64(1001)]["b"]
		assert.Equal(t, "b", position.Key)
		assert.Equal(t, "f", string(position.Data))
	})

	t.Run("flush", func(t *testing.T) {
		defer assertPosition.clear()

		writerCallback := mocks.NewWriteCallback(t)
		mockMilvusClient := mocks.NewMilvusClientApi(t)
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()
		successCallbackCall := writerCallback.On("OnSuccess", mock.Anything, mock.Anything).Return()
		defer successCallbackCall.Unset()
		deleteCall := mockMilvusClient.On("DeleteByPks", mock.Anything, mock.Anything, "", mock.Anything).Return(nil)
		defer deleteCall.Unset()

		cdcWriter := newWriter()
		err := cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "a", "part", "a", []int64{1, 2, 3}), writerCallback)
		assert.NoError(t, err)

		// wait the flush time
		time.Sleep(7 * time.Second)
		writerCallback.AssertCalled(t, "OnSuccess", int64(1001), mock.Anything)
		mockMilvusClient.AssertNumberOfCalls(t, "DeleteByPks", 1)
		assertPosition.lock.Lock()
		defer assertPosition.lock.Unlock()
		assert.Equal(t, 1, assertPosition.saveNum)
	})

	t.Run("err", func(t *testing.T) {
		defer assertPosition.clear()

		writerCallback := mocks.NewWriteCallback(t)
		mockMilvusClient := mocks.NewMilvusClientApi(t)
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()
		successCallbackCall := writerCallback.On("OnSuccess", mock.Anything, mock.Anything).Return()
		defer successCallbackCall.Unset()
		failCallbackCall := writerCallback.On("OnFail", mock.Anything, mock.Anything).Return()
		defer failCallbackCall.Unset()
		insertCall := mockMilvusClient.On("Insert", mock.Anything, mock.Anything, "", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		defer insertCall.Unset()
		deleteCall := mockMilvusClient.On("DeleteByPks", mock.Anything, mock.Anything, "", mock.Anything).Return(errors.New("delete error"))
		defer deleteCall.Unset()

		cdcWriter := newWriter()
		for i := 0; i < 3; i++ {
			err := cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "a", "part", "a", []int64{1, 2, 3}), writerCallback)
			assert.NoError(t, err)
		}
		cdcWriter.Flush(context.Background())
		err := cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "b", "part", "b"), writerCallback)
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
		for i := 0; i < 6; i++ {
			err := cdcWriter.Write(context.Background(), generateDeleteData(int64(1001), "col1", "a", "part", "a", []int64{1, 2, 3}), writerCallback)
			assert.NoError(t, err)
		}
		cdcWriter.Flush(context.Background())
		time.Sleep(time.Second)
		err = cdcWriter.Write(context.Background(), generateInsertData(int64(1001), "coll", "b", "part", "b"), writerCallback)
		assert.Error(t, err)
	})
}
