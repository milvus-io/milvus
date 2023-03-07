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

package reader_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/model"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/cdc/core/pb"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/reader"
)

var (
	endpoints        = []string{"localhost:2379"}
	rootPath         = "dev"
	metaSubPath      = "meta"
	collectionKey    = "root-coord/collection"
	filedKey         = "root-coord/fields"
	collectionPrefix = util.GetCollectionPrefix(rootPath, metaSubPath, collectionKey)
	fieldPrefix      = util.GetFieldPrefix(rootPath, metaSubPath, filedKey)
	etcdConfig       = config.NewMilvusEtcdConfig(config.MilvusEtcdEndpointsOption(endpoints),
		config.MilvusEtcdRootPathOption(rootPath),
		config.MilvusEtcdMetaSubPathOption(metaSubPath))
	pulsarConfig = config.NewPulsarConfig(
		config.PulsarAddressOption(fmt.Sprintf("pulsar://%s:%d", "localhost", 6650)),
		config.PulsarWebAddressOption("", 80),
		config.PulsarMaxMessageSizeOption(5242880),
		config.PulsarTenantOption("public", "default"),
	)
	kafkaConfig = config.NewKafkaConfig(config.KafkaAddressOption("localhost:9092"))
)

func TestNewMilvusCollectionReader(t *testing.T) {
	monitor := mocks.NewMonitor(t)
	var options []config.Option[*reader.MilvusCollectionReader]
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		call := mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(nil, errors.New("status error"))
		defer call.Unset()
		_, err := reader.NewMilvusCollectionReader(append(options,
			reader.EtcdOption(etcdConfig),
			reader.MqOption(config.PulsarConfig{}, config.KafkaConfig{Address: "address"}),
			reader.MonitorOption(monitor),
			reader.ChanLenOption(10))...)
		assert.Error(t, err)
	})

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		call := mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
		defer call.Unset()
		_, err := reader.NewMilvusCollectionReader(append(options,
			reader.EtcdOption(etcdConfig),
			reader.MqOption(pulsarConfig, config.KafkaConfig{}),
			reader.MonitorOption(monitor),
			reader.ChanLenOption(10))...)
		assert.NoError(t, err)
	})
}

func TestReaderGetCollectionInfo(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		call := mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
		defer call.Unset()
		collectionName1 := "coll1"
		collectionID1 := int64(100)
		collectionName2 := "coll2"
		collectionID2 := int64(200)

		factoryCreator := mocks.NewFactoryCreator(t)
		monitor := mocks.NewMonitor(t)

		util.EtcdOpRetryTime = 1
		defer func() {
			util.EtcdOpRetryTime = 5
		}()

		t.Run("get error", func(t *testing.T) {
			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, nil))
			options = append(options, reader.CollectionInfoOption(collectionName2, nil))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			getCall := mockEtcdCli.On("Get", mock.Anything, collectionPrefix+"/", mock.Anything).Return(nil, errors.New("get error"))
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(clientv3.WatchChan))
			unknownCall := monitor.On("OnFailUnKnowCollection", collectionPrefix, mock.Anything).Return()
			defer func() {
				watchCall.Unset()
				getCall.Unset()
				unknownCall.Unset()
			}()
			collectionReader.StartRead(context.Background())
			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())
			monitor.AssertCalled(t, "OnFailUnKnowCollection", collectionPrefix, mock.Anything)
		})

		info1 := &pb.CollectionInfo{
			ID: collectionID1,
			Schema: &schemapb.CollectionSchema{
				Name: collectionName1,
			},
		}
		byte1, _ := proto.Marshal(info1)
		info2 := &pb.CollectionInfo{
			ID: collectionID2,
			Schema: &schemapb.CollectionSchema{
				Name: collectionName2,
			},
		}
		byte2, _ := proto.Marshal(info2)
		field := &schemapb.FieldSchema{
			FieldID: 101,
		}
		fieldByte3, _ := proto.Marshal(field)

		t.Run("field error", func(t *testing.T) {
			getCall := mockEtcdCli.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, option ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if s == collectionPrefix+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: byte2,
							},
							{
								Value: byte1,
							},
						},
					}, nil
				}
				return nil, errors.New("get error")
			})
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(clientv3.WatchChan))
			unknownCall := monitor.On("OnFailUnKnowCollection", collectionPrefix, mock.Anything).Return()
			defer func() {
				getCall.Unset()
				watchCall.Unset()
				unknownCall.Unset()
			}()

			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, nil))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			collectionReader.StartRead(context.Background())
			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())
		})

		t.Run("no field error", func(t *testing.T) {
			getCall := mockEtcdCli.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, option ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if s == collectionPrefix+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: byte2,
							},
							{
								Value: byte1,
							},
						},
					}, nil
				} else if s == fieldPrefix+"/"+strconv.FormatInt(collectionID1, 10)+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{},
					}, nil
				}
				return nil, errors.New("get error")
			})
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(clientv3.WatchChan))
			unknownCall := monitor.On("OnFailUnKnowCollection", collectionPrefix, mock.Anything).Return()
			defer func() {
				getCall.Unset()
				watchCall.Unset()
				unknownCall.Unset()
			}()

			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, nil))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			collectionReader.StartRead(context.Background())
			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())
		})

		t.Run("success", func(t *testing.T) {
			getCall := mockEtcdCli.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, option ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if s == collectionPrefix+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: byte2,
							},
							{
								Value: byte1,
							},
						},
					}, nil
				} else if s == fieldPrefix+"/"+strconv.FormatInt(collectionID1, 10)+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: fieldByte3,
							},
						},
					}, nil
				}
				return nil, errors.New("get error")
			})
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(clientv3.WatchChan))
			asuccessCall := monitor.On("OnSuccessGetACollectionInfo", collectionID1, collectionName1).Return()
			successCall := monitor.On("OnSuccessGetAllCollectionInfo").Return()
			defer func() {
				getCall.Unset()
				watchCall.Unset()
				asuccessCall.Unset()
				successCall.Unset()
			}()

			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, nil))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			collectionReader.StartRead(context.Background())
			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())
		})
	})
}

func TestReaderWatchCollectionInfo(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	call := mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	defer call.Unset()
	collectionName1 := "coll1"
	collectionID1 := int64(100)
	factoryCreator := mocks.NewFactoryCreator(t)
	field := &schemapb.FieldSchema{
		FieldID: 101,
	}
	fieldByte3, _ := proto.Marshal(field)

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		monitor := mocks.NewMonitor(t)

		t.Run("watch chan close", func(t *testing.T) {
			getCall := mockEtcdCli.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, option ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if s == collectionPrefix+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{},
					}, nil
				} else if s == fieldPrefix+"/"+strconv.FormatInt(collectionID1, 10)+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: fieldByte3,
							},
						},
					}, nil
				}
				return nil, errors.New("get error")
			})
			watchChan := make(chan clientv3.WatchResponse)
			var onlyReadChan clientv3.WatchChan = watchChan
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(onlyReadChan)
			closeCall := monitor.On("WatchChanClosed").Return()

			defer func() {
				getCall.Unset()
				watchCall.Unset()
				closeCall.Unset()
			}()
			close(watchChan)

			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, nil))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			collectionReader.StartRead(context.Background())
			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())
		})
	})

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		monitor := mocks.NewMonitor(t)
		t.Run("send msg", func(t *testing.T) {
			msgStream1 := mocks.NewMsgStream(t)
			msgStream2 := mocks.NewMsgStream(t)
			factory := mocks.NewFactory(t)
			pmsCall := factoryCreator.EXPECT().NewPmsFactory(mock.Anything).RunAndReturn(func(c *config.PulsarConfig) msgstream.Factory {
				return factory
			})
			defer pmsCall.Unset()
			i := 0
			factory.EXPECT().NewMsgStream(mock.Anything).RunAndReturn(func(ctx context.Context) (msgstream.MsgStream, error) {
				if i == 0 {
					i++
					return msgStream1, nil
				}
				return msgStream2, nil
			})

			shardNum := int32(4)
			level := commonpb.ConsistencyLevel_Session
			kv := &commonpb.KeyValuePair{Key: "foo", Value: "123"}
			info1 := &pb.CollectionInfo{
				ID: collectionID1,
				Schema: &schemapb.CollectionSchema{
					Name: collectionName1,
				},
				State:            pb.CollectionState_CollectionCreated,
				ShardsNum:        shardNum,
				ConsistencyLevel: level,
				Properties: []*commonpb.KeyValuePair{
					kv,
				},
				VirtualChannelNames: []string{"p1_v1", "p2_v1"},
				StartPositions: []*commonpb.KeyDataPair{
					{
						Key:  "p1",
						Data: []byte("foo1"),
					},
					{
						Key:  "p2",
						Data: []byte("foo2"),
					},
				},
			}
			byte1, _ := proto.Marshal(info1)

			getCall := mockEtcdCli.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, option ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if s == collectionPrefix+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{},
					}, nil
				} else if s == fieldPrefix+"/"+strconv.FormatInt(collectionID1, 10)+"/" {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value: fieldByte3,
							},
						},
					}, nil
				}
				return nil, errors.New("get error")
			})
			watchChan := make(chan clientv3.WatchResponse, 10)
			var onlyReadChan clientv3.WatchChan = watchChan
			watchCall := mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(onlyReadChan)
			asuccessCall := monitor.On("OnSuccessGetACollectionInfo", collectionID1, collectionName1).Return()
			filterCall := monitor.On("OnFilterReadMsg", "Delete").Return()
			msgStream1.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything).Return()
			msgStream2.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything).Return()
			msgStream1.EXPECT().Seek(mock.Anything).RunAndReturn(func(positions []*msgstream.MsgPosition) error {
				if positions[0].ChannelName == "p1" {
					assert.EqualValues(t, []byte("hello"), positions[0].MsgID)
					return nil
				}
				return errors.New("consume error")
			})
			msgStream2.EXPECT().Seek(mock.Anything).RunAndReturn(func(positions []*msgstream.MsgPosition) error {
				if positions[0].ChannelName == "p2" {
					assert.EqualValues(t, []byte("foo2"), positions[0].MsgID)
					return nil
				}
				return errors.New("consume error")
			})
			ch1 := make(chan *msgstream.MsgPack, 10)
			ch2 := make(chan *msgstream.MsgPack, 10)
			msgStream1.EXPECT().Chan().Return(ch1)
			msgStream2.EXPECT().Chan().Return(ch2)
			defer func() {
				getCall.Unset()
				watchCall.Unset()
				asuccessCall.Unset()
				filterCall.Unset()
			}()

			var options []config.Option[*reader.MilvusCollectionReader]
			options = append(options, reader.CollectionInfoOption(collectionName1, map[string]*commonpb.KeyDataPair{
				"p1": {Key: "p1", Data: []byte("hello")},
			}))
			collectionReader, err := reader.NewMilvusCollectionReader(append(options,
				reader.FactoryCreatorOption(factoryCreator),
				reader.EtcdOption(etcdConfig),
				reader.MqOption(pulsarConfig, config.KafkaConfig{}),
				reader.MonitorOption(monitor),
				reader.ChanLenOption(10))...)
			assert.NoError(t, err)
			cdcChan := collectionReader.StartRead(context.Background())
			ch1 <- &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.TimeTickMsg{
						TimeTickMsg: msgpb.TimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_TimeTick}},
					},
					&msgstream.InsertMsg{
						InsertRequest: msgpb.InsertRequest{
							Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
							CollectionName: collectionName1,
						},
					},
					&msgstream.DropCollectionMsg{
						DropCollectionRequest: msgpb.DropCollectionRequest{
							Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
							CollectionName: collectionName1,
						},
					},
				},
			}
			ch2 <- &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.InsertMsg{
						InsertRequest: msgpb.InsertRequest{
							Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
							CollectionName: "xxxxx",
						},
					},
					&msgstream.DeleteMsg{
						DeleteRequest: msgpb.DeleteRequest{
							Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
							CollectionName: collectionName1,
						},
					},
					&msgstream.DropCollectionMsg{
						DropCollectionRequest: msgpb.DropCollectionRequest{
							Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
							CollectionName: collectionName1,
						},
					},
				},
			}

			watchChan <- clientv3.WatchResponse{
				Events: []*clientv3.Event{
					{
						Type: clientv3.EventTypePut,
						Kv: &mvccpb.KeyValue{
							Key:   []byte(collectionPrefix + "/" + strconv.FormatInt(collectionID1, 10)),
							Value: byte1,
						},
					},
				},
			}

			time.Sleep(time.Second)
			collectionReader.QuitRead(context.Background())

			//create message
			cdcData := <-cdcChan
			assert.EqualValues(t, shardNum, cdcData.Extra[model.ShardNumKey])
			assert.EqualValues(t, level, cdcData.Extra[model.ConsistencyLevelKey])
			receiveKv := cdcData.Extra[model.CollectionPropertiesKey].([]*commonpb.KeyValuePair)[0]
			assert.EqualValues(t, kv.Key, receiveKv.Key)
			assert.EqualValues(t, kv.Value, receiveKv.Value)
			createCollectionMsg := cdcData.Msg.(*msgstream.CreateCollectionMsg)
			assert.EqualValues(t, collectionID1, createCollectionMsg.CollectionID)
			assert.EqualValues(t, collectionName1, createCollectionMsg.CollectionName)
			schema := &schemapb.CollectionSchema{
				Name:   collectionName1,
				Fields: []*schemapb.FieldSchema{field},
			}
			schemaByte, _ := json.Marshal(schema)
			assert.EqualValues(t, schemaByte, createCollectionMsg.Schema)

			hasInsert, hasDelete := false, false
			checkInsertOrDelete := func(d *model.CDCData) {
				if insertMsg, ok := d.Msg.(*msgstream.InsertMsg); ok {
					hasInsert = true
					assert.Equal(t, collectionName1, insertMsg.CollectionName)
					return
				}
				if deleteMsg, ok := d.Msg.(*msgstream.DeleteMsg); ok {
					hasDelete = true
					assert.Equal(t, collectionName1, deleteMsg.CollectionName)
					return
				}
			}
			cdcData = <-cdcChan
			checkInsertOrDelete(cdcData)
			cdcData = <-cdcChan
			checkInsertOrDelete(cdcData)
			assert.True(t, hasInsert)
			assert.True(t, hasDelete)

			cdcData = <-cdcChan
			dropMsg := cdcData.Msg.(*msgstream.DropCollectionMsg)
			assert.Equal(t, collectionName1, dropMsg.CollectionName)
			assert.Len(t, cdcData.Extra[model.DropCollectionMsgsKey], 1)
		})
	})
}
