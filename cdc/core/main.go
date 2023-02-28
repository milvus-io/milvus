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

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/mq"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/samber/lo"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// go run main.go > cdc.log 2>&1
func main() {
	etcdcli, err := util.GetEtcdClient([]string{"localhost:2379"})
	if err != nil {
		panic("fail to get etcd")
	}
	// by-dev/meta/root-coord/collection/
	// "by-dev/meta/snapshots/root-coord/collection/"
	prefix := "by-dev/meta/root-coord/collection/"
	// TODO get the data from the prefix

	initMilvus()
	watchChan := etcdcli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	var collectionKeys []string
	fmt.Println("start...")
	util.Log.Info("test log", zap.String("foo", "hello"))
	for {
		select {
		case watchResp := <-watchChan:
			lo.ForEach(watchResp.Events, func(event *clientv3.Event, _ int) {
				if event.Type == clientv3.EventTypePut {
					collectionKey := util.ToString(event.Kv.Key)
					if slices.Contains(collectionKeys, collectionKey) {
						return
					}
					collectionKeys = append(collectionKeys, collectionKey)
					info := getCollectionsInfo(etcdcli, collectionKey)
					util.Log.Info("collection_info", zap.Any("info", info))

					var fields []*entity.Field
					lo.ForEach(info.Schema.Fields, func(schema *schemapb.FieldSchema, _ int) {
						fields = append(fields, &entity.Field{
							Name:        schema.Name,
							DataType:    entity.FieldType(schema.DataType),
							PrimaryKey:  schema.IsPrimaryKey,
							AutoID:      schema.AutoID,
							Description: schema.Description,
							TypeParams:  util.ConvertKVPairToMap(schema.TypeParams),
							IndexParams: util.ConvertKVPairToMap(schema.IndexParams),
						})
					})
					err := milvus.CreateCollection(context.Background(), &entity.Schema{
						CollectionName: info.Schema.Name,
						Description:    info.Schema.Description,
						AutoID:         info.Schema.AutoID,
						Fields:         fields,
					}, info.ShardsNum, client.WithConsistencyLevel(entity.ConsistencyLevel(info.ConsistencyLevel)))
					if err != nil {
						panic(err)
					}

					vchannels := info.VirtualChannelNames
					startPositions := info.StartPositions
					fmt.Println("vchannels", vchannels)
					w := &sync.WaitGroup{}
					dropCollectionChannel := make(chan string, 1)
					w.Add(len(vchannels))
					lo.ForEach(vchannels, func(s string, _ int) {
						position := toMsgPosition(s, startPositions)
						name, c := getMsgChan(s, position)
						readMsg(name, c, w, dropCollectionChannel)
					})
					go func() {
						w.Wait()
						name := <-dropCollectionChannel
						fmt.Println("execute drop collection", name)
						err := milvus.DropCollection(context.Background(), name)
						if err != nil {
							panic(err)
						}
					}()
				}
			})
		}
	}
}

func getCollectionsInfo(cli *clientv3.Client, collectionKey string) *pb.CollectionInfo {
	collResp, err := cli.Get(context.Background(), collectionKey)
	if err != nil {
		// TODO fubang
		panic(err)
		return nil
	}
	if len(collResp.Kvs) == 0 {
		err = errors.New("empty response")
		// TODO fubang
		panic(err)
		return nil
	}
	info := &pb.CollectionInfo{}
	err = proto.Unmarshal(collResp.Kvs[0].Value, info)

	if err != nil {
		// TODO fubang
		panic(err)
		return nil
	}
	// by-dev/meta/root-coord/fields
	// "by-dev/meta/snapshots/root-coord/fields"
	filedPrefix := "by-dev/meta/root-coord/fields"
	for {
		resp, err := cli.Get(context.Background(), fmt.Sprintf("%s/%d/", filedPrefix, info.ID), clientv3.WithPrefix())
		if err != nil {
			panic(err)
		}
		fmt.Println("field_key", fmt.Sprintf("%s/%d/", filedPrefix, info.ID))
		fmt.Println("len(kvs),", len(resp.Kvs))
		// TODO fubang the field is later put than the collection info
		if len(resp.Kvs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var fields []*schemapb.FieldSchema
		lo.ForEach(resp.Kvs, func(kv *mvccpb.KeyValue, _ int) {
			field := &schemapb.FieldSchema{}
			err = proto.Unmarshal(kv.Value, field)
			if err != nil {
				panic(err)
			}
			if field.FieldID >= 100 {
				fields = append(fields, field)
			}
		})
		info.Schema.Fields = fields
		break
	}

	return info
}

func toMsgPosition(vchannel string, startPositions []*commonpb.KeyDataPair) *pb.MsgPosition {
	for _, sp := range startPositions {
		if sp.GetKey() != util.ToPhysicalChannel(vchannel) {
			continue
		}
		return &pb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       sp.GetData(),
		}
	}
	panic("Not found position")
}

func getMsgChan(vchannel string, position *pb.MsgPosition) (string, <-chan *api.MsgPack) {

	var (
		consumeSubName = vchannel + string(rand.Int31())
		steam          api.MsgStream
		err            error
	)

	// TODO fubang
	//kafkaStream := func() {
	//	cfg := &config.KafkaConfig{
	//		Address:             "localhost1:9092",
	//		ConsumerExtraConfig: map[string]string{"client.id": "dc1"},
	//		ProducerExtraConfig: map[string]string{"client.id": "dc"},
	//	}
	//	factory := mq.NewKmsFactory(cfg)
	//	steam, err = factory.NewMsgStream(context.Background())
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//kafkaStream()

	pulsarStream := func() {
		cfg := &config.PulsarConfig{
			Address:        "pulsar://localhost:6650",
			WebPort:        80,
			MaxMessageSize: "5242880",
			Tenant:         "public",
			Namespace:      "default",
		}
		factory := mq.NewPmsFactory(cfg)
		steam, err = factory.NewMsgStream(context.Background())
		if err != nil {
			panic(err)
		}
	}
	pulsarStream()

	pchannelName := util.ToPhysicalChannel(vchannel)
	if position != nil {
		steam.AsConsumer([]string{pchannelName}, consumeSubName, api.SubscriptionPositionUnknown)
		position.ChannelName = pchannelName
		err = steam.Seek([]*pb.MsgPosition{position})
		if err != nil {
			panic(err)
		}
	} else {
		steam.AsConsumer([]string{pchannelName}, consumeSubName, api.SubscriptionPositionEarliest)
	}
	return consumeSubName, steam.Chan()
}

func readMsg(name string, c <-chan *api.MsgPack, w *sync.WaitGroup, dropCollectionChan chan<- string) {
	go func() {
		for {
			msg := <-c
			lo.ForEach[api.TsMsg](msg.Msgs, func(item api.TsMsg, index int) {
				msgType := item.Type().String()
				if msgType != "TimeTick" {
					fmt.Println("consumerName:", name, ", msgType:", msgType)
					if insertMsg, ok := item.(*api.InsertMsg); ok {
						fmt.Println("insertMsg, row:", insertMsg.NumRows)
					}
					writeMilvus(item, w, dropCollectionChan)
					fmt.Println("-------------------")
					// TODO fubang should return when drop
				}
			})
		}
	}()
}

var milvus client.Client

func initMilvus() {
	timeoutContext, _ := context.WithTimeout(context.Background(), 5*time.Second)
	milvusClient, err := client.NewDefaultGrpcClientWithTLSAuth(timeoutContext,
		"in01-b7766436294c416.aws-us-west-2.vectordb-uat3.zillizcloud.com:19530",
		"db_admin", "Cdc123456")
	if err != nil {
		panic(err)
	}
	milvus = milvusClient
}

func writeMilvus(msg api.TsMsg, w *sync.WaitGroup, dropCollectionChan chan<- string) {
	switch msg.(type) {
	case *api.CreateCollectionMsg:
		util.Log.Info("CreateCollectionMsg")
	case *api.CreatePartitionMsg:
		// TODO fubang
		util.Log.Info("CreatePartitionMsg")
	case *api.InsertMsg:
		insertMsg := msg.(*api.InsertMsg)
		var columns []entity.Column
		lo.ForEach(insertMsg.FieldsData, func(data *schemapb.FieldData, _ int) {
			if c, err := entity.FieldDataColumn(data, 0, -1); err == nil {
				columns = append(columns, c)
			} else {
				c, err := entity.FieldDataVector(data)
				if err != nil {
					panic(err)
				}
				columns = append(columns, c)
			}
		})
		_, err := milvus.Insert(context.Background(), insertMsg.CollectionName, insertMsg.PartitionName, columns...)
		if err != nil {
			panic(err)
		}
	case *api.DeleteMsg:
		deleteMsg := msg.(*api.DeleteMsg)
		c, err := entity.IDColumns(deleteMsg.PrimaryKeys, 0, -1)
		if err != nil {
			panic(err)
		}
		err = milvus.DeleteByPks(context.Background(), deleteMsg.CollectionName, deleteMsg.PartitionName, c)
		if err != nil {
			panic(err)
		}
	case *api.DropCollectionMsg:
		w.Done()
		dropMsg := msg.(*api.DropCollectionMsg)
		select {
		case dropCollectionChan <- dropMsg.CollectionName:
			fmt.Println("DropCollectionMsg", dropMsg.CollectionName)
		default:
		}
	case *api.DropPartitionMsg:
		dropMsg := msg.(*api.DropPartitionMsg)
		err := milvus.DropPartition(context.Background(), dropMsg.CollectionName, dropMsg.PartitionName)
		if err != nil {
			panic(err)
		}
	default:
		util.Log.Info("ignore msg", zap.Any("type", msg.Type()))
	}
}
