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

package rmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

type fixture struct {
	t  *testing.T
	kv kv.MetaKv
}

type parameters struct {
	client mqwrapper.Client
}

func (f *fixture) setup() []parameters {
	rocksdbName := "/tmp/rocksmq_unittest_" + f.t.Name()
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	defer etcdCli.Close()
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	f.kv = etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", f.kv)
	_ = idAllocator.Initialize()
	err = server.InitRmq(rocksdbName, idAllocator)
	if err != nil {
		log.Fatalf("InitRmq error = %v", err)
	}

	rmqClient, _ := NewClientWithDefaultOptions()

	parameters := []parameters{
		{rmqClient},
	}
	return parameters
}

func (f *fixture) teardown() {
	rocksdbName := "/tmp/rocksmq_unittest_" + f.t.Name()

	server.CloseRocksMQ()
	f.kv.Close()
	_ = os.RemoveAll(rocksdbName)
	_ = os.RemoveAll(rocksdbName + "_meta_kv")
}

func Test_NewMqMsgStream(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			_, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)
		}(parameters[i].client)
	}
}

// TODO(wxyu): add a mock implement of mqwrapper.Client, then inject errors to improve coverage
func TestMqMsgStream_AsProducer(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// empty channel name
			m.AsProducer([]string{""})
		}(parameters[i].client)
	}
}

// TODO(wxyu): add a mock implement of mqwrapper.Client, then inject errors to improve coverage
func TestMqMsgStream_AsConsumer(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// repeat calling AsConsumer
			m.AsConsumer([]string{"a"}, "b", mqwrapper.SubscriptionPositionUnknown)
			m.AsConsumer([]string{"a"}, "b", mqwrapper.SubscriptionPositionUnknown)
		}(parameters[i].client)
	}
}

func TestMqMsgStream_ComputeProduceChannelIndexes(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// empty parameters
			reBucketValues := m.ComputeProduceChannelIndexes([]msgstream.TsMsg{})
			assert.Nil(t, reBucketValues)

			// not called AsProducer yet
			insertMsg := &msgstream.InsertMsg{
				BaseMsg: generateBaseMsg(),
				InsertRequest: msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						MsgID:     1,
						Timestamp: 2,
						SourceID:  3,
					},

					DbName:         "test_db",
					CollectionName: "test_collection",
					PartitionName:  "test_partition",
					DbID:           4,
					CollectionID:   5,
					PartitionID:    6,
					SegmentID:      7,
					ShardName:      "test-channel",
					Timestamps:     []uint64{2, 1, 3},
					RowData:        []*commonpb.Blob{},
				},
			}
			reBucketValues = m.ComputeProduceChannelIndexes([]msgstream.TsMsg{insertMsg})
			assert.Nil(t, reBucketValues)
		}(parameters[i].client)
	}
}

func TestMqMsgStream_GetProduceChannels(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// empty if not called AsProducer yet
			chs := m.GetProduceChannels()
			assert.Equal(t, 0, len(chs))

			// not empty after AsProducer
			m.AsProducer([]string{"a"})
			chs = m.GetProduceChannels()
			assert.Equal(t, 1, len(chs))
		}(parameters[i].client)
	}
}

func TestMqMsgStream_Produce(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// Produce before called AsProducer
			insertMsg := &msgstream.InsertMsg{
				BaseMsg: generateBaseMsg(),
				InsertRequest: msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						MsgID:     1,
						Timestamp: 2,
						SourceID:  3,
					},

					DbName:         "test_db",
					CollectionName: "test_collection",
					PartitionName:  "test_partition",
					DbID:           4,
					CollectionID:   5,
					PartitionID:    6,
					SegmentID:      7,
					ShardName:      "test-channel",
					Timestamps:     []uint64{2, 1, 3},
					RowData:        []*commonpb.Blob{},
				},
			}
			msgPack := &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{insertMsg},
			}
			err = m.Produce(msgPack)
			assert.Error(t, err)
		}(parameters[i].client)
	}
}

func TestMqMsgStream_Broadcast(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// Broadcast nil pointer
			_, err = m.Broadcast(nil)
			assert.Error(t, err)
		}(parameters[i].client)
	}
}

func TestMqMsgStream_Consume(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			// Consume return nil when ctx canceled
			var wg sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			m, err := msgstream.NewMqMsgStream(ctx, 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			wg.Add(1)
			go func() {
				defer wg.Done()
				msgPack := consumer(ctx, m)
				assert.Nil(t, msgPack)
			}()

			cancel()
			wg.Wait()
		}(parameters[i].client)
	}
}

func consumer(ctx context.Context, mq msgstream.MsgStream) *msgstream.MsgPack {
	for {
		select {
		case msgPack, ok := <-mq.Chan():
			if !ok {
				panic("Should not reach here")
			}
			return msgPack
		case <-ctx.Done():
			return nil
		}
	}
}

func TestMqMsgStream_Chan(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			ch := m.Chan()
			assert.NotNil(t, ch)
		}(parameters[i].client)
	}
}

func TestMqMsgStream_SeekNotSubscribed(t *testing.T) {
	f := &fixture{t: t}
	parameters := f.setup()
	defer f.teardown()

	factory := &msgstream.ProtoUDFactory{}
	for i := range parameters {
		func(client mqwrapper.Client) {
			m, err := msgstream.NewMqMsgStream(context.Background(), 100, 100, client, factory.NewUnmarshalDispatcher())
			assert.NoError(t, err)

			// seek in not subscribed channel
			p := []*msgpb.MsgPosition{
				{
					ChannelName: "b",
				},
			}
			err = m.Seek(p)
			assert.Error(t, err)
		}(parameters[i].client)
	}
}

func generateBaseMsg() msgstream.BaseMsg {
	ctx := context.Background()
	return msgstream.BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: msgstream.Timestamp(0),
		EndTimestamp:   msgstream.Timestamp(1),
		HashValues:     []uint32{2},
		MsgPosition:    nil,
	}
}

/****************************************Rmq test******************************************/

func initRmq(name string) kv.MetaKv {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	kv := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", kv)
	_ = idAllocator.Initialize()

	err = server.InitRmq(name, idAllocator)

	if err != nil {
		log.Fatalf("InitRmq error = %v", err)
	}
	return kv
}

func Close(rocksdbName string, intputStream, outputStream msgstream.MsgStream, kv kv.MetaKv) {
	server.CloseRocksMQ()
	intputStream.Close()
	outputStream.Close()
	kv.Close()
	err := os.RemoveAll(rocksdbName)
	_ = os.RemoveAll(rocksdbName + "_meta_kv")
	log.Println(err)
}

func initRmqStream(ctx context.Context,
	producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...msgstream.RepackFunc,
) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	rmqClient, _ := NewClientWithDefaultOptions()
	inputStream, _ := msgstream.NewMqMsgStream(ctx, 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	var input msgstream.MsgStream = inputStream

	rmqClient2, _ := NewClientWithDefaultOptions()
	outputStream, _ := msgstream.NewMqMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName, mqwrapper.SubscriptionPositionEarliest)
	var output msgstream.MsgStream = outputStream

	return input, output
}

func initRmqTtStream(ctx context.Context,
	producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...msgstream.RepackFunc,
) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	rmqClient, _ := NewClientWithDefaultOptions()
	inputStream, _ := msgstream.NewMqMsgStream(ctx, 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	var input msgstream.MsgStream = inputStream

	rmqClient2, _ := NewClientWithDefaultOptions()
	outputStream, _ := msgstream.NewMqTtMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName, mqwrapper.SubscriptionPositionEarliest)
	var output msgstream.MsgStream = outputStream

	return input, output
}

func TestStream_RmqMsgStream_Insert(t *testing.T) {
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerGroupName := "InsertGroup"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	rocksdbName := "/tmp/rocksmq_insert"
	kv := initRmq(rocksdbName)
	ctx := context.Background()
	inputStream, outputStream := initRmqStream(ctx, producerChannels, consumerChannels, consumerGroupName)
	err := inputStream.Produce(&msgPack)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, kv)
}

func TestStream_RmqTtMsgStream_Insert(t *testing.T) {
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	rocksdbName := "/tmp/rocksmq_insert_tt"
	kv := initRmq(rocksdbName)
	ctx := context.Background()
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	err = inputStream.Produce(&msgPack1)
	require.NoErrorf(t, err, fmt.Sprintf("produce error = %v", err))

	_, err = inputStream.Broadcast(&msgPack2)
	require.NoErrorf(t, err, fmt.Sprintf("broadcast error = %v", err))

	receiveMsg(ctx, outputStream, len(msgPack1.Msgs))
	Close(rocksdbName, inputStream, outputStream, kv)
}

func TestStream_RmqTtMsgStream_DuplicatedIDs(t *testing.T) {
	rocksdbName := "/tmp/rocksmq_tt_msg_seek"
	kv := initRmq(rocksdbName)

	c1 := funcutil.RandomString(8)
	producerChannels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))

	// would not dedup for non-dml messages
	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTsMsg(commonpb.MsgType_CreateCollection, 2))
	msgPack2.Msgs = append(msgPack2.Msgs, getTsMsg(commonpb.MsgType_CreateCollection, 2))

	msgPack3 := msgstream.MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTimeTickMsg(15))

	ctx := context.Background()
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack1)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack2)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack3)
	assert.NoError(t, err)

	receivedMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg.Msgs), 3)
	assert.Equal(t, receivedMsg.BeginTs, uint64(0))
	assert.Equal(t, receivedMsg.EndTs, uint64(15))

	outputStream.Close()

	factory := msgstream.ProtoUDFactory{}

	rmqClient, _ := NewClientWithDefaultOptions()
	outputStream, _ = msgstream.NewMqTtMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	consumerSubName = funcutil.RandomString(8)
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionUnknown)
	outputStream.Seek(receivedMsg.StartPositions)
	seekMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg.Msgs), 1+2)
	assert.EqualValues(t, seekMsg.Msgs[0].BeginTs(), 1)
	assert.Equal(t, commonpb.MsgType_CreateCollection, seekMsg.Msgs[1].Type())
	assert.Equal(t, commonpb.MsgType_CreateCollection, seekMsg.Msgs[2].Type())

	Close(rocksdbName, inputStream, outputStream, kv)
}

func TestStream_RmqTtMsgStream_Seek(t *testing.T) {
	rocksdbName := "/tmp/rocksmq_tt_msg_seek"
	kv := initRmq(rocksdbName)

	c1 := funcutil.RandomString(8)
	producerChannels := []string{c1}
	consumerChannels := []string{c1}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 3))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_Insert, 19))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5))

	msgPack3 := msgstream.MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 14))
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_Insert, 9))

	msgPack4 := msgstream.MsgPack{}
	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11))

	msgPack5 := msgstream.MsgPack{}
	msgPack5.Msgs = append(msgPack5.Msgs, getTsMsg(commonpb.MsgType_Insert, 12))
	msgPack5.Msgs = append(msgPack5.Msgs, getTsMsg(commonpb.MsgType_Insert, 13))

	msgPack6 := msgstream.MsgPack{}
	msgPack6.Msgs = append(msgPack6.Msgs, getTimeTickMsg(15))

	msgPack7 := msgstream.MsgPack{}
	msgPack7.Msgs = append(msgPack7.Msgs, getTimeTickMsg(20))

	ctx := context.Background()
	inputStream, outputStream := initRmqTtStream(ctx, producerChannels, consumerChannels, consumerSubName)

	_, err := inputStream.Broadcast(&msgPack0)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack1)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack2)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack3)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack4)
	assert.NoError(t, err)
	err = inputStream.Produce(&msgPack5)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack6)
	assert.NoError(t, err)
	_, err = inputStream.Broadcast(&msgPack7)
	assert.NoError(t, err)

	receivedMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg.Msgs), 2)
	assert.Equal(t, receivedMsg.BeginTs, uint64(0))
	assert.Equal(t, receivedMsg.EndTs, uint64(5))

	assert.Equal(t, receivedMsg.StartPositions[0].Timestamp, uint64(0))
	assert.Equal(t, receivedMsg.EndPositions[0].Timestamp, uint64(5))

	receivedMsg2 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg2.Msgs), 1)
	assert.Equal(t, receivedMsg2.BeginTs, uint64(5))
	assert.Equal(t, receivedMsg2.EndTs, uint64(11))
	assert.Equal(t, receivedMsg2.StartPositions[0].Timestamp, uint64(5))
	assert.Equal(t, receivedMsg2.EndPositions[0].Timestamp, uint64(11))

	receivedMsg3 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg3.Msgs), 3)
	assert.Equal(t, receivedMsg3.BeginTs, uint64(11))
	assert.Equal(t, receivedMsg3.EndTs, uint64(15))
	assert.Equal(t, receivedMsg3.StartPositions[0].Timestamp, uint64(11))
	assert.Equal(t, receivedMsg3.EndPositions[0].Timestamp, uint64(15))

	receivedMsg4 := consumer(ctx, outputStream)
	assert.Equal(t, len(receivedMsg4.Msgs), 1)
	assert.Equal(t, receivedMsg4.BeginTs, uint64(15))
	assert.Equal(t, receivedMsg4.EndTs, uint64(20))
	assert.Equal(t, receivedMsg4.StartPositions[0].Timestamp, uint64(15))
	assert.Equal(t, receivedMsg4.EndPositions[0].Timestamp, uint64(20))

	outputStream.Close()

	factory := msgstream.ProtoUDFactory{}

	rmqClient, _ := NewClientWithDefaultOptions()
	outputStream, _ = msgstream.NewMqTtMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	consumerSubName = funcutil.RandomString(8)
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionUnknown)

	outputStream.Seek(receivedMsg3.StartPositions)
	seekMsg := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg.Msgs), 3)
	result := []uint64{14, 12, 13}
	for i, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), result[i])
	}

	seekMsg2 := consumer(ctx, outputStream)
	assert.Equal(t, len(seekMsg2.Msgs), 1)
	for _, msg := range seekMsg2.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(19))
	}

	Close(rocksdbName, inputStream, outputStream, kv)
}

func TestStream_RMqMsgStream_SeekInvalidMessage(t *testing.T) {
	rocksdbName := "/tmp/rocksmq_tt_msg_seekInvalid"
	kv := initRmq(rocksdbName)
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	ctx := context.Background()
	inputStream, outputStream := initRmqStream(ctx, producerChannels, consumerChannels, consumerSubName)

	msgPack := &msgstream.MsgPack{}
	for i := 0; i < 10; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err := inputStream.Produce(msgPack)
	assert.NoError(t, err)
	var seekPosition *msgpb.MsgPosition
	for i := 0; i < 10; i++ {
		result := consumer(ctx, outputStream)
		assert.Equal(t, result.Msgs[0].ID(), int64(i))
		seekPosition = result.EndPositions[0]
	}
	outputStream.Close()

	factory := msgstream.ProtoUDFactory{}
	rmqClient2, _ := NewClientWithDefaultOptions()
	outputStream2, _ := msgstream.NewMqMsgStream(ctx, 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream2.AsConsumer(consumerChannels, funcutil.RandomString(8), mqwrapper.SubscriptionPositionUnknown)

	id := common.Endian.Uint64(seekPosition.MsgID) + 10
	bs := make([]byte, 8)
	common.Endian.PutUint64(bs, id)
	p := []*msgpb.MsgPosition{
		{
			ChannelName: seekPosition.ChannelName,
			Timestamp:   seekPosition.Timestamp,
			MsgGroup:    seekPosition.MsgGroup,
			MsgID:       bs,
		},
	}

	err = outputStream2.Seek(p)
	assert.NoError(t, err)

	for i := 10; i < 20; i++ {
		insertMsg := getTsMsg(commonpb.MsgType_Insert, int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}
	err = inputStream.Produce(msgPack)
	assert.NoError(t, err)

	result := consumer(ctx, outputStream2)
	assert.Equal(t, result.Msgs[0].ID(), int64(1))

	Close(rocksdbName, inputStream, outputStream2, kv)
}

func TestStream_RmqTtMsgStream_AsConsumerWithPosition(t *testing.T) {
	producerChannels := []string{"insert1"}
	consumerChannels := []string{"insert1"}
	consumerSubName := "subInsert"

	rocksdbName := "/tmp/rocksmq_asconsumer_withpos"
	kv := initRmq(rocksdbName)
	factory := msgstream.ProtoUDFactory{}

	rmqClient, _ := NewClientWithDefaultOptions()

	otherInputStream, _ := msgstream.NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	otherInputStream.AsProducer([]string{"root_timetick"})
	otherInputStream.Produce(getTimeTickMsgPack(999))

	inputStream, _ := msgstream.NewMqMsgStream(context.Background(), 100, 100, rmqClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)

	for i := 0; i < 100; i++ {
		inputStream.Produce(getTimeTickMsgPack(int64(i)))
	}

	rmqClient2, _ := NewClientWithDefaultOptions()
	outputStream, _ := msgstream.NewMqMsgStream(context.Background(), 100, 100, rmqClient2, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName, mqwrapper.SubscriptionPositionLatest)

	inputStream.Produce(getTimeTickMsgPack(1000))
	pack := <-outputStream.Chan()
	assert.NotNil(t, pack)
	assert.Equal(t, 1, len(pack.Msgs))
	assert.EqualValues(t, 1000, pack.Msgs[0].BeginTs())

	Close(rocksdbName, inputStream, outputStream, kv)
}

func getTimeTickMsgPack(reqID msgstream.UniqueID) *msgstream.MsgPack {
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTimeTickMsg(reqID))
	return &msgPack
}

func getTsMsg(msgType msgstream.MsgType, reqID msgstream.UniqueID) msgstream.TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		insertRequest := msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     reqID,
				Timestamp: time,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ShardName:      "0",
			Timestamps:     []msgstream.Timestamp{time},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &msgstream.InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		return insertMsg
	case commonpb.MsgType_CreateCollection:
		createCollectionRequest := msgpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			DbName:               "test_db",
			CollectionName:       "test_collection",
			PartitionName:        "test_partition",
			DbID:                 4,
			CollectionID:         5,
			PartitionID:          6,
			Schema:               []byte{},
			VirtualChannelNames:  []string{},
			PhysicalChannelNames: []string{},
		}
		createCollectionMsg := &msgstream.CreateCollectionMsg{
			BaseMsg:                 baseMsg,
			CreateCollectionRequest: createCollectionRequest,
		}
		return createCollectionMsg
	}
	return nil
}

func getTimeTickMsg(reqID msgstream.UniqueID) msgstream.TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	timeTickResult := msgpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	return timeTickMsg
}

func receiveMsg(ctx context.Context, outputStream msgstream.MsgStream, msgCount int) {
	receiveCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-outputStream.Chan():
			if !ok || result == nil || len(result.Msgs) == 0 {
				return
			}
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					receiveCount++
					log.Println("msg type: ", v.Type(), ", msg value: ", v)
				}
				log.Println("================")
			}
			if receiveCount >= msgCount {
				return
			}
		}
	}
}
