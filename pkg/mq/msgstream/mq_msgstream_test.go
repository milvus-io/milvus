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

package msgstream

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	pulsarwrapper "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	DefaultPulsarTenant    = "public"
	DefaultPulsarNamespace = "default"
)

var Params *paramtable.ComponentParam

func TestMain(m *testing.M) {
	paramtable.Init()
	Params = paramtable.Get()
	mockKafkaCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		// nolint
		fmt.Printf("Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}
	defer mockKafkaCluster.Close()
	broker := mockKafkaCluster.BootstrapServers()
	Params.Save("kafka.brokerList", broker)
	// Disable pursuit mode for unit test by default
	Params.Save(Params.ServiceParam.MQCfg.EnablePursuitMode.Key, "false")

	exitCode := m.Run()
	os.Exit(exitCode)
}

func getPulsarAddress() string {
	pulsarAddress := Params.PulsarCfg.Address.GetValue()
	if len(pulsarAddress) != 0 {
		return pulsarAddress
	}
	panic("invalid pulsar address")
}

func getKafkaBrokerList() string {
	brokerList := Params.KafkaCfg.Address.GetValue()
	log.Printf("kafka broker list: %s", brokerList)
	return brokerList
}

func consumer(ctx context.Context, mq MsgStream) *ConsumeMsgPack {
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

func createRandMsgPacks(msgsInPack int, numOfMsgPack int, deltaTs int) []*MsgPack {
	msgPacks := make([]*MsgPack, numOfMsgPack)

	// generate MsgPack
	for i := 0; i < numOfMsgPack; i++ {
		if i%2 == 0 {
			msgPacks[i] = getRandInsertMsgPack(msgsInPack, i/2*deltaTs, (i/2+2)*deltaTs+2)
		} else {
			msgPacks[i] = getTimeTickMsgPack(int64((i + 1) / 2 * deltaTs))
		}
	}
	msgPacks = append(msgPacks, nil)
	msgPacks = append(msgPacks, getTimeTickMsgPack(int64(numOfMsgPack*deltaTs)))
	return msgPacks
}

func createMsgPacks(ts [][]int, numOfMsgPack int, deltaTs int) []*MsgPack {
	msgPacks := make([]*MsgPack, numOfMsgPack)

	// generate MsgPack
	for i := 0; i < numOfMsgPack; i++ {
		if i%2 == 0 {
			msgPacks[i] = getInsertMsgPack(ts[i/2])
		} else {
			msgPacks[i] = getTimeTickMsgPack(int64((i + 1) / 2 * deltaTs))
		}
	}
	msgPacks = append(msgPacks, nil)
	msgPacks = append(msgPacks, getTimeTickMsgPack(int64(numOfMsgPack*deltaTs)))
	return msgPacks
}

func sendMsgPacks(ms MsgStream, msgPacks []*MsgPack) error {
	log.Println("==============produce msg==================")
	for i := 0; i < len(msgPacks); i++ {
		printMsgPack(msgPacks[i])
		if i%2 == 0 {
			// insert msg use Produce
			if err := ms.Produce(context.TODO(), msgPacks[i]); err != nil {
				return err
			}
		} else {
			// tt msg use Broadcast
			if _, err := ms.Broadcast(context.TODO(), msgPacks[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ TsMsg = (*MarshalFailTsMsg)(nil)

type MarshalFailTsMsg struct {
	BaseMsg
}

func (t *MarshalFailTsMsg) ID() UniqueID {
	return 0
}

func (t *MarshalFailTsMsg) SetID(id UniqueID) {
	// do nothing
}

func (t *MarshalFailTsMsg) Type() MsgType {
	return commonpb.MsgType_Undefined
}

func (t *MarshalFailTsMsg) SourceID() int64 {
	return -1
}

func (t *MarshalFailTsMsg) Marshal(_ TsMsg) (MarshalType, error) {
	return nil, errors.New("mocked error")
}

func (t *MarshalFailTsMsg) Unmarshal(_ MarshalType) (TsMsg, error) {
	return nil, errors.New("mocked error")
}

func (t *MarshalFailTsMsg) Size() int {
	return 0
}

var _ mqwrapper.Producer = (*mockSendFailProducer)(nil)

type mockSendFailProducer struct {
	mqwrapper.Producer
}

func (p *mockSendFailProducer) Send(_ context.Context, _ *common.ProducerMessage) (MessageID, error) {
	return nil, errors.New("mocked error")
}

/* ========================== Utility functions ========================== */
func repackFunc(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range msgs {
		keys := hashKeys[i]
		for _, channelID := range keys {
			_, ok := result[channelID]
			if ok == false {
				msgPack := MsgPack{}
				result[channelID] = &msgPack
			}
			result[channelID].Msgs = append(result[channelID].Msgs, request)
		}
	}
	return result, nil
}

func getTsMsg(msgType MsgType, reqID UniqueID) TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	switch msgType {
	case commonpb.MsgType_Insert:
		insertRequest := &msgpb.InsertRequest{
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
			Timestamps:     []Timestamp{time},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &InsertMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: 0,
				EndTimestamp:   0,
				HashValues:     []uint32{hashValue},
			},
			InsertRequest: insertRequest,
		}
		return insertMsg
	case commonpb.MsgType_Delete:
		deleteRequest := &msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName:   "Collection",
			ShardName:        "1",
			Timestamps:       []Timestamp{time},
			Int64PrimaryKeys: []int64{1},
			NumRows:          1,
		}
		deleteMsg := &DeleteMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: 0,
				EndTimestamp:   0,
				HashValues:     []uint32{hashValue},
			},
			DeleteRequest: deleteRequest,
		}
		return deleteMsg
	case commonpb.MsgType_CreateCollection:
		createCollectionRequest := &msgpb.CreateCollectionRequest{
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
		createCollectionMsg := &CreateCollectionMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: 0,
				EndTimestamp:   0,
				HashValues:     []uint32{hashValue},
			},
			CreateCollectionRequest: createCollectionRequest,
		}
		return createCollectionMsg
	case commonpb.MsgType_DropCollection:
		dropCollectionRequest := &msgpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     reqID,
				Timestamp: time,
				SourceID:  reqID,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			DbID:           4,
			CollectionID:   5,
		}
		dropCollectionMsg := &DropCollectionMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: 0,
				EndTimestamp:   0,
				HashValues:     []uint32{hashValue},
			},
			DropCollectionRequest: dropCollectionRequest,
		}
		return dropCollectionMsg
	case commonpb.MsgType_TimeTick:
		timeTickResult := &msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: 0,
				EndTimestamp:   0,
				HashValues:     []uint32{hashValue},
			},
			TimeTickMsg: timeTickResult,
		}
		return timeTickMsg
	}
	return nil
}

func getTimeTickMsg(reqID UniqueID) TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	timeTickResult := &msgpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
	}
	timeTickMsg := &TimeTickMsg{
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{hashValue},
		},
		TimeTickMsg: timeTickResult,
	}
	return timeTickMsg
}

// Generate MsgPack contains 'num' msgs, with timestamp in (start, end)
func getRandInsertMsgPack(num int, start int, end int) *MsgPack {
	Rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	set := make(map[int]bool)
	msgPack := MsgPack{}
	for len(set) < num {
		reqID := Rand.Int()%(end-start-1) + start + 1
		_, ok := set[reqID]
		if !ok {
			set[reqID] = true
			msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(reqID))) // getTsMsg(commonpb.MsgType_Insert, int64(reqID)))
		}
	}
	return &msgPack
}

func getInsertMsgPack(ts []int) *MsgPack {
	msgPack := MsgPack{}
	for i := 0; i < len(ts); i++ {
		msgPack.Msgs = append(msgPack.Msgs, getInsertMsgUniqueID(int64(ts[i]))) // getTsMsg(commonpb.MsgType_Insert, int64(ts[i])))
	}
	return &msgPack
}

var idCounter atomic.Int64

func getInsertMsgUniqueID(ts UniqueID) TsMsg {
	hashValue := uint32(ts)
	time := uint64(ts)

	insertRequest := &msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     idCounter.Inc(),
			Timestamp: time,
			SourceID:  ts,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ShardName:      "0",
		Timestamps:     []Timestamp{time},
		RowIDs:         []int64{1},
		RowData:        []*commonpb.Blob{{}},
	}
	insertMsg := &InsertMsg{
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{hashValue},
		},
		InsertRequest: insertRequest,
	}
	return insertMsg
}

func getTimeTickMsgPack(reqID UniqueID) *MsgPack {
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTimeTickMsg(reqID))
	return &msgPack
}

func getPulsarInputStream(ctx context.Context, pulsarAddress string, producerChannels []string, opts ...RepackFunc) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	inputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(ctx, producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	return inputStream
}

func getPulsarOutputStream(ctx context.Context, pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(context.Background(), consumerChannels, consumerSubName, common.SubscriptionPositionEarliest)
	return outputStream
}

func getPulsarTtOutputStream(ctx context.Context, pulsarAddress string, consumerChannels []string, consumerSubName string) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(context.Background(), consumerChannels, consumerSubName, common.SubscriptionPositionEarliest)
	return outputStream
}

func getPulsarTtOutputStreamAndSeek(ctx context.Context, pulsarAddress string, positions []*MsgPosition) MsgStream {
	factory := ProtoUDFactory{}
	pulsarClient, _ := pulsarwrapper.NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	outputStream, _ := NewMqTtMsgStream(ctx, 100, 100, pulsarClient, factory.NewUnmarshalDispatcher())
	consumerName := []string{}
	for _, c := range positions {
		consumerName = append(consumerName, c.ChannelName)
	}
	outputStream.AsConsumer(context.Background(), consumerName, funcutil.RandomString(8), common.SubscriptionPositionUnknown)
	outputStream.Seek(context.Background(), positions, false)
	return outputStream
}

func receiveMsg(ctx context.Context, outputStream MsgStream, msgCount int) {
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
					log.Println("msg type: ", v.GetType(), ", msg value: ", v)
				}
				log.Println("================")
			}
			if receiveCount >= msgCount {
				return
			}
		}
	}
}

func printMsgPack(msgPack *MsgPack) {
	if msgPack == nil {
		log.Println("msg nil")
	} else {
		for _, v := range msgPack.Msgs {
			log.Println("msg type: ", v.Type(), ", msg value: ", v)
		}
	}
	log.Println("================")
}

func patchMessageID(mid *pulsar.MessageID, entryID int64) {
	// use direct unsafe conversion
	/* #nosec G103 */
	r := (*iface)(unsafe.Pointer(mid))
	id := (*messageID)(r.Data)
	id.entryID = entryID
}

// unsafe access pointer, same as pulsar.messageID
type messageID struct {
	ledgerID     int64
	entryID      int64
	batchID      int32
	partitionIdx int32
}

// interface struct mapping
type iface struct {
	Type, Data unsafe.Pointer
}
