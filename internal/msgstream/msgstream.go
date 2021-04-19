package msgstream

import (
	"context"
	"log"
	"sync"

	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/apache/pulsar-client-go/pulsar"
	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp
type IntPrimaryKey = typeutil.IntPrimaryKey

type MsgPack struct {
	BeginTs Timestamp
	EndTs   Timestamp
	Msgs    []*TsMsg
}

type RepackFunc func(msgs []*TsMsg, hashKeys [][]int32) map[int32]*MsgPack

type MsgStream interface {
	Start()
	Close()

	SetRepackFunc(repackFunc RepackFunc)
	SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
	Produce(*MsgPack) commonPb.Status
	Consume() *MsgPack // message can be consumed exactly once
}

type PulsarMsgStream struct {
	client     *pulsar.Client
	producers  []*pulsar.Producer
	consumers  []*pulsar.Consumer
	repackFunc RepackFunc // return a map from produceChannel idx to *MsgPack

	receiveBuf chan *MsgPack

	msgMarshaler   *TsMsgMarshaler
	msgUnmarshaler *TsMsgMarshaler
	inputChannel   chan *MsgPack
	outputChannel  chan *MsgPack
}

func (ms *PulsarMsgStream) SetPulsarCient(address string) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: address})
	if err != nil {
		log.Printf("connect pulsar failed, %v", err)
	}
	ms.client = &client
}

func (ms *PulsarMsgStream) SetProducers(channels []string) {
	for i := 0; i < len(channels); i++ {
		pp, err := (*ms.client).CreateProducer(pulsar.ProducerOptions{Topic: channels[i]})
		if err != nil {
			log.Printf("failed to create reader producer %s, error = %v", channels[i], err)
		}
		ms.producers = append(ms.producers, &pp)
	}
}

func (ms *PulsarMsgStream) SetConsumers(channels []string, subName string, pulsarBufSize int64) {
	for i := 0; i < len(channels); i++ {
		receiveChannel := make(chan pulsar.ConsumerMessage, pulsarBufSize)
		pc, err := (*ms.client).Subscribe(pulsar.ConsumerOptions{
			Topic:                       channels[i],
			SubscriptionName:            subName,
			Type:                        pulsar.KeyShared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			MessageChannel:              receiveChannel,
		})
		if err != nil {
			log.Printf("failed to subscribe topic, error = %v", err)
		}
		ms.consumers = append(ms.consumers, &pc)
	}
}

func (ms *PulsarMsgStream) SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler) {
	ms.msgMarshaler = marshal
	ms.msgUnmarshaler = unmarshal
}

func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *PulsarMsgStream) Start() {
	go ms.bufMsgPackToChannel()
}

func (ms *PulsarMsgStream) Close() {
	for _, producer := range ms.producers {
		if producer != nil {
			(*producer).Close()
		}
	}
	for _, consumer := range ms.consumers {
		if consumer != nil {
			(*consumer).Close()
		}
	}
	if ms.client != nil {
		(*ms.client).Close()
	}
}

func (ms *PulsarMsgStream) InitMsgPackBuf(msgPackBufSize int64) {
	ms.receiveBuf = make(chan *MsgPack, msgPackBufSize)
}

func (ms *PulsarMsgStream) Produce(msgPack *MsgPack) commonPb.Status {
	tsMsgs := msgPack.Msgs
	if len(tsMsgs) <= 0 {
		log.Println("receive empty msgPack")
		return commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	for channelId, tsMsg := range tsMsgs {
		hashValues := (*tsMsg).HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			bucketValues[index] = hashValue % int32(len(ms.producers))
		}
		reBucketValues[channelId] = bucketValues
	}

	var result map[int32]*MsgPack
	if ms.repackFunc != nil {
		result = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		result = make(map[int32]*MsgPack)
		for i, request := range tsMsgs {
			keys := reBucketValues[i]
			for _, channelId := range keys {
				_, ok := result[channelId]
				if ok == false {
					msgPack := MsgPack{}
					result[channelId] = &msgPack
				}
				result[channelId].Msgs = append(result[channelId].Msgs, request)
			}
		}
	}
	for k, v := range result {
		for i := 0; i < len(v.Msgs); i++ {
			mb, status := (*ms.msgMarshaler).Marshal(v.Msgs[i])
			if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
				log.Printf("Marshal ManipulationReqMsg failed, error ")
				continue
			}
			if _, err := (*ms.producers[k]).Send(
				context.Background(),
				&pulsar.ProducerMessage{Payload: mb},
			); err != nil {
				log.Printf("post into pulsar filed, error = %v", err)
			}
		}
	}

	return commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (ms *PulsarMsgStream) BroadCast(msgPack *MsgPack) commonPb.Status {
	producerLen := len(ms.producers)
	for _, v := range msgPack.Msgs {
		mb, status := (*ms.msgMarshaler).Marshal(v)
		if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
			log.Printf("Marshal ManipulationReqMsg failed, error ")
			continue
		}
		for i := 0; i < producerLen; i++ {
			if _, err := (*ms.producers[i]).Send(
				context.Background(),
				&pulsar.ProducerMessage{Payload: mb},
			); err != nil {
				log.Printf("post into pulsar filed, error = %v", err)
			}
		}
	}

	return commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (ms *PulsarMsgStream) Consume() *MsgPack {
	ctx := context.Background()
	for {
		select {
		case cm, ok := <-ms.receiveBuf:
			if !ok {
				log.Println("buf chan closed")
				return nil
			}
			return cm
		case <-ctx.Done():
			return nil
		}
	}
}

func (ms *PulsarMsgStream) bufMsgPackToChannel() {
	for {
		tsMsgList := make([]*TsMsg, 0)
		for i := 0; i < len(ms.consumers); i++ {
			consumerChan := (*ms.consumers[i]).Chan()
			chanLen := len(consumerChan)
			for l := 0; l < chanLen; l++ {
				pulsarMsg, ok := <-consumerChan
				if ok == false {
					log.Printf("channel closed")
				}
				(*ms.consumers[i]).AckID(pulsarMsg.ID())
				tsMsg, status := (*ms.msgUnmarshaler).Unmarshal(pulsarMsg.Payload())
				if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
					log.Printf("Marshal ManipulationReqMsg failed, error ")
				}
				tsMsgList = append(tsMsgList, tsMsg)
			}
		}
		if len(tsMsgList) > 0 {
			msgPack := MsgPack{Msgs: tsMsgList}
			ms.receiveBuf <- &msgPack
		}
	}
}

type PulsarTtMsgStream struct {
	PulsarMsgStream
	inputBuf      []*TsMsg
	unsolvedBuf   []*TsMsg
	msgPacks      []*MsgPack
	lastTimeStamp Timestamp
}

func (ms *PulsarTtMsgStream) Start() {
	go ms.bufMsgPackToChannel()
}

func (ms *PulsarTtMsgStream) bufMsgPackToChannel() {
	wg := sync.WaitGroup{}
	wg.Add(len(ms.consumers))
	eofMsgTimeStamp := make(map[int]Timestamp)
	mu := sync.Mutex{}
	for i := 0; i < len(ms.consumers); i++ {
		go ms.findTimeTick(context.Background(), i, eofMsgTimeStamp, &wg, &mu)
	}
	wg.Wait()
	timeStamp, ok := checkTimeTickMsg(eofMsgTimeStamp)
	if ok == false {
		log.Fatal("timeTick err")
	}

	timeTickBuf := make([]*TsMsg, 0)
	ms.inputBuf = append(ms.inputBuf, ms.unsolvedBuf...)
	ms.unsolvedBuf = ms.unsolvedBuf[:0]
	for _, v := range ms.inputBuf {
		if (*v).EndTs() >= timeStamp {
			timeTickBuf = append(timeTickBuf, v)
		} else {
			ms.unsolvedBuf = append(ms.unsolvedBuf, v)
		}
	}
	ms.inputBuf = ms.inputBuf[:0]

	msgPack := MsgPack{
		BeginTs: ms.lastTimeStamp,
		EndTs:   timeStamp,
		Msgs:    timeTickBuf,
	}

	ms.receiveBuf <- &msgPack
}

func (ms *PulsarTtMsgStream) findTimeTick(ctx context.Context,
	channelIndex int,
	eofMsgMap map[int]Timestamp,
	wg *sync.WaitGroup,
	mu *sync.Mutex) {
	for {
		select {
		case <-ctx.Done():
			return
		case pulsarMsg, ok := <-(*ms.consumers[channelIndex]).Chan():
			if ok == false {
				log.Fatal("consumer closed!")
				continue
			}
			(*ms.consumers[channelIndex]).Ack(pulsarMsg)
			tsMsg, status := (*ms.msgUnmarshaler).Unmarshal(pulsarMsg.Payload())
			// TODO:: Find the EOF
			if (*tsMsg).Type() == internalPb.MsgType_kTimeTick {
				eofMsgMap[channelIndex] = (*tsMsg).EndTs()
				wg.Done()
				return
			}
			if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
				log.Printf("Marshal ManipulationReqMsg failed, error ")
			}
			mu.Lock()
			ms.inputBuf = append(ms.inputBuf, tsMsg)
			mu.Unlock()
		}
	}
}

func checkTimeTickMsg(msg map[int]Timestamp) (Timestamp, bool) {
	checkMap := make(map[Timestamp]int)
	for _, v := range msg {
		checkMap[v] += 1
	}
	if len(checkMap) <= 1 {
		for k, _ := range checkMap {
			return k, true
		}
	}
	return 0, false
}
