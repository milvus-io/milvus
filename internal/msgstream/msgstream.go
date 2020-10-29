package msgstream

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"log"
	"sync"
)

const PulsarChannelLength = 100

type TimeStamp uint64

type MsgPack struct {
	BeginTs TimeStamp
	EndTs   TimeStamp
	Msgs    []*TsMsg
}

type HashFunc func(*MsgPack) map[uint32]*MsgPack

type MsgStream interface {
	SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
	Produce(*MsgPack) commonPb.Status
	Consume() *MsgPack // message can be consumed exactly once
}

type PulsarMsgStream struct {
	client      *pulsar.Client
	producers   []*pulsar.Producer
	consumers   []*pulsar.Consumer
	msgHashFunc HashFunc // return a map from produceChannel idx to *MsgPack

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

func (ms *PulsarMsgStream) SetConsumers(channels []string, subName string) {
	for i := 0; i < len(channels); i++ {
		receiveChannel := make(chan pulsar.ConsumerMessage, PulsarChannelLength)
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

func (ms *PulsarMsgStream) SetHashFunc(hashFunc HashFunc) {
	ms.msgHashFunc = func(pack *MsgPack) map[uint32]*MsgPack {
		hashResult := hashFunc(pack)
		bucketResult := make(map[uint32]*MsgPack)
		for k, v := range hashResult {
			channelIndex := k % uint32(len(ms.producers))
			_, ok := bucketResult[channelIndex]
			if ok == false {
				msgPack := MsgPack{}
				bucketResult[channelIndex] = &msgPack
			}
			for _, msg := range v.Msgs {
				bucketResult[channelIndex].Msgs = append(bucketResult[channelIndex].Msgs, msg)
			}
		}
		return bucketResult
	}
}

func (ms *PulsarMsgStream) Produce(msg *MsgPack) commonPb.Status {
	result := ms.msgHashFunc(msg)
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
func (ms *PulsarMsgStream) Consume() *MsgPack {
	tsMsgList := make([]*TsMsg, 0)
	for i := 0; i < len(ms.consumers); i++ {
		pulsarMsg, ok := <-(*ms.consumers[i]).Chan()
		if ok == false {
			log.Fatal("consumer closed!")
			continue
		}
		(*ms.consumers[i]).AckID(pulsarMsg.ID())
		tsMsg, status := (*ms.msgUnmarshaler).Unmarshal(pulsarMsg.Payload())
		if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
			log.Printf("Marshal ManipulationReqMsg failed, error ")
		}
		tsMsgList = append(tsMsgList, tsMsg)

	}

	msgPack := MsgPack{Msgs: tsMsgList}
	return &msgPack
}

type PulsarTtMsgStream struct {
	PulsarMsgStream
	inputBuf      []*TsMsg
	unsolvedBuf   []*TsMsg
	msgPacks      []*MsgPack
	lastTimeStamp TimeStamp
}

func (ms *PulsarTtMsgStream) Consume() *MsgPack { //return messages in one time tick
	wg := sync.WaitGroup{}
	wg.Add(len(ms.consumers))
	eofMsgTimeStamp := make(map[int]TimeStamp)
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
	for _, v := range ms.unsolvedBuf {
		ms.inputBuf = append(ms.inputBuf, v)
	}
	ms.unsolvedBuf = ms.unsolvedBuf[:0]
	for _, v := range ms.inputBuf {
		if (*v).Ts() >= timeStamp {
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

	return &msgPack
}

func (ms *PulsarTtMsgStream) findTimeTick(ctx context.Context,
	channelIndex int,
	eofMsgMap map[int]TimeStamp,
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
			if (*tsMsg).Type() == msgclient.kTimeTick {
				eofMsgMap[channelIndex] = (*tsMsg).Ts()
				break
			}
			if status.ErrorCode != commonPb.ErrorCode_SUCCESS {
				log.Printf("Marshal ManipulationReqMsg failed, error ")
			}
			mu.Lock()
			ms.inputBuf = append(ms.inputBuf, tsMsg)
			mu.Unlock()
		}
	}
	wg.Done()
}

func checkTimeTickMsg(msg map[int]TimeStamp) (TimeStamp, bool) {
	checkMap := make(map[TimeStamp]int)
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
