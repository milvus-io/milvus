package msgstream

import (
	"context"
	"log"
	"sync"

	"github.com/golang/protobuf/proto"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/apache/pulsar-client-go/pulsar"
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

	Produce(*MsgPack) error
	Broadcast(*MsgPack) error
	Consume() *MsgPack
	Chan() <-chan *MsgPack
}

type PulsarMsgStream struct {
	ctx            context.Context
	client         *pulsar.Client
	producers      []*pulsar.Producer
	consumers      []*pulsar.Consumer
	repackFunc     RepackFunc
	unmarshal      *UnmarshalDispatcher
	receiveBuf     chan *MsgPack
	wait           *sync.WaitGroup
	streamCancel   func()
}

func NewPulsarMsgStream(ctx context.Context, receiveBufSize int64) *PulsarMsgStream {
	streamCtx, streamCancel := context.WithCancel(ctx)
	stream := &PulsarMsgStream{
		ctx:            streamCtx,
		streamCancel:   streamCancel,
	}
	stream.receiveBuf = make(chan *MsgPack, receiveBufSize)
	return stream
}

func (ms *PulsarMsgStream) SetPulsarCient(address string) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: address})
	if err != nil {
		log.Printf("Set pulsar client failed, error = %v", err)
	}
	ms.client = &client
}

func (ms *PulsarMsgStream) CreatePulsarProducers(channels []string) {
	for i := 0; i < len(channels); i++ {
		pp, err := (*ms.client).CreateProducer(pulsar.ProducerOptions{Topic: channels[i]})
		if err != nil {
			log.Printf("Failed to create reader producer %s, error = %v", channels[i], err)
		}
		ms.producers = append(ms.producers, &pp)
	}
}

func (ms *PulsarMsgStream) CreatePulsarConsumers(channels []string,
	subName string,
	unmarshal *UnmarshalDispatcher,
	pulsarBufSize int64) {
	ms.unmarshal = unmarshal
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
			log.Printf("Failed to subscribe topic, error = %v", err)
		}
		ms.consumers = append(ms.consumers, &pc)
	}
}

func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *PulsarMsgStream) Start() {
	ms.wait = &sync.WaitGroup{}
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *PulsarMsgStream) Close() {
	ms.streamCancel()
	if ms.wait != nil {
		ms.wait.Wait()
	}

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

func (ms *PulsarMsgStream) Produce(msgPack *MsgPack) error {
	tsMsgs := msgPack.Msgs
	if len(tsMsgs) <= 0 {
		log.Printf("Warning: Receive empty msgPack")
		return nil
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	for channelID, tsMsg := range tsMsgs {
		hashValues := (*tsMsg).HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			bucketValues[index] = hashValue % int32(len(ms.producers))
		}
		reBucketValues[channelID] = bucketValues
	}

	var result map[int32]*MsgPack
	if ms.repackFunc != nil {
		result = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		result = make(map[int32]*MsgPack)
		for i, request := range tsMsgs {
			keys := reBucketValues[i]
			for _, channelID := range keys {
				_, ok := result[channelID]
				if !ok {
					msgPack := MsgPack{}
					result[channelID] = &msgPack
				}
				result[channelID].Msgs = append(result[channelID].Msgs, request)
			}
		}
	}
	for k, v := range result {
		for i := 0; i < len(v.Msgs); i++ {
			mb, err := (*v.Msgs[i]).Marshal(v.Msgs[i])
			if err != nil {
				return err
			}
			if _, err := (*ms.producers[k]).Send(
				context.Background(),
				&pulsar.ProducerMessage{Payload: mb},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Broadcast(msgPack *MsgPack) error {
	producerLen := len(ms.producers)
	for _, v := range msgPack.Msgs {
		mb, err := (*v).Marshal(v)
		if err != nil {
			return err
		}
		for i := 0; i < producerLen; i++ {
			if _, err := (*ms.producers[i]).Send(
				context.Background(),
				&pulsar.ProducerMessage{Payload: mb},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Consume() *MsgPack {
	for {
		select {
		case cm, ok := <-ms.receiveBuf:
			if !ok {
				log.Println("buf chan closed")
				return nil
			}
			return cm
		case <-ms.ctx.Done():
			log.Printf("context closed")
			return nil
		}
	}
}

func (ms *PulsarMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			tsMsgList := make([]*TsMsg, 0)
			for i := 0; i < len(ms.consumers); i++ {
				consumerChan := (*ms.consumers[i]).Chan()
				chanLen := len(consumerChan)
				for l := 0; l < chanLen; l++ {
					pulsarMsg, ok := <-consumerChan
					if !ok {
						log.Printf("channel closed")
						return
					}
					(*ms.consumers[i]).AckID(pulsarMsg.ID())

					headerMsg := internalPb.MsgHeader{}
					err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
					if err != nil {
						log.Printf("Failed to unmarshal message header, error = %v", err)
						continue
					}
					tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.MsgType)
					if err != nil {
						log.Printf("Failed to unmarshal tsMsg, error = %v", err)
						continue
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
}

func (ms *PulsarMsgStream) Chan() <-chan *MsgPack {
	return ms.receiveBuf
}

type PulsarTtMsgStream struct {
	PulsarMsgStream
	inputBuf      []*TsMsg
	unsolvedBuf   []*TsMsg
	lastTimeStamp Timestamp
}

func NewPulsarTtMsgStream(ctx context.Context, receiveBufSize int64) *PulsarTtMsgStream {
	streamCtx, streamCancel := context.WithCancel(ctx)
	pulsarMsgStream := PulsarMsgStream{
		ctx:            streamCtx,
		streamCancel:   streamCancel,
	}
	pulsarMsgStream.receiveBuf = make(chan *MsgPack, receiveBufSize)
	return &PulsarTtMsgStream{
		PulsarMsgStream: pulsarMsgStream,
	}
}

func (ms *PulsarTtMsgStream) Start() {
	ms.wait = &sync.WaitGroup{}
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *PulsarTtMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	ms.unsolvedBuf = make([]*TsMsg, 0)
	ms.inputBuf = make([]*TsMsg, 0)
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			wg := sync.WaitGroup{}
			wg.Add(len(ms.consumers))
			eofMsgTimeStamp := make(map[int]Timestamp)
			mu := sync.Mutex{}
			for i := 0; i < len(ms.consumers); i++ {
				go ms.findTimeTick(i, eofMsgTimeStamp, &wg, &mu)
			}
			wg.Wait()
			timeStamp, ok := checkTimeTickMsg(eofMsgTimeStamp)
			if !ok {
				log.Printf("All timeTick's timestamps are inconsistent")
			}

			timeTickBuf := make([]*TsMsg, 0)
			ms.inputBuf = append(ms.inputBuf, ms.unsolvedBuf...)
			ms.unsolvedBuf = ms.unsolvedBuf[:0]
			for _, v := range ms.inputBuf {
				if (*v).EndTs() <= timeStamp {
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
			ms.lastTimeStamp = timeStamp
		}
	}

}

func (ms *PulsarTtMsgStream) findTimeTick(channelIndex int,
	eofMsgMap map[int]Timestamp,
	wg *sync.WaitGroup,
	mu *sync.Mutex) {
	defer wg.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		case pulsarMsg, ok := <-(*ms.consumers[channelIndex]).Chan():
			if !ok {
				log.Printf("consumer closed!")
				return
			}
			(*ms.consumers[channelIndex]).Ack(pulsarMsg)

			headerMsg := internalPb.MsgHeader{}
			err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
			if err != nil {
				log.Printf("Failed to unmarshal, error = %v", err)
			}
			unMarshalFunc := (*ms.unmarshal).tempMap[headerMsg.MsgType]
			tsMsg, err := unMarshalFunc(pulsarMsg.Payload())
			if err != nil {
				log.Printf("Failed to unmarshal, error = %v", err)
			}
			if headerMsg.MsgType == internalPb.MsgType_kTimeTick {
				eofMsgMap[channelIndex] = (*tsMsg).(*TimeTickMsg).Timestamp
				return
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
		checkMap[v]++
	}
	if len(checkMap) <= 1 {
		for k := range checkMap {
			return k, true
		}
	}
	return 0, false
}
