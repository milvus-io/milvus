package pulsarms

import (
	"context"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type TsMsg = msgstream.TsMsg
type MsgPack = msgstream.MsgPack
type MsgType = msgstream.MsgType
type UniqueID = msgstream.UniqueID
type BaseMsg = msgstream.BaseMsg
type Timestamp = msgstream.Timestamp
type IntPrimaryKey = msgstream.IntPrimaryKey
type TimeTickMsg = msgstream.TimeTickMsg
type QueryNodeStatsMsg = msgstream.QueryNodeStatsMsg
type RepackFunc = msgstream.RepackFunc
type Consumer = pulsar.Consumer
type Producer = pulsar.Producer
type MessageID = pulsar.MessageID
type UnmarshalDispatcher = msgstream.UnmarshalDispatcher

type PulsarMsgStream struct {
	ctx              context.Context
	client           pulsar.Client
	producers        []Producer
	consumers        []Consumer
	consumerChannels []string
	repackFunc       RepackFunc
	unmarshal        UnmarshalDispatcher
	receiveBuf       chan *MsgPack
	wait             *sync.WaitGroup
	streamCancel     func()
	pulsarBufSize    int64
	consumerLock     *sync.Mutex
	consumerReflects []reflect.SelectCase
}

func newPulsarMsgStream(ctx context.Context,
	address string,
	receiveBufSize int64,
	pulsarBufSize int64,
	unmarshal UnmarshalDispatcher) (*PulsarMsgStream, error) {

	streamCtx, streamCancel := context.WithCancel(ctx)
	producers := make([]Producer, 0)
	consumers := make([]Consumer, 0)
	consumerChannels := make([]string, 0)
	consumerReflects := make([]reflect.SelectCase, 0)
	receiveBuf := make(chan *MsgPack, receiveBufSize)

	var client pulsar.Client
	var err error
	opts := pulsar.ClientOptions{
		URL: address,
	}
	client, err = pulsar.NewClient(opts)
	if err != nil {
		defer streamCancel()
		log.Error("Set pulsar client failed, error", zap.Error(err))
		return nil, err
	}

	stream := &PulsarMsgStream{
		ctx:              streamCtx,
		client:           client,
		producers:        producers,
		consumers:        consumers,
		consumerChannels: consumerChannels,
		unmarshal:        unmarshal,
		pulsarBufSize:    pulsarBufSize,
		receiveBuf:       receiveBuf,
		streamCancel:     streamCancel,
		consumerReflects: consumerReflects,
		consumerLock:     &sync.Mutex{},
		wait:             &sync.WaitGroup{},
	}

	return stream, nil
}

func (ms *PulsarMsgStream) AsProducer(channels []string) {
	for i := 0; i < len(channels); i++ {
		fn := func() error {
			pp, err := ms.client.CreateProducer(pulsar.ProducerOptions{Topic: channels[i]})
			if err != nil {
				return err
			}
			if pp == nil {
				return errors.New("pulsar is not ready, producer is nil")
			}

			ms.producers = append(ms.producers, pp)
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create producer " + channels[i] + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *PulsarMsgStream) AsConsumer(channels []string,
	subName string) {
	for i := 0; i < len(channels); i++ {
		fn := func() error {
			receiveChannel := make(chan pulsar.ConsumerMessage, ms.pulsarBufSize)
			pc, err := ms.client.Subscribe(pulsar.ConsumerOptions{
				Topic:                       channels[i],
				SubscriptionName:            subName,
				Type:                        pulsar.KeyShared,
				SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("pulsar is not ready, consumer is nil")
			}

			ms.consumers = append(ms.consumers, pc)
			ms.consumerChannels = append(ms.consumerChannels, channels[i])
			ms.consumerReflects = append(ms.consumerReflects, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(pc.Chan()),
			})
			ms.wait.Add(1)
			go ms.receiveMsg(pc)
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channels[i] + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *PulsarMsgStream) Start() {
}

func (ms *PulsarMsgStream) Close() {
	ms.streamCancel()
	ms.wait.Wait()

	for _, producer := range ms.producers {
		if producer != nil {
			producer.Close()
		}
	}
	for _, consumer := range ms.consumers {
		if consumer != nil {
			consumer.Close()
		}
	}
	if ms.client != nil {
		ms.client.Close()
	}
}

type propertiesReaderWriter struct {
	ppMap map[string]string
}

func (ppRW *propertiesReaderWriter) Set(key, val string) {
	// The GRPC HPACK implementation rejects any uppercase keys here.
	//
	// As such, since the HTTP_HEADERS format is case-insensitive anyway, we
	// blindly lowercase the key (which is guaranteed to work in the
	// Inject/Extract sense per the OpenTracing spec).
	key = strings.ToLower(key)
	ppRW.ppMap[key] = val
}

func (ppRW *propertiesReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, val := range ppRW.ppMap {
		if err := handler(k, val); err != nil {
			return err
		}
	}
	return nil
}

func (ms *PulsarMsgStream) Produce(msgPack *MsgPack) error {
	tsMsgs := msgPack.Msgs
	if len(tsMsgs) <= 0 {
		log.Debug("Warning: Receive empty msgPack")
		return nil
	}
	if len(ms.producers) <= 0 {
		return errors.New("nil producer in msg stream")
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	for idx, tsMsg := range tsMsgs {
		hashValues := tsMsg.HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			if tsMsg.Type() == commonpb.MsgType_kSearchResult {
				searchResult := tsMsg.(*msgstream.SearchResultMsg)
				channelID := searchResult.ResultChannelID
				channelIDInt, _ := strconv.ParseInt(channelID, 10, 64)
				if channelIDInt >= int64(len(ms.producers)) {
					return errors.New("Failed to produce pulsar msg to unKnow channel")
				}
				bucketValues[index] = int32(hashValue % uint32(len(ms.producers)))
				continue
			}
			bucketValues[index] = int32(hashValue % uint32(len(ms.producers)))
		}
		reBucketValues[idx] = bucketValues
	}

	var result map[int32]*MsgPack
	var err error
	if ms.repackFunc != nil {
		result, err = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		msgType := (tsMsgs[0]).Type()
		switch msgType {
		case commonpb.MsgType_kInsert:
			result, err = util.InsertRepackFunc(tsMsgs, reBucketValues)
		case commonpb.MsgType_kDelete:
			result, err = util.DeleteRepackFunc(tsMsgs, reBucketValues)
		default:
			result, err = util.DefaultRepackFunc(tsMsgs, reBucketValues)
		}
	}
	if err != nil {
		return err
	}
	for k, v := range result {
		for i := 0; i < len(v.Msgs); i++ {
			mb, err := v.Msgs[i].Marshal(v.Msgs[i])
			if err != nil {
				return err
			}

			m, err := msgstream.ConvertToByteArray(mb)
			if err != nil {
				return err
			}

			msg := &pulsar.ProducerMessage{Payload: m}

			if _, err := ms.producers[k].Send(
				context.Background(),
				msg,
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
		mb, err := v.Marshal(v)
		if err != nil {
			return err
		}

		m, err := msgstream.ConvertToByteArray(mb)
		if err != nil {
			return err
		}

		msg := &pulsar.ProducerMessage{Payload: m}
		for i := 0; i < producerLen; i++ {
			if _, err := ms.producers[i].Send(
				context.Background(),
				msg,
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
				log.Debug("buf chan closed")
				return nil
			}
			return cm
		case <-ms.ctx.Done():
			log.Debug("context closed")
			return nil
		}
	}
}

func (ms *PulsarMsgStream) receiveMsg(consumer Consumer) {
	defer ms.wait.Done()

	for {
		select {
		case <-ms.ctx.Done():
			return
		case pulsarMsg, ok := <-consumer.Chan():
			if !ok {
				return
			}
			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
			if err != nil {
				log.Error("Failed to unmarshal message header", zap.Error(err))
				continue
			}
			tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
			if err != nil {
				log.Error("Failed to unmarshal tsMsg", zap.Error(err))
				continue
			}

			tsMsg.SetPosition(&msgstream.MsgPosition{
				ChannelName: filepath.Base(pulsarMsg.Topic()),
				MsgID:       typeutil.PulsarMsgIDToString(pulsarMsg.ID()),
			})

			msgPack := MsgPack{Msgs: []TsMsg{tsMsg}}
			ms.receiveBuf <- &msgPack
		}
	}
}

func (ms *PulsarMsgStream) bufMsgPackToChannel() {

	for {
		select {
		case <-ms.ctx.Done():
			log.Debug("done")
			return
		default:
			tsMsgList := make([]TsMsg, 0)

			//for {
			//	ms.consumerLock.Lock()
			//	chosen, value, ok := reflect.Select(ms.consumerReflects)
			//	ms.consumerLock.Unlock()
			//	if !ok {
			//		log.Printf("channel closed")
			//		return
			//	}
			//
			//	pulsarMsg, ok := value.Interface().(pulsar.ConsumerMessage)
			//
			//	if !ok {
			//		log.Printf("type assertion failed, not consumer message type")
			//		continue
			//	}
			//	ms.consumers[chosen].AckID(pulsarMsg.ID())
			//
			//	headerMsg := commonpb.MsgHeader{}
			//	err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
			//	if err != nil {
			//		log.Printf("Failed to unmarshal message header, error = %v", err)
			//		continue
			//	}
			//	tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
			//	if err != nil {
			//		log.Printf("Failed to unmarshal tsMsg, error = %v", err)
			//		continue
			//	}
			//
			//	tsMsg.SetPosition(&msgstream.MsgPosition{
			//		ChannelName: filepath.Base(pulsarMsg.Topic()),
			//		MsgID:       typeutil.PulsarMsgIDToString(pulsarMsg.ID()),
			//	})
			//	tsMsgList = append(tsMsgList, tsMsg)
			//
			//	noMoreMessage := true
			//	for i := 0; i < len(ms.consumers); i++ {
			//		if len(ms.consumers[i].Chan()) > 0 {
			//			noMoreMessage = false
			//		}
			//	}
			//
			//	if noMoreMessage {
			//		break
			//	}
			//}

			pulsarMsgBuffer := make([]pulsar.ConsumerMessage, 0)
			ms.consumerLock.Lock()
			consumers := ms.consumers
			ms.consumerLock.Unlock()
			for _, consumer := range consumers {
				msgLen := len(consumer.Chan())
				for i := 0; i < msgLen; i++ {
					msg := <-consumer.Chan()
					pulsarMsgBuffer = append(pulsarMsgBuffer, msg)
				}
			}
			for _, pulsarMsg := range pulsarMsgBuffer {
				headerMsg := commonpb.MsgHeader{}
				err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
				if err != nil {
					log.Error("Failed to unmarshal message header", zap.Error(err))
					continue
				}
				tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
				if err != nil {
					log.Error("Failed to unmarshal tsMsg", zap.Error(err))
					continue
				}

				tsMsg.SetPosition(&msgstream.MsgPosition{
					ChannelName: filepath.Base(pulsarMsg.Topic()),
					MsgID:       typeutil.PulsarMsgIDToString(pulsarMsg.ID()),
				})
				tsMsgList = append(tsMsgList, tsMsg)
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

func (ms *PulsarMsgStream) Seek(mp *internalpb2.MsgPosition) error {
	for index, channel := range ms.consumerChannels {
		if channel == mp.ChannelName {
			messageID, err := typeutil.StringToPulsarMsgID(mp.MsgID)
			if err != nil {
				return err
			}
			err = ms.consumers[index].Seek(messageID)
			if err != nil {
				return err
			}
			return nil
		}
	}

	return errors.New("msgStream seek fail")
}

type PulsarTtMsgStream struct {
	PulsarMsgStream
	unsolvedBuf   map[Consumer][]TsMsg
	unsolvedMutex *sync.Mutex
	lastTimeStamp Timestamp
	syncConsumer  chan int
}

func newPulsarTtMsgStream(ctx context.Context,
	address string,
	receiveBufSize int64,
	pulsarBufSize int64,
	unmarshal msgstream.UnmarshalDispatcher) (*PulsarTtMsgStream, error) {
	pulsarMsgStream, err := newPulsarMsgStream(ctx, address, receiveBufSize, pulsarBufSize, unmarshal)
	if err != nil {
		return nil, err
	}
	unsolvedBuf := make(map[Consumer][]TsMsg)
	syncConsumer := make(chan int, 1)

	return &PulsarTtMsgStream{
		PulsarMsgStream: *pulsarMsgStream,
		unsolvedBuf:     unsolvedBuf,
		unsolvedMutex:   &sync.Mutex{},
		syncConsumer:    syncConsumer,
	}, nil
}

func (ms *PulsarTtMsgStream) AsConsumer(channels []string,
	subName string) {
	for i := 0; i < len(channels); i++ {
		fn := func() error {
			receiveChannel := make(chan pulsar.ConsumerMessage, ms.pulsarBufSize)
			pc, err := ms.client.Subscribe(pulsar.ConsumerOptions{
				Topic:                       channels[i],
				SubscriptionName:            subName,
				Type:                        pulsar.KeyShared,
				SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("pulsar is not ready, consumer is nil")
			}

			ms.consumerLock.Lock()
			if len(ms.consumers) == 0 {
				ms.syncConsumer <- 1
			}
			ms.consumers = append(ms.consumers, pc)
			ms.unsolvedBuf[pc] = make([]TsMsg, 0)
			ms.consumerChannels = append(ms.consumerChannels, channels[i])
			ms.consumerLock.Unlock()
			return nil
		}
		err := util.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channels[i] + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *PulsarTtMsgStream) Start() {
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *PulsarTtMsgStream) Close() {
	ms.streamCancel()
	close(ms.syncConsumer)
	ms.wait.Wait()

	for _, producer := range ms.producers {
		if producer != nil {
			producer.Close()
		}
	}
	for _, consumer := range ms.consumers {
		if consumer != nil {
			consumer.Close()
		}
	}
	if ms.client != nil {
		ms.client.Close()
	}
}

func (ms *PulsarTtMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	ms.unsolvedBuf = make(map[Consumer][]TsMsg)
	isChannelReady := make(map[Consumer]bool)
	eofMsgTimeStamp := make(map[Consumer]Timestamp)

	if _, ok := <-ms.syncConsumer; !ok {
		log.Debug("consumer closed!")
		return
	}

	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			wg := sync.WaitGroup{}
			findMapMutex := sync.RWMutex{}
			ms.consumerLock.Lock()
			for _, consumer := range ms.consumers {
				if isChannelReady[consumer] {
					continue
				}
				wg.Add(1)
				go ms.findTimeTick(consumer, eofMsgTimeStamp, &wg, &findMapMutex)
			}
			ms.consumerLock.Unlock()
			wg.Wait()
			timeStamp, ok := checkTimeTickMsg(eofMsgTimeStamp, isChannelReady, &findMapMutex)
			if !ok || timeStamp <= ms.lastTimeStamp {
				//log.Printf("All timeTick's timestamps are inconsistent")
				continue
			}
			timeTickBuf := make([]TsMsg, 0)
			msgPositions := make([]*internalpb2.MsgPosition, 0)
			ms.unsolvedMutex.Lock()
			for consumer, msgs := range ms.unsolvedBuf {
				if len(msgs) == 0 {
					continue
				}
				tempBuffer := make([]TsMsg, 0)
				var timeTickMsg TsMsg
				for _, v := range msgs {
					if v.Type() == commonpb.MsgType_kTimeTick {
						timeTickMsg = v
						continue
					}
					if v.EndTs() <= timeStamp {
						timeTickBuf = append(timeTickBuf, v)
					} else {
						tempBuffer = append(tempBuffer, v)
					}
				}
				ms.unsolvedBuf[consumer] = tempBuffer

				if len(tempBuffer) > 0 {
					msgPositions = append(msgPositions, &internalpb2.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   timeStamp,
					})
				} else {
					msgPositions = append(msgPositions, &internalpb2.MsgPosition{
						ChannelName: timeTickMsg.Position().ChannelName,
						MsgID:       timeTickMsg.Position().MsgID,
						Timestamp:   timeStamp,
					})
				}
			}
			ms.unsolvedMutex.Unlock()

			msgPack := MsgPack{
				BeginTs:        ms.lastTimeStamp,
				EndTs:          timeStamp,
				Msgs:           timeTickBuf,
				StartPositions: msgPositions,
			}

			ms.receiveBuf <- &msgPack
			ms.lastTimeStamp = timeStamp
		}
	}
}

func (ms *PulsarTtMsgStream) findTimeTick(consumer Consumer,
	eofMsgMap map[Consumer]Timestamp,
	wg *sync.WaitGroup,
	findMapMutex *sync.RWMutex) {
	defer wg.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		case pulsarMsg, ok := <-consumer.Chan():
			if !ok {
				log.Debug("consumer closed!")
				return
			}
			consumer.Ack(pulsarMsg)

			headerMsg := commonpb.MsgHeader{}
			err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
			if err != nil {
				log.Error("Failed to unmarshal message header", zap.Error(err))
				continue
			}
			tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
			if err != nil {
				log.Error("Failed to unmarshal tsMsg", zap.Error(err))
				continue
			}
			// set pulsar info to tsMsg
			tsMsg.SetPosition(&msgstream.MsgPosition{
				ChannelName: filepath.Base(pulsarMsg.Topic()),
				MsgID:       typeutil.PulsarMsgIDToString(pulsarMsg.ID()),
			})

			ms.unsolvedMutex.Lock()
			ms.unsolvedBuf[consumer] = append(ms.unsolvedBuf[consumer], tsMsg)
			ms.unsolvedMutex.Unlock()

			if headerMsg.Base.MsgType == commonpb.MsgType_kTimeTick {
				findMapMutex.Lock()
				eofMsgMap[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
				findMapMutex.Unlock()
				return
			}
		}
	}
}

func (ms *PulsarTtMsgStream) Seek(mp *internalpb2.MsgPosition) error {
	var consumer Consumer
	var messageID MessageID
	for index, channel := range ms.consumerChannels {
		if filepath.Base(channel) == filepath.Base(mp.ChannelName) {
			seekMsgID, err := typeutil.StringToPulsarMsgID(mp.MsgID)
			if err != nil {
				return err
			}
			consumer = ms.consumers[index]
			messageID = seekMsgID
			break
		}
	}

	if consumer != nil {
		err := (consumer).Seek(messageID)
		if err != nil {
			return err
		}

		ms.unsolvedMutex.Lock()
		ms.unsolvedBuf[consumer] = make([]TsMsg, 0)
		for {
			select {
			case <-ms.ctx.Done():
				return nil
			case pulsarMsg, ok := <-consumer.Chan():
				if !ok {
					return errors.New("consumer closed")
				}
				consumer.Ack(pulsarMsg)

				headerMsg := commonpb.MsgHeader{}
				err := proto.Unmarshal(pulsarMsg.Payload(), &headerMsg)
				if err != nil {
					log.Error("Failed to unmarshal message header", zap.Error(err))
				}
				tsMsg, err := ms.unmarshal.Unmarshal(pulsarMsg.Payload(), headerMsg.Base.MsgType)
				if err != nil {
					log.Error("Failed to unmarshal tsMsg", zap.Error(err))
				}
				if tsMsg.Type() == commonpb.MsgType_kTimeTick {
					if tsMsg.BeginTs() >= mp.Timestamp {
						ms.unsolvedMutex.Unlock()
						return nil
					}
					continue
				}
				if tsMsg.BeginTs() > mp.Timestamp {
					tsMsg.SetPosition(&msgstream.MsgPosition{
						ChannelName: filepath.Base(pulsarMsg.Topic()),
						MsgID:       typeutil.PulsarMsgIDToString(pulsarMsg.ID()),
					})
					ms.unsolvedBuf[consumer] = append(ms.unsolvedBuf[consumer], tsMsg)
				}
			}
		}
	}

	return errors.New("msgStream seek fail")
}

func checkTimeTickMsg(msg map[Consumer]Timestamp,
	isChannelReady map[Consumer]bool,
	mu *sync.RWMutex) (Timestamp, bool) {
	checkMap := make(map[Timestamp]int)
	var maxTime Timestamp = 0
	for _, v := range msg {
		checkMap[v]++
		if v > maxTime {
			maxTime = v
		}
	}
	if len(checkMap) <= 1 {
		for consumer := range msg {
			isChannelReady[consumer] = false
		}
		return maxTime, true
	}
	for consumer := range msg {
		mu.RLock()
		v := msg[consumer]
		mu.RUnlock()
		if v != maxTime {
			isChannelReady[consumer] = false
		} else {
			isChannelReady[consumer] = true
		}
	}

	return 0, false
}

//TODO test InMemMsgStream
/*
type InMemMsgStream struct {
	buffer chan *MsgPack
}

func (ms *InMemMsgStream) Start() {}
func (ms *InMemMsgStream) Close() {}

func (ms *InMemMsgStream) ProduceOne(msg TsMsg) error {
	msgPack := MsgPack{}
	msgPack.BeginTs = msg.BeginTs()
	msgPack.EndTs = msg.EndTs()
	msgPack.Msgs = append(msgPack.Msgs, msg)
	buffer <- &msgPack
	return nil
}

func (ms *InMemMsgStream) Produce(msgPack *MsgPack) error {
	buffer <- msgPack
	return nil
}

func (ms *InMemMsgStream) Broadcast(msgPack *MsgPack) error {
	return ms.Produce(msgPack)
}

func (ms *InMemMsgStream) Consume() *MsgPack {
	select {
	case msgPack := <-ms.buffer:
		return msgPack
	}
}

func (ms *InMemMsgStream) Chan() <- chan *MsgPack {
	return buffer
}
*/
