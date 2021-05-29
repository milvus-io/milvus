// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package msgstream

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type mqMsgStream struct {
	ctx              context.Context
	client           mqclient.Client
	producers        map[string]mqclient.Producer
	producerChannels []string
	consumers        map[string]mqclient.Consumer
	consumerChannels []string
	repackFunc       RepackFunc
	unmarshal        UnmarshalDispatcher
	receiveBuf       chan *MsgPack
	wait             *sync.WaitGroup
	streamCancel     func()
	bufSize          int64
	producerLock     *sync.Mutex
	consumerLock     *sync.Mutex
}

func NewMqMsgStream(ctx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client mqclient.Client,
	unmarshal UnmarshalDispatcher) (*mqMsgStream, error) {

	streamCtx, streamCancel := context.WithCancel(ctx)
	producers := make(map[string]mqclient.Producer)
	consumers := make(map[string]mqclient.Consumer)
	producerChannels := make([]string, 0)
	consumerChannels := make([]string, 0)
	receiveBuf := make(chan *MsgPack, receiveBufSize)

	stream := &mqMsgStream{
		ctx:              streamCtx,
		client:           client,
		producers:        producers,
		producerChannels: producerChannels,
		consumers:        consumers,
		consumerChannels: consumerChannels,
		unmarshal:        unmarshal,
		bufSize:          bufSize,
		receiveBuf:       receiveBuf,
		streamCancel:     streamCancel,
		producerLock:     &sync.Mutex{},
		consumerLock:     &sync.Mutex{},
		wait:             &sync.WaitGroup{},
	}

	return stream, nil
}

func (ms *mqMsgStream) AsProducer(channels []string) {
	for _, channel := range channels {
		fn := func() error {
			pp, err := ms.client.CreateProducer(mqclient.ProducerOptions{Topic: channel})
			if err != nil {
				return err
			}
			if pp == nil {
				return errors.New("Producer is nil")
			}

			ms.producerLock.Lock()
			ms.producers[channel] = pp
			ms.producerChannels = append(ms.producerChannels, channel)
			ms.producerLock.Unlock()
			return nil
		}
		err := Retry(20, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *mqMsgStream) AsConsumer(channels []string, subName string) {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			receiveChannel := make(chan mqclient.ConsumerMessage, ms.bufSize)
			pc, err := ms.client.Subscribe(mqclient.ConsumerOptions{
				Topic:                       channel,
				SubscriptionName:            subName,
				Type:                        mqclient.KeyShared,
				SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("Consumer is nil")
			}

			ms.consumerLock.Lock()
			ms.consumers[channel] = pc
			ms.consumerChannels = append(ms.consumerChannels, channel)
			ms.consumerLock.Unlock()
			ms.wait.Add(1)
			go ms.receiveMsg(pc)
			return nil
		}
		err := Retry(20, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *mqMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *mqMsgStream) Start() {
}

func (ms *mqMsgStream) Close() {
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

func (ms *mqMsgStream) ComputeProduceChannelIndexes(tsMsgs []TsMsg) [][]int32 {
	if len(tsMsgs) <= 0 {
		return nil
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	channelNum := uint32(len(ms.producerChannels))

	if channelNum == 0 {
		return nil
	}
	for idx, tsMsg := range tsMsgs {
		hashValues := tsMsg.HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			bucketValues[index] = int32(hashValue % channelNum)
		}
		reBucketValues[idx] = bucketValues
	}
	return reBucketValues
}

func (ms *mqMsgStream) GetProduceChannels() []string {
	return ms.producerChannels
}

func (ms *mqMsgStream) Produce(msgPack *MsgPack) error {
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		log.Debug("Warning: Receive empty msgPack")
		return nil
	}
	if len(ms.producers) <= 0 {
		return errors.New("nil producer in msg stream")
	}
	tsMsgs := msgPack.Msgs
	reBucketValues := ms.ComputeProduceChannelIndexes(msgPack.Msgs)
	var result map[int32]*MsgPack
	var err error
	if ms.repackFunc != nil {
		result, err = ms.repackFunc(tsMsgs, reBucketValues)
	} else {
		msgType := (tsMsgs[0]).Type()
		switch msgType {
		case commonpb.MsgType_Insert:
			result, err = InsertRepackFunc(tsMsgs, reBucketValues)
		case commonpb.MsgType_Delete:
			result, err = DeleteRepackFunc(tsMsgs, reBucketValues)
		default:
			result, err = DefaultRepackFunc(tsMsgs, reBucketValues)
		}
	}
	if err != nil {
		return err
	}
	for k, v := range result {
		channel := ms.producerChannels[k]
		for i := 0; i < len(v.Msgs); i++ {
			sp, spanCtx := MsgSpanFromCtx(v.Msgs[i].TraceCtx(), v.Msgs[i])

			mb, err := v.Msgs[i].Marshal(v.Msgs[i])
			if err != nil {
				return err
			}

			m, err := ConvertToByteArray(mb)
			if err != nil {
				return err
			}

			msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

			trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

			if err := ms.producers[channel].Send(
				spanCtx,
				msg,
			); err != nil {
				trace.LogError(sp, err)
				sp.Finish()
				return err
			}
			sp.Finish()
		}
	}
	return nil
}

func (ms *mqMsgStream) Broadcast(msgPack *MsgPack) error {
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		log.Debug("Warning: Receive empty msgPack")
		return nil
	}
	for _, v := range msgPack.Msgs {
		sp, spanCtx := MsgSpanFromCtx(v.TraceCtx(), v)

		mb, err := v.Marshal(v)
		if err != nil {
			return err
		}

		m, err := ConvertToByteArray(mb)
		if err != nil {
			return err
		}

		msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

		trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

		ms.producerLock.Lock()
		for _, producer := range ms.producers {
			if err := producer.Send(
				spanCtx,
				msg,
			); err != nil {
				trace.LogError(sp, err)
				sp.Finish()
				return err
			}
		}
		ms.producerLock.Unlock()
		sp.Finish()
	}
	return nil
}

func (ms *mqMsgStream) Consume() *MsgPack {
	for {
		select {
		case <-ms.ctx.Done():
			//log.Debug("context closed")
			return nil
		case cm, ok := <-ms.receiveBuf:
			if !ok {
				log.Debug("buf chan closed")
				return nil
			}
			return cm
		}
	}
}

func (ms *mqMsgStream) getTsMsgFromConsumerMsg(msg mqclient.ConsumerMessage) (TsMsg, error) {
	header := commonpb.MsgHeader{}
	err := proto.Unmarshal(msg.Payload(), &header)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal message header, err %s", err.Error())
	}
	tsMsg, err := ms.unmarshal.Unmarshal(msg.Payload(), header.Base.MsgType)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal tsMsg, err %s", err.Error())
	}

	// set msg info to tsMsg
	tsMsg.SetPosition(&MsgPosition{
		ChannelName: filepath.Base(msg.Topic()),
		MsgID:       msg.ID().Serialize(),
	})

	return tsMsg, nil
}

func (ms *mqMsgStream) receiveMsg(consumer mqclient.Consumer) {
	defer ms.wait.Done()

	for {
		select {
		case <-ms.ctx.Done():
			return
		case msg, ok := <-consumer.Chan():
			if !ok {
				return
			}
			consumer.Ack(msg)

			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				log.Error("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}

			sp, ok := ExtractFromPulsarMsgProperties(tsMsg, msg.Properties())
			if ok {
				tsMsg.SetTraceCtx(opentracing.ContextWithSpan(context.Background(), sp))
			}

			msgPack := MsgPack{Msgs: []TsMsg{tsMsg}}
			ms.receiveBuf <- &msgPack

			sp.Finish()
		}
	}
}

func (ms *mqMsgStream) Chan() <-chan *MsgPack {
	return ms.receiveBuf
}

func (ms *mqMsgStream) Seek(mp *internalpb.MsgPosition) error {
	if _, ok := ms.consumers[mp.ChannelName]; ok {
		consumer := ms.consumers[mp.ChannelName]
		messageID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			return err
		}
		err = consumer.Seek(messageID)
		if err != nil {
			return err
		}
		return nil
	}

	return errors.New("msgStream seek fail")
}

type MqTtMsgStream struct {
	mqMsgStream
	chanMsgBuf      map[mqclient.Consumer][]TsMsg
	chanMsgPos      map[mqclient.Consumer]*internalpb.MsgPosition
	chanStopChan    map[mqclient.Consumer]chan bool
	chanMsgBufMutex *sync.Mutex
	lastTimeStamp   Timestamp
	syncConsumer    chan int
}

func NewMqTtMsgStream(ctx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client mqclient.Client,
	unmarshal UnmarshalDispatcher) (*MqTtMsgStream, error) {
	msgStream, err := NewMqMsgStream(ctx, receiveBufSize, bufSize, client, unmarshal)
	if err != nil {
		return nil, err
	}
	chanMsgBuf := make(map[mqclient.Consumer][]TsMsg)
	chanStopChan := make(map[mqclient.Consumer]chan bool)
	chanMsgPos := make(map[mqclient.Consumer]*internalpb.MsgPosition)
	syncConsumer := make(chan int, 1)

	return &MqTtMsgStream{
		mqMsgStream:     *msgStream,
		chanMsgBuf:      chanMsgBuf,
		chanMsgPos:      chanMsgPos,
		chanStopChan:    chanStopChan,
		chanMsgBufMutex: &sync.Mutex{},
		syncConsumer:    syncConsumer,
	}, nil
}

func (ms *MqTtMsgStream) addConsumer(consumer mqclient.Consumer, channel string) {
	if len(ms.consumers) == 0 {
		ms.syncConsumer <- 1
	}
	ms.consumers[channel] = consumer
	ms.chanMsgBuf[consumer] = make([]TsMsg, 0)
	ms.consumerChannels = append(ms.consumerChannels, channel)
	ms.chanMsgPos[consumer] = &internalpb.MsgPosition{
		ChannelName: channel,
		MsgID:       make([]byte, 0),
		Timestamp:   ms.lastTimeStamp,
	}
	ms.chanStopChan[consumer] = make(chan bool)
}

func (ms *MqTtMsgStream) AsConsumer(channels []string, subName string) {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			receiveChannel := make(chan mqclient.ConsumerMessage, ms.bufSize)
			pc, err := ms.client.Subscribe(mqclient.ConsumerOptions{
				Topic:                       channel,
				SubscriptionName:            subName,
				Type:                        mqclient.KeyShared,
				SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("Consumer is nil")
			}

			ms.consumerLock.Lock()
			ms.addConsumer(pc, channel)
			ms.consumerLock.Unlock()
			return nil
		}
		err := Retry(10, time.Millisecond*200, fn)
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *MqTtMsgStream) Start() {
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

func (ms *MqTtMsgStream) Close() {
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

func (ms *MqTtMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	chanTtMsgSync := make(map[mqclient.Consumer]bool)
	chanTtMsgTime := make(map[mqclient.Consumer]Timestamp)

	// block here until addConsumer
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
				if !chanTtMsgSync[consumer] {
					wg.Add(1)
					go ms.findTimeTick(consumer, chanTtMsgTime, &wg, &findMapMutex)
				}
			}
			wg.Wait()

			// block here until all channels reach same timetick
			timeStamp, ok := checkTimeTickMsg(chanTtMsgTime, chanTtMsgSync, &findMapMutex)
			if !ok || timeStamp <= ms.lastTimeStamp {
				//log.Printf("All timeTick's timestamps are inconsistent")
				ms.consumerLock.Unlock()
				continue
			}

			timeTickBuf := make([]TsMsg, 0)
			startMsgPosition := make([]*internalpb.MsgPosition, 0)
			endMsgPositions := make([]*internalpb.MsgPosition, 0)
			ms.chanMsgBufMutex.Lock()
			for consumer, msgs := range ms.chanMsgBuf {
				if len(msgs) == 0 {
					continue
				}
				tempBuffer := make([]TsMsg, 0)
				var timeTickMsg TsMsg
				for _, v := range msgs {
					if v.Type() == commonpb.MsgType_TimeTick {
						timeTickMsg = v
						continue
					}
					if v.EndTs() <= timeStamp {
						timeTickBuf = append(timeTickBuf, v)
					} else {
						tempBuffer = append(tempBuffer, v)
					}
				}
				ms.chanMsgBuf[consumer] = tempBuffer

				startMsgPosition = append(startMsgPosition, ms.chanMsgPos[consumer])
				var newPos *internalpb.MsgPosition
				if len(tempBuffer) > 0 {
					// if tempBuffer is not empty, use tempBuffer[0] to seek
					newPos = &internalpb.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   timeStamp,
						MsgGroup:    consumer.Subscription(),
					}
					endMsgPositions = append(endMsgPositions, newPos)
				} else {
					// if tempBuffer is empty, use timeTickMsg to seek
					newPos = &internalpb.MsgPosition{
						ChannelName: timeTickMsg.Position().ChannelName,
						MsgID:       timeTickMsg.Position().MsgID,
						Timestamp:   timeStamp,
						MsgGroup:    consumer.Subscription(),
					}
					endMsgPositions = append(endMsgPositions, newPos)
				}
				ms.chanMsgPos[consumer] = newPos
			}
			ms.chanMsgBufMutex.Unlock()
			ms.consumerLock.Unlock()

			msgPack := MsgPack{
				BeginTs:        ms.lastTimeStamp,
				EndTs:          timeStamp,
				Msgs:           timeTickBuf,
				StartPositions: startMsgPosition,
				EndPositions:   endMsgPositions,
			}

			ms.receiveBuf <- &msgPack
			ms.lastTimeStamp = timeStamp
		}
	}
}

// Save all msgs into chanMsgBuf[] till receive one ttMsg
func (ms *MqTtMsgStream) findTimeTick(consumer mqclient.Consumer,
	eofMsgMap map[mqclient.Consumer]Timestamp,
	wg *sync.WaitGroup,
	findMapMutex *sync.RWMutex) {
	defer wg.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		case <-ms.chanStopChan[consumer]:
			return
		case msg, ok := <-consumer.Chan():
			if !ok {
				log.Debug("consumer closed!")
				return
			}
			consumer.Ack(msg)

			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				log.Error("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}

			sp, ok := ExtractFromPulsarMsgProperties(tsMsg, msg.Properties())
			if ok {
				tsMsg.SetTraceCtx(opentracing.ContextWithSpan(context.Background(), sp))
			}

			ms.chanMsgBufMutex.Lock()
			ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
			ms.chanMsgBufMutex.Unlock()

			if tsMsg.Type() == commonpb.MsgType_TimeTick {
				findMapMutex.Lock()
				eofMsgMap[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
				findMapMutex.Unlock()
				sp.Finish()
				return
			}
			sp.Finish()
		}
	}
}

// return true only when all channels reach same timetick
func checkTimeTickMsg(chanTtMsgTime map[mqclient.Consumer]Timestamp,
	chanTtMsgSync map[mqclient.Consumer]bool,
	mu *sync.RWMutex) (Timestamp, bool) {

	timeMap := make(map[Timestamp]int)
	var maxTime Timestamp = 0
	for _, t := range chanTtMsgTime {
		timeMap[t]++
		if t > maxTime {
			maxTime = t
		}
	}
	// when all channels reach same timetick, timeMap should contain only 1 timestamp
	if len(timeMap) <= 1 {
		for consumer := range chanTtMsgTime {
			chanTtMsgSync[consumer] = false
		}
		return maxTime, true
	}
	for consumer := range chanTtMsgTime {
		mu.RLock()
		chanTtMsgSync[consumer] = (chanTtMsgTime[consumer] == maxTime)
		mu.RUnlock()
	}

	return 0, false
}

func (ms *MqTtMsgStream) Seek(mp *internalpb.MsgPosition) error {
	if len(mp.MsgID) == 0 {
		return errors.New("when msgID's length equal to 0, please use AsConsumer interface")
	}
	var consumer mqclient.Consumer
	var err error
	var hasWatched bool
	seekChannel := mp.ChannelName
	subName := mp.MsgGroup
	ms.consumerLock.Lock()
	defer ms.consumerLock.Unlock()
	consumer, hasWatched = ms.consumers[seekChannel]

	if hasWatched {
		return errors.New("the channel should has not been subscribed")
	}

	fn := func() error {
		receiveChannel := make(chan mqclient.ConsumerMessage, ms.bufSize)
		consumer, err = ms.client.Subscribe(mqclient.ConsumerOptions{
			Topic:                       seekChannel,
			SubscriptionName:            subName,
			SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
			Type:                        mqclient.KeyShared,
			MessageChannel:              receiveChannel,
		})
		if err != nil {
			return err
		}
		if consumer == nil {
			err = errors.New("consumer is nil")
			log.Debug("subscribe error", zap.String("error = ", err.Error()))
			return err
		}

		seekMsgID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			log.Debug("convert messageID error", zap.String("error = ", err.Error()))
			return err
		}
		err = consumer.Seek(seekMsgID)
		if err != nil {
			log.Debug("seek error ", zap.String("error = ", err.Error()))
			return err
		}

		return nil
	}
	err = Retry(20, time.Millisecond*200, fn)
	if err != nil {
		errMsg := "Failed to seek, error = " + err.Error()
		panic(errMsg)
	}
	ms.addConsumer(consumer, seekChannel)

	//TODO: May cause problem
	//if len(consumer.Chan()) == 0 {
	//	return nil
	//}

	for {
		select {
		case <-ms.ctx.Done():
			return nil
		case msg, ok := <-consumer.Chan():
			if !ok {
				return errors.New("consumer closed")
			}
			consumer.Ack(msg)

			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				log.Error("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}

			if tsMsg.Type() == commonpb.MsgType_TimeTick {
				if tsMsg.BeginTs() >= mp.Timestamp {
					return nil
				}
				continue
			}
			if tsMsg.BeginTs() > mp.Timestamp {
				ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
			}
		}
	}
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
