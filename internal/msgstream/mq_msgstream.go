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
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
)

var _ MsgStream = (*mqMsgStream)(nil)

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

// NewMqMsgStream is used to generate a new mqMsgStream object
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

// AsProducer create producer to send message to channels
func (ms *mqMsgStream) AsProducer(channels []string) {
	for _, channel := range channels {
		if len(channel) == 0 {
			log.Error("MsgStream asProducer's channel is a empty string")
			break
		}
		fn := func() error {
			pp, err := ms.client.CreateProducer(mqclient.ProducerOptions{Topic: channel})
			if err != nil {
				return err
			}
			if pp == nil {
				return errors.New("Producer is nil")
			}

			ms.producerLock.Lock()
			defer ms.producerLock.Unlock()
			ms.producers[channel] = pp
			ms.producerChannels = append(ms.producerChannels, channel)
			return nil
		}
		err := retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200))
		if err != nil {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

// Create consumer to receive message from channels
func (ms *mqMsgStream) AsConsumer(channels []string, subName string) {
	ms.AsConsumerWithPosition(channels, subName, mqclient.SubscriptionPositionEarliest)
}

// Create consumer to receive message from channels, with initial position
func (ms *mqMsgStream) AsConsumerWithPosition(channels []string, subName string, position mqclient.SubscriptionInitialPosition) {
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
				SubscriptionInitialPosition: position,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("Consumer is nil")
			}

			ms.consumerLock.Lock()
			defer ms.consumerLock.Unlock()
			ms.consumers[channel] = pc
			ms.consumerChannels = append(ms.consumerChannels, channel)
			return nil
		}
		err := retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200))
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
	for _, c := range ms.consumers {
		ms.wait.Add(1)
		go ms.receiveMsg(c)
	}
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

			m, err := convertToByteArray(mb)
			if err != nil {
				return err
			}

			msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

			trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

			ms.producerLock.Lock()
			if _, err := ms.producers[channel].Send(
				spanCtx,
				msg,
			); err != nil {
				ms.producerLock.Unlock()
				trace.LogError(sp, err)
				sp.Finish()
				return err
			}
			sp.Finish()
			ms.producerLock.Unlock()
		}
	}
	return nil
}

// ProduceMark send msg pack to all producers and returns corresponding msg id
// the returned message id serves as marking
func (ms *mqMsgStream) ProduceMark(msgPack *MsgPack) (map[string][]MessageID, error) {
	ids := make(map[string][]MessageID)
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		return ids, errors.New("empty msgs")
	}
	if len(ms.producers) <= 0 {
		return ids, errors.New("nil producer in msg stream")
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
		return ids, err
	}
	for k, v := range result {
		channel := ms.producerChannels[k]
		for i, tsMsg := range v.Msgs {
			sp, spanCtx := MsgSpanFromCtx(v.Msgs[i].TraceCtx(), tsMsg)

			mb, err := tsMsg.Marshal(tsMsg)
			if err != nil {
				return ids, err
			}

			m, err := convertToByteArray(mb)
			if err != nil {
				return ids, err
			}

			msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

			trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

			ms.producerLock.Lock()
			id, err := ms.producers[channel].Send(
				spanCtx,
				msg,
			)
			if err != nil {
				ms.producerLock.Unlock()
				trace.LogError(sp, err)
				sp.Finish()
				return ids, err
			}
			ids[channel] = append(ids[channel], id)
			sp.Finish()
			ms.producerLock.Unlock()
		}
	}
	return ids, nil
}

// Broadcast put msgPack to all producer in current msgstream
// which ignores repackFunc logic
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

		m, err := convertToByteArray(mb)
		if err != nil {
			return err
		}

		msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

		trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

		ms.producerLock.Lock()
		for _, producer := range ms.producers {
			if _, err := producer.Send(
				spanCtx,
				msg,
			); err != nil {
				ms.producerLock.Unlock()
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

// BroadcastMark broadcast msg pack to all producers and returns corresponding msg id
// the returned message id serves as marking
func (ms *mqMsgStream) BroadcastMark(msgPack *MsgPack) (map[string][]MessageID, error) {
	ids := make(map[string][]MessageID)
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		return ids, errors.New("empty msgs")
	}
	for _, v := range msgPack.Msgs {
		sp, spanCtx := MsgSpanFromCtx(v.TraceCtx(), v)

		mb, err := v.Marshal(v)
		if err != nil {
			return ids, err
		}

		m, err := convertToByteArray(mb)
		if err != nil {
			return ids, err
		}

		msg := &mqclient.ProducerMessage{Payload: m, Properties: map[string]string{}}

		trace.InjectContextToPulsarMsgProperties(sp.Context(), msg.Properties)

		ms.producerLock.Lock()
		for channel, producer := range ms.producers {
			id, err := producer.Send(spanCtx, msg)
			if err != nil {
				ms.producerLock.Unlock()
				trace.LogError(sp, err)
				sp.Finish()
				return ids, err
			}
			ids[channel] = append(ids[channel], id)
		}
		ms.producerLock.Unlock()
		sp.Finish()
	}
	return ids, nil
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
			pos := tsMsg.Position()
			tsMsg.SetPosition(&MsgPosition{
				ChannelName: pos.ChannelName,
				MsgID:       pos.MsgID,
				MsgGroup:    consumer.Subscription(),
				Timestamp:   tsMsg.BeginTs(),
			})

			sp, ok := ExtractFromPulsarMsgProperties(tsMsg, msg.Properties())
			if ok {
				tsMsg.SetTraceCtx(opentracing.ContextWithSpan(context.Background(), sp))
			}

			msgPack := MsgPack{
				Msgs:           []TsMsg{tsMsg},
				StartPositions: []*internalpb.MsgPosition{tsMsg.Position()},
				EndPositions:   []*internalpb.MsgPosition{tsMsg.Position()},
			}
			ms.receiveBuf <- &msgPack

			sp.Finish()
		}
	}
}

func (ms *mqMsgStream) Chan() <-chan *MsgPack {
	return ms.receiveBuf
}

// Seek reset the subscription associated with this consumer to a specific position
func (ms *mqMsgStream) Seek(msgPositions []*internalpb.MsgPosition) error {
	for _, mp := range msgPositions {
		consumer, ok := ms.consumers[mp.ChannelName]
		if !ok {
			return fmt.Errorf("channel %s not subscribed", mp.ChannelName)
		}
		messageID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			return err
		}
		log.Debug("MsgStream begin to seek", zap.Any("MessageID", messageID))
		err = consumer.Seek(messageID)
		if err != nil {
			return err
		}
		log.Debug("MsgStream seek finished", zap.Any("MessageID", messageID))
		if _, ok := consumer.(*mqclient.RmqConsumer); !ok {
			log.Debug("MsgStream begin to read one message after seek")
			msg, ok := <-consumer.Chan()
			if !ok {
				return errors.New("consumer closed")
			}
			log.Debug("MsgStream finish reading one message after seek")
			consumer.Ack(msg)
			if !bytes.Equal(msg.ID().Serialize(), messageID.Serialize()) {
				err = fmt.Errorf("seek msg not correct")
				log.Error("msMsgStream seek", zap.Error(err))
			}
		}
	}
	return nil
}

var _ MsgStream = (*MqTtMsgStream)(nil)

// MqTtMsgStream is a msgstream that contains timeticks
type MqTtMsgStream struct {
	mqMsgStream
	chanMsgBuf         map[mqclient.Consumer][]TsMsg
	chanMsgPos         map[mqclient.Consumer]*internalpb.MsgPosition
	chanStopChan       map[mqclient.Consumer]chan bool
	chanTtMsgTime      map[mqclient.Consumer]Timestamp
	chanMsgBufMutex    *sync.Mutex
	chanTtMsgTimeMutex *sync.RWMutex
	chanWaitGroup      *sync.WaitGroup
	lastTimeStamp      Timestamp
	syncConsumer       chan int
}

// NewMqTtMsgStream is used to generate a new MqTtMsgStream object
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
	chanMsgPos := make(map[mqclient.Consumer]*internalpb.MsgPosition)
	chanStopChan := make(map[mqclient.Consumer]chan bool)
	chanTtMsgTime := make(map[mqclient.Consumer]Timestamp)
	syncConsumer := make(chan int, 1)

	return &MqTtMsgStream{
		mqMsgStream:        *msgStream,
		chanMsgBuf:         chanMsgBuf,
		chanMsgPos:         chanMsgPos,
		chanStopChan:       chanStopChan,
		chanTtMsgTime:      chanTtMsgTime,
		chanMsgBufMutex:    &sync.Mutex{},
		chanTtMsgTimeMutex: &sync.RWMutex{},
		chanWaitGroup:      &sync.WaitGroup{},
		syncConsumer:       syncConsumer,
	}, nil
}

func (ms *MqTtMsgStream) addConsumer(consumer mqclient.Consumer, channel string) {
	if len(ms.consumers) == 0 {
		ms.syncConsumer <- 1
	}
	ms.consumers[channel] = consumer
	ms.consumerChannels = append(ms.consumerChannels, channel)
	ms.chanMsgBuf[consumer] = make([]TsMsg, 0)
	ms.chanMsgPos[consumer] = &internalpb.MsgPosition{
		ChannelName: channel,
		MsgID:       make([]byte, 0),
		Timestamp:   ms.lastTimeStamp,
	}
	ms.chanStopChan[consumer] = make(chan bool)
	ms.chanTtMsgTime[consumer] = 0
}

// AsConsumer subscribes channels as consumer for a MsgStream
func (ms *MqTtMsgStream) AsConsumer(channels []string, subName string) {
	ms.AsConsumerWithPosition(channels, subName, mqclient.SubscriptionPositionEarliest)
}

func (ms *MqTtMsgStream) AsConsumerWithPosition(channels []string, subName string, position mqclient.SubscriptionInitialPosition) {
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
				SubscriptionInitialPosition: position,
				MessageChannel:              receiveChannel,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return errors.New("Consumer is nil")
			}

			ms.consumerLock.Lock()
			defer ms.consumerLock.Unlock()
			ms.addConsumer(pc, channel)
			return nil
		}
		err := retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200))
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

// Start will start a goroutine which keep carrying msg from pulsar/rocksmq to golang chan
func (ms *MqTtMsgStream) Start() {
	if ms.consumers != nil {
		ms.wait.Add(1)
		go ms.bufMsgPackToChannel()
	}
}

// Close will stop goroutine and free internal producers and consumers
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
}

func (ms *MqTtMsgStream) bufMsgPackToChannel() {
	defer ms.wait.Done()
	chanTtMsgSync := make(map[mqclient.Consumer]bool)

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
			ms.consumerLock.Lock()

			// wait all channels get ttMsg
			for _, consumer := range ms.consumers {
				if !chanTtMsgSync[consumer] {
					ms.chanWaitGroup.Add(1)
					go ms.consumeToTtMsg(consumer)
				}
			}
			ms.chanWaitGroup.Wait()

			// block here until all channels reach same timetick
			currTs, ok := ms.allChanReachSameTtMsg(chanTtMsgSync)
			if !ok || currTs <= ms.lastTimeStamp {
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
					if v.EndTs() <= currTs {
						timeTickBuf = append(timeTickBuf, v)
						//log.Debug("pack msg", zap.Uint64("curr", v.EndTs()), zap.Uint64("currTs", currTs))
					} else {
						tempBuffer = append(tempBuffer, v)
					}
				}
				ms.chanMsgBuf[consumer] = tempBuffer

				startMsgPosition = append(startMsgPosition, proto.Clone(ms.chanMsgPos[consumer]).(*internalpb.MsgPosition))
				var newPos *internalpb.MsgPosition
				if len(tempBuffer) > 0 {
					// if tempBuffer is not empty, use tempBuffer[0] to seek
					newPos = &internalpb.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   currTs,
						MsgGroup:    consumer.Subscription(),
					}
					endMsgPositions = append(endMsgPositions, newPos)
				} else if timeTickMsg != nil {
					// if tempBuffer is empty, use timeTickMsg to seek
					newPos = &internalpb.MsgPosition{
						ChannelName: timeTickMsg.Position().ChannelName,
						MsgID:       timeTickMsg.Position().MsgID,
						Timestamp:   currTs,
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
				EndTs:          currTs,
				Msgs:           timeTickBuf,
				StartPositions: startMsgPosition,
				EndPositions:   endMsgPositions,
			}

			//log.Debug("send msg pack", zap.Int("len", len(msgPack.Msgs)), zap.Uint64("currTs", currTs))
			ms.receiveBuf <- &msgPack
			ms.lastTimeStamp = currTs
		}
	}
}

// Save all msgs into chanMsgBuf[] till receive one ttMsg
func (ms *MqTtMsgStream) consumeToTtMsg(consumer mqclient.Consumer) {
	defer ms.chanWaitGroup.Done()
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
				ms.chanTtMsgTimeMutex.Lock()
				ms.chanTtMsgTime[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
				ms.chanTtMsgTimeMutex.Unlock()
				sp.Finish()
				return
			}
			sp.Finish()
		}
	}
}

// return true only when all channels reach same timetick
func (ms *MqTtMsgStream) allChanReachSameTtMsg(chanTtMsgSync map[mqclient.Consumer]bool) (Timestamp, bool) {
	tsMap := make(map[Timestamp]int)
	var maxTime Timestamp = 0
	for _, t := range ms.chanTtMsgTime {
		tsMap[t]++
		if t > maxTime {
			maxTime = t
		}
	}
	// when all channels reach same timetick, timeMap should contain only 1 timestamp
	if len(tsMap) <= 1 {
		for consumer := range ms.chanTtMsgTime {
			chanTtMsgSync[consumer] = false
		}
		return maxTime, true
	}
	for consumer := range ms.chanTtMsgTime {
		ms.chanTtMsgTimeMutex.RLock()
		chanTtMsgSync[consumer] = (ms.chanTtMsgTime[consumer] == maxTime)
		ms.chanTtMsgTimeMutex.RUnlock()
	}

	return 0, false
}

// Seek to the specified position
func (ms *MqTtMsgStream) Seek(msgPositions []*internalpb.MsgPosition) error {
	var consumer mqclient.Consumer
	var mp *MsgPosition
	var err error
	fn := func() error {
		var ok bool
		consumer, ok = ms.consumers[mp.ChannelName]
		if !ok {
			return fmt.Errorf("please subcribe the channel, channel name =%s", mp.ChannelName)
		}

		if consumer == nil {
			return fmt.Errorf("consumer is nil")
		}

		seekMsgID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			return err
		}
		err = consumer.Seek(seekMsgID)
		if err != nil {
			return err
		}

		return nil
	}

	ms.consumerLock.Lock()
	defer ms.consumerLock.Unlock()

	for idx := range msgPositions {
		mp = msgPositions[idx]
		if len(mp.MsgID) == 0 {
			return fmt.Errorf("when msgID's length equal to 0, please use AsConsumer interface")
		}
		if err = retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200)); err != nil {
			return fmt.Errorf("Failed to seek, error %s", err.Error())
		}
		ms.addConsumer(consumer, mp.ChannelName)

		runLoop := true
		for runLoop {
			select {
			case <-ms.ctx.Done():
				return nil
			case msg, ok := <-consumer.Chan():
				if !ok {
					return fmt.Errorf("consumer closed")
				}
				consumer.Ack(msg)

				headerMsg := commonpb.MsgHeader{}
				err := proto.Unmarshal(msg.Payload(), &headerMsg)
				if err != nil {
					return fmt.Errorf("Failed to unmarshal message header, err %s", err.Error())
				}
				tsMsg, err := ms.unmarshal.Unmarshal(msg.Payload(), headerMsg.Base.MsgType)
				if err != nil {
					return fmt.Errorf("Failed to unmarshal tsMsg, err %s", err.Error())
				}
				if tsMsg.Type() == commonpb.MsgType_TimeTick && tsMsg.BeginTs() >= mp.Timestamp {
					runLoop = false
					break
				} else if tsMsg.BeginTs() > mp.Timestamp {
					tsMsg.SetPosition(&MsgPosition{
						ChannelName: filepath.Base(msg.Topic()),
						MsgID:       msg.ID().Serialize(),
					})
					ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
				}
			}
		}
	}
	return nil
}
