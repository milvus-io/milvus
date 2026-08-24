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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ MsgStream = (*mqMsgStream)(nil)

type mqMsgStream struct {
	ctx              context.Context
	client           mqwrapper.Client
	producers        map[string]mqwrapper.Producer
	producerChannels []string
	consumers        map[string]mqwrapper.Consumer
	consumerChannels []string

	repackFunc   RepackFunc
	unmarshal    UnmarshalDispatcher
	receiveBuf   chan *ConsumeMsgPack
	closeRWMutex *sync.RWMutex
	streamCancel func()
	bufSize      int64
	producerLock *sync.RWMutex
	consumerLock *sync.Mutex
	closed       int32
	onceChan     sync.Once
}

// NewMqMsgStream is used to generate a new mqMsgStream object
func NewMqMsgStream(initCtx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client mqwrapper.Client,
	unmarshal UnmarshalDispatcher,
) (*mqMsgStream, error) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	producers := make(map[string]mqwrapper.Producer)
	consumers := make(map[string]mqwrapper.Consumer)
	producerChannels := make([]string, 0)
	consumerChannels := make([]string, 0)
	receiveBuf := make(chan *ConsumeMsgPack, receiveBufSize)

	stream := &mqMsgStream{
		ctx:              streamCtx,
		streamCancel:     streamCancel,
		client:           client,
		producers:        producers,
		producerChannels: producerChannels,
		consumers:        consumers,
		consumerChannels: consumerChannels,

		unmarshal:    unmarshal,
		bufSize:      bufSize,
		receiveBuf:   receiveBuf,
		producerLock: &sync.RWMutex{},
		consumerLock: &sync.Mutex{},
		closeRWMutex: &sync.RWMutex{},
		closed:       0,
	}
	mlog.Info(initCtx, "Msg Stream initialized")

	return stream, nil
}

// AsProducer create producer to send message to channels
func (ms *mqMsgStream) AsProducer(ctx context.Context, channels []string) {
	for _, channel := range channels {
		if len(channel) == 0 {
			mlog.Error(ctx, "MsgStream asProducer's channel is an empty string")
			break
		}

		fn := func() error {
			pp, err := ms.client.CreateProducer(ctx, common.ProducerOptions{Topic: channel, EnableCompression: true})
			if err != nil {
				return err
			}
			if pp == nil {
				return merr.WrapErrMqInternalMsg("Producer is nil")
			}

			ms.producerLock.Lock()
			defer ms.producerLock.Unlock()
			ms.producers[channel] = pp
			ms.producerChannels = append(ms.producerChannels, channel)
			return nil
		}
		err := retry.Do(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *mqMsgStream) GetLatestMsgID(channel string) (MessageID, error) {
	lastMsg, err := ms.consumers[channel].GetLatestMsgID()
	if err != nil {
		return nil, merr.WrapErrMqInternal(err, "Failed to get latest MsgID from channel: "+channel)
	}
	return lastMsg, nil
}

func (ms *mqMsgStream) CheckTopicValid(channel string) error {
	err := ms.consumers[channel].CheckTopicValid(channel)
	if err != nil {
		return err
	}
	return nil
}

// AsConsumerWithPosition Create consumer to receive message from channels, with initial position
// if initial position is set to latest, last message in the channel is exclusive
func (ms *mqMsgStream) AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			pc, err := ms.client.Subscribe(ctx, mqwrapper.ConsumerOptions{
				Topic:                       channel,
				SubscriptionName:            subName,
				SubscriptionInitialPosition: position,
				BufSize:                     ms.bufSize,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return merr.WrapErrMqInternalMsg("Consumer is nil")
			}

			ms.consumerLock.Lock()
			defer ms.consumerLock.Unlock()
			ms.consumers[channel] = pc
			ms.consumerChannels = append(ms.consumerChannels, channel)
			return nil
		}

		err := retry.Do(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := fmt.Sprintf("Failed to create consumer %s", channel)
			if merr.IsCanceledOrTimeout(err) {
				return errors.Wrapf(err, errMsg)
			}

			panic(fmt.Sprintf("%s, errors = %s", errMsg, err.Error()))
		}
		mlog.Info(ms.ctx, "Successfully create consumer", mlog.String("channel", channel), mlog.String("subname", subName))
	}
	return nil
}

func (ms *mqMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *mqMsgStream) Close() {
	log := mlog.With(mlog.Strings("producers", ms.producerChannels),
		mlog.Strings("consumers", ms.consumerChannels))
	log.Info(ms.ctx, "start to close mq msg stream")
	ms.streamCancel()
	ms.closeRWMutex.Lock()
	defer ms.closeRWMutex.Unlock()
	if !atomic.CompareAndSwapInt32(&ms.closed, 0, 1) {
		return
	}
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

	ms.client.Close()
	close(ms.receiveBuf)
	log.Info(ms.ctx, "mq msg stream closed")
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

func (ms *mqMsgStream) Produce(ctx context.Context, msgPack *MsgPack) error {
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		mlog.Debug(ms.ctx, "Warning: Receive empty msgPack")
		return nil
	}
	if len(ms.producers) <= 0 {
		return merr.WrapErrServiceInternal("nil producer in msg stream")
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
	eg, _ := errgroup.WithContext(context.Background())
	for k, v := range result {
		k := k
		v := v
		eg.Go(func() error {
			ms.producerLock.RLock()
			channel := ms.producerChannels[k]
			producer, ok := ms.producers[channel]
			ms.producerLock.RUnlock()
			if !ok {
				return merr.WrapErrMqInternalMsg("producer not found for channel: %s", channel)
			}

			for i := 0; i < len(v.Msgs); i++ {
				spanCtx, sp := MsgSpanFromCtx(v.Msgs[i].TraceCtx(), v.Msgs[i])
				defer sp.End()

				mb, err := v.Msgs[i].Marshal(v.Msgs[i])
				if err != nil {
					return err
				}

				m, err := convertToByteArray(mb)
				if err != nil {
					return err
				}

				msg := &common.ProducerMessage{Payload: m, Properties: GetPorperties(v.Msgs[i])}
				InjectCtx(spanCtx, msg.Properties)

				if _, err := producer.Send(spanCtx, msg); err != nil {
					sp.RecordError(err)
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// BroadcastMark broadcast msg pack to all producers and returns corresponding msg id
// the returned message id serves as marking
func (ms *mqMsgStream) Broadcast(ctx context.Context, msgPack *MsgPack) (map[string][]MessageID, error) {
	ids := make(map[string][]MessageID)
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		return ids, merr.WrapErrParameterInvalidMsg("empty msgs")
	}

	for _, v := range msgPack.Msgs {
		spanCtx, sp := MsgSpanFromCtx(v.TraceCtx(), v)

		mb, err := v.Marshal(v)
		if err != nil {
			return ids, err
		}

		m, err := convertToByteArray(mb)
		if err != nil {
			return ids, err
		}

		msg := &common.ProducerMessage{Payload: m, Properties: GetPorperties(v)}
		InjectCtx(spanCtx, msg.Properties)

		ms.producerLock.RLock()
		// since the element never be removed in ms.producers, so it's safe to clone and iterate producers
		producers := maps.Clone(ms.producers)
		ms.producerLock.RUnlock()

		for channel, producer := range producers {
			id, err := producer.Send(spanCtx, msg)
			if err != nil {
				sp.RecordError(err)
				sp.End()
				return ids, err
			}
			ids[channel] = append(ids[channel], id)
		}
		sp.End()
	}
	return ids, nil
}

// GetTsMsgFromConsumerMsg get TsMsg from consumer message
func GetTsMsgFromConsumerMsg(unmarshalDispatcher UnmarshalDispatcher, msg common.Message) (TsMsg, error) {
	msgType, err := common.GetMsgType(msg)
	if err != nil {
		return nil, err
	}
	tsMsg, err := unmarshalDispatcher.Unmarshal(msg.Payload(), msgType)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "failed to unmarshal tsMsg")
	}

	tsMsg.SetPosition(&MsgPosition{
		ChannelName: filepath.Base(msg.Topic()),
		MsgID:       msg.ID().Serialize(),
	})

	return tsMsg, nil
}

func (ms *mqMsgStream) receiveMsg(consumer mqwrapper.Consumer) {
	ms.closeRWMutex.RLock()
	defer ms.closeRWMutex.RUnlock()
	if atomic.LoadInt32(&ms.closed) != 0 {
		return
	}

	for {
		select {
		case <-ms.ctx.Done():
			return
		case msg, ok := <-consumer.Chan():
			if !ok {
				return
			}
			consumer.Ack(msg)
			if msg.Payload() == nil {
				mlog.Warn(ms.ctx, "MqMsgStream get msg whose payload is nil")
				continue
			}
			if message.CheckIfMessageFromStreaming(msg.Properties()) {
				mlog.Warn(ms.ctx, "MqMsgStream can not consume the message from streaming service")
				continue
			}

			var err error
			var packMsg ConsumeMsg

			packMsg, err = NewMarshaledMsg(msg, consumer.Subscription())
			if err != nil {
				packMsg, err = UnmarshalMsg(msg, ms.unmarshal)
				if err != nil {
					mlog.Warn(ms.ctx, "Failed to getTsMsgFromConsumerMsg", mlog.Err(err))
					continue
				}
			}

			pos := &msgpb.MsgPosition{
				ChannelName: filepath.Base(msg.Topic()),
				MsgID:       packMsg.GetMessageID(),
				MsgGroup:    consumer.Subscription(),
				Timestamp:   packMsg.GetTimestamp(),
			}

			packMsg.SetPosition(pos)
			msgPack := ConsumeMsgPack{
				Msgs:           []ConsumeMsg{packMsg},
				StartPositions: []*msgpb.MsgPosition{pos},
				EndPositions:   []*msgpb.MsgPosition{pos},
				BeginTs:        packMsg.GetTimestamp(),
				EndTs:          packMsg.GetTimestamp(),
			}

			select {
			case ms.receiveBuf <- &msgPack:
			case <-ms.ctx.Done():
				return
			}
		}
	}
}

func (ms *mqMsgStream) GetUnmarshalDispatcher() UnmarshalDispatcher {
	return ms.unmarshal
}

func (ms *mqMsgStream) Chan() <-chan *ConsumeMsgPack {
	ms.onceChan.Do(func() {
		for _, c := range ms.consumers {
			go ms.receiveMsg(c)
		}
	})

	return ms.receiveBuf
}

// Seek reset the subscription associated with this consumer to a specific position, the seek position is exclusive
// User has to ensure mq_msgstream is not closed before seek, and the seek position is already written.
func (ms *mqMsgStream) Seek(ctx context.Context, msgPositions []*MsgPosition, includeCurrentMsg bool) error {
	for _, mp := range msgPositions {
		consumer, ok := ms.consumers[mp.ChannelName]
		if !ok {
			return merr.WrapErrMqInternalMsg("channel %s not subscribed", mp.ChannelName)
		}
		messageID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			if paramtable.Get().MQCfg.IgnoreBadPosition.GetAsBool() {
				// try to use latest message ID first
				messageID, err = consumer.GetLatestMsgID()
				if err != nil {
					mlog.Warn(ctx, "Ignoring bad message id", mlog.Err(err))
					continue
				}
			} else {
				return err
			}
		}
		mlog.Info(ctx, "MsgStream seek begin", mlog.String("channel", mp.ChannelName), mlog.Any("MessageID", mp.MsgID), mlog.Bool("includeCurrentMsg", includeCurrentMsg))
		err = consumer.Seek(messageID, includeCurrentMsg)
		if err != nil {
			mlog.Warn(ctx, "Failed to seek", mlog.String("channel", mp.ChannelName), mlog.Err(err))
			return err
		}
		mlog.Info(ctx, "MsgStream seek finished", mlog.String("channel", mp.ChannelName))
	}
	return nil
}

var _ MsgStream = (*MqTtMsgStream)(nil)

// MqTtMsgStream is a msgstream that contains timeticks
type MqTtMsgStream struct {
	*mqMsgStream
	chanMsgBuf         map[mqwrapper.Consumer][]ConsumeMsg
	chanMsgPos         map[mqwrapper.Consumer]*msgpb.MsgPosition
	chanStopChan       map[mqwrapper.Consumer]chan bool
	chanTtMsgTime      map[mqwrapper.Consumer]Timestamp
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
	client mqwrapper.Client,
	unmarshal UnmarshalDispatcher,
) (*MqTtMsgStream, error) {
	msgStream, err := NewMqMsgStream(ctx, receiveBufSize, bufSize, client, unmarshal)
	if err != nil {
		return nil, err
	}
	chanMsgBuf := make(map[mqwrapper.Consumer][]ConsumeMsg)
	chanMsgPos := make(map[mqwrapper.Consumer]*msgpb.MsgPosition)
	chanStopChan := make(map[mqwrapper.Consumer]chan bool)
	chanTtMsgTime := make(map[mqwrapper.Consumer]Timestamp)
	syncConsumer := make(chan int, 1)

	return &MqTtMsgStream{
		mqMsgStream:        msgStream,
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

func (ms *MqTtMsgStream) addConsumer(consumer mqwrapper.Consumer, channel string) {
	if len(ms.consumers) == 0 {
		ms.syncConsumer <- 1
	}
	ms.consumers[channel] = consumer
	ms.consumerChannels = append(ms.consumerChannels, channel)
	ms.chanMsgBuf[consumer] = make([]ConsumeMsg, 0)
	ms.chanMsgPos[consumer] = &msgpb.MsgPosition{
		ChannelName: channel,
		MsgID:       make([]byte, 0),
		Timestamp:   ms.lastTimeStamp,
	}
	ms.chanStopChan[consumer] = make(chan bool)
	ms.chanTtMsgTime[consumer] = 0
}

// AsConsumerWithPosition subscribes channels as consumer for a MsgStream and seeks to a certain position.
func (ms *MqTtMsgStream) AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			pc, err := ms.client.Subscribe(ctx, mqwrapper.ConsumerOptions{
				Topic:                       channel,
				SubscriptionName:            subName,
				SubscriptionInitialPosition: position,
				BufSize:                     ms.bufSize,
			})
			if err != nil {
				return err
			}
			if pc == nil {
				return merr.WrapErrMqInternalMsg("Consumer is nil")
			}

			ms.consumerLock.Lock()
			defer ms.consumerLock.Unlock()
			ms.addConsumer(pc, channel)
			return nil
		}

		err := retry.Do(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := fmt.Sprintf("Failed to create consumer %s", channel)
			if merr.IsCanceledOrTimeout(err) {
				return errors.Wrapf(err, errMsg)
			}

			panic(fmt.Sprintf("%s, subName = %s, errors = %s", errMsg, subName, err.Error()))
		}
	}

	return nil
}

// Close will stop goroutine and free internal producers and consumers
func (ms *MqTtMsgStream) Close() {
	close(ms.syncConsumer)
	ms.mqMsgStream.Close()
}

func isDMLMsg(msg ConsumeMsg) bool {
	return msg.GetType() == commonpb.MsgType_Insert || msg.GetType() == commonpb.MsgType_Delete
}

func (ms *MqTtMsgStream) continueBuffering(endTs, size uint64, startTime time.Time) bool {
	if ms.ctx.Err() != nil {
		return false
	}

	// first run
	if endTs == 0 {
		return true
	}

	// pursuit mode not enabled
	if !paramtable.Get().MQCfg.EnablePursuitMode.GetAsBool() {
		return false
	}

	// buffer full
	if size > paramtable.Get().MQCfg.PursuitBufferSize.GetAsUint64() {
		return false
	}

	if time.Since(startTime) > paramtable.Get().MQCfg.PursuitBufferTime.GetAsDuration(time.Second) {
		return false
	}

	endTime, _ := tsoutil.ParseTS(endTs)
	return time.Since(endTime) > paramtable.Get().MQCfg.PursuitLag.GetAsDuration(time.Second)
}

func (ms *MqTtMsgStream) bufMsgPackToChannel() {
	ms.closeRWMutex.RLock()
	defer ms.closeRWMutex.RUnlock()
	if atomic.LoadInt32(&ms.closed) != 0 {
		return
	}
	chanTtMsgSync := make(map[mqwrapper.Consumer]bool)

	// block here until addConsumer
	if _, ok := <-ms.syncConsumer; !ok {
		mlog.Warn(ms.ctx, "consumer closed!")
		return
	}

	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			timeTickBuf := make([]ConsumeMsg, 0)
			// startMsgPosition := make([]*msgpb.MsgPosition, 0)
			// endMsgPositions := make([]*msgpb.MsgPosition, 0)
			startPositions := make(map[string]*msgpb.MsgPosition)
			endPositions := make(map[string]*msgpb.MsgPosition)
			startBufTime := time.Now()
			var endTs uint64
			var size uint64
			var containsEndBufferMsg bool

			for ms.continueBuffering(endTs, size, startBufTime) && !containsEndBufferMsg {
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
					ms.consumerLock.Unlock()
					continue
				}
				endTs = currTs

				ms.chanMsgBufMutex.Lock()
				for consumer, msgs := range ms.chanMsgBuf {
					if len(msgs) == 0 {
						continue
					}
					startPos := typeutil.Clone(ms.chanMsgPos[consumer])
					channelName := startPos.ChannelName
					if _, ok := startPositions[channelName]; !ok {
						startPositions[channelName] = startPos
					}
					tempBuffer := make([]ConsumeMsg, 0)
					var timeTickMsg ConsumeMsg
					for _, v := range msgs {
						if v.GetType() == commonpb.MsgType_TimeTick {
							timeTickMsg = v
							continue
						}
						if v.GetTimestamp() <= currTs {
							size += uint64(v.GetSize())
							timeTickBuf = append(timeTickBuf, v)
						} else {
							tempBuffer = append(tempBuffer, v)
						}
						// when drop collection, force to exit the buffer loop
						if v.GetType() == commonpb.MsgType_DropCollection {
							containsEndBufferMsg = true
						}
					}
					ms.chanMsgBuf[consumer] = tempBuffer

					//	startMsgPosition = append(startMsgPosition, proto.Clone(ms.chanMsgPos[consumer]).(*msgpb.MsgPosition))
					var newPos *msgpb.MsgPosition
					if len(tempBuffer) > 0 {
						// if tempBuffer is not empty, use tempBuffer[0] to seek
						newPos = &msgpb.MsgPosition{
							ChannelName: tempBuffer[0].GetPChannel(),
							MsgID:       tempBuffer[0].GetMessageID(),
							Timestamp:   currTs,
							MsgGroup:    consumer.Subscription(),
						}
						endPositions[channelName] = newPos
					} else if timeTickMsg != nil {
						// if tempBuffer is empty, use timeTickMsg to seek
						newPos = &msgpb.MsgPosition{
							ChannelName: timeTickMsg.GetPChannel(),
							MsgID:       timeTickMsg.GetMessageID(),
							Timestamp:   currTs,
							MsgGroup:    consumer.Subscription(),
						}
						endPositions[channelName] = newPos
					}
					ms.chanMsgPos[consumer] = newPos
				}
				ms.chanMsgBufMutex.Unlock()
				ms.consumerLock.Unlock()
			}

			idset := make(typeutil.Set[int64])
			uniqueMsgs := make([]ConsumeMsg, 0, len(timeTickBuf))
			for _, msg := range timeTickBuf {
				if isDMLMsg(msg) && idset.Contain(msg.GetID()) {
					mlog.Warn(ms.ctx, "mqTtMsgStream, found duplicated msg", mlog.Int64("msgID", msg.GetID()))
					continue
				}
				idset.Insert(msg.GetID())
				uniqueMsgs = append(uniqueMsgs, msg)
			}

			// skip endTs = 0 (no run for ctx error)
			if endTs > 0 {
				msgPack := ConsumeMsgPack{
					BeginTs:        ms.lastTimeStamp,
					EndTs:          endTs,
					Msgs:           uniqueMsgs,
					StartPositions: lo.MapToSlice(startPositions, func(_ string, pos *msgpb.MsgPosition) *msgpb.MsgPosition { return pos }),
					EndPositions:   lo.MapToSlice(endPositions, func(_ string, pos *msgpb.MsgPosition) *msgpb.MsgPosition { return pos }),
				}

				select {
				case ms.receiveBuf <- &msgPack:
				case <-ms.ctx.Done():
					return
				}
				ms.lastTimeStamp = endTs
			}
		}
	}
}

// Save all msgs into chanMsgBuf[] till receive one ttMsg
func (ms *MqTtMsgStream) consumeToTtMsg(consumer mqwrapper.Consumer) {
	defer ms.chanWaitGroup.Done()
	msgTick := time.NewTimer(3 * time.Second)
	defer msgTick.Stop()
	for {
		msgTick.Reset(3 * time.Second)
		select {
		case <-ms.ctx.Done():
			return
		case <-ms.chanStopChan[consumer]:
			return
		case <-msgTick.C:
			mlog.Info(ms.ctx, "stop consumer, because no msg received in 3s", mlog.Strings("channel", ms.consumerChannels))
			return
		case msg, ok := <-consumer.Chan():
			if !ok {
				mlog.Debug(ms.ctx, "consumer closed!")
				return
			}
			consumer.Ack(msg)

			if msg.Payload() == nil {
				mlog.Warn(ms.ctx, "MqTtMsgStream get msg whose payload is nil")
				continue
			}
			if message.CheckIfMessageFromStreaming(msg.Properties()) {
				mlog.Warn(ms.ctx, "MqTtMsgStream can not consume the message from streaming service")
				continue
			}

			var err error
			var packMsg ConsumeMsg

			packMsg, err = NewMarshaledMsg(msg, consumer.Subscription())
			if err != nil {
				packMsg, err = UnmarshalMsg(msg, ms.unmarshal)
				if err != nil {
					mlog.Warn(ms.ctx, "Failed to getTsMsgFromConsumerMsg", mlog.Err(err))
					continue
				}
			}

			ms.chanMsgBufMutex.Lock()
			ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], packMsg)
			ms.chanMsgBufMutex.Unlock()

			if packMsg.GetType() == commonpb.MsgType_TimeTick {
				ms.chanTtMsgTimeMutex.Lock()
				ms.chanTtMsgTime[consumer] = packMsg.GetTimestamp()
				ms.chanTtMsgTimeMutex.Unlock()
				return
			}
		}
	}
}

// return true only when all channels reach same timetick
func (ms *MqTtMsgStream) allChanReachSameTtMsg(chanTtMsgSync map[mqwrapper.Consumer]bool) (Timestamp, bool) {
	tsMap := make(map[Timestamp]int)
	var maxTime Timestamp
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
		chanTtMsgSync[consumer] = ms.chanTtMsgTime[consumer] == maxTime
		ms.chanTtMsgTimeMutex.RUnlock()
	}

	return 0, false
}

// Seek to the specified position
func (ms *MqTtMsgStream) Seek(ctx context.Context, msgPositions []*MsgPosition, includeCurrentMsg bool) error {
	var consumer mqwrapper.Consumer
	var mp *MsgPosition
	var err error

	fn := func() (bool, error) {
		var ok bool
		consumer, ok = ms.consumers[mp.ChannelName]
		if !ok {
			return false, merr.WrapErrMqInternalMsg("please subcribe the channel, channel name =%s", mp.ChannelName)
		}

		if consumer == nil {
			return false, merr.WrapErrMqInternalMsg("consumer is nil")
		}

		seekMsgID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			if paramtable.Get().MQCfg.IgnoreBadPosition.GetAsBool() {
				// try to use latest message ID first
				seekMsgID, err = consumer.GetLatestMsgID()
				if err != nil {
					mlog.Warn(ctx, "Ignoring bad message id", mlog.Err(err))
					return false, nil
				}
			} else {
				return false, err
			}
		}

		mlog.Info(ctx, "MsgStream begin to seek start msg: ", mlog.String("channel", mp.ChannelName), mlog.Any("MessageID", mp.MsgID))
		err = consumer.Seek(seekMsgID, true)
		if err != nil {
			mlog.Warn(ctx, "Failed to seek", mlog.String("channel", mp.ChannelName), mlog.Err(err))
			// stop retry if consumer topic not exist
			if errors.Is(err, merr.ErrMqTopicNotFound) {
				return false, err
			}
			return true, err
		}
		mlog.Info(ctx, "MsgStream seek finished", mlog.String("channel", mp.ChannelName))

		return false, nil
	}

	ms.consumerLock.Lock()
	defer ms.consumerLock.Unlock()

	loopTick := time.NewTicker(5 * time.Second)
	defer loopTick.Stop()

	for idx := range msgPositions {
		mp = msgPositions[idx]
		if len(mp.MsgID) == 0 {
			return merr.WrapErrParameterInvalidMsg("when msgID's length equal to 0, please use AsConsumer interface")
		}
		err = retry.Handle(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		// err = retry.Do(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			return merr.WrapErrMqInternal(err, "failed to seek")
		}
		ms.addConsumer(consumer, mp.ChannelName)
		ms.chanMsgPos[consumer] = (proto.Clone(mp)).(*MsgPosition)

		// skip all data before current tt
		runLoop := true
		loopMsgCnt := 0
		loopStarTime := time.Now()
		for runLoop {
			select {
			case <-ms.ctx.Done():
				return ms.ctx.Err()
			case <-ctx.Done():
				return ctx.Err()
			case <-loopTick.C:
				mlog.Info(ctx, "seek loop tick", mlog.Int("loopMsgCnt", loopMsgCnt), mlog.String("channel", mp.ChannelName))
			case msg, ok := <-consumer.Chan():
				if !ok {
					return merr.WrapErrServiceUnavailable("consumer closed")
				}
				loopMsgCnt++
				consumer.Ack(msg)

				var err error
				var packMsg ConsumeMsg

				packMsg, err = NewMarshaledMsg(msg, consumer.Subscription())
				if err != nil {
					packMsg, err = UnmarshalMsg(msg, ms.unmarshal)
					if err != nil {
						mlog.Warn(ctx, "Failed to getTsMsgFromConsumerMsg", mlog.Err(err))
						continue
					}
				}

				if packMsg.GetType() == commonpb.MsgType_TimeTick && packMsg.GetTimestamp() >= mp.Timestamp {
					runLoop = false
					if time.Since(loopStarTime) > 30*time.Second {
						mlog.Info(ctx, "seek loop finished long time",
							mlog.Int("loopMsgCnt", loopMsgCnt),
							mlog.String("channel", mp.ChannelName),
							mlog.Duration("cost", time.Since(loopStarTime)))
					}
				} else if packMsg.GetTimestamp() > mp.Timestamp {
					ctx, _ := ExtractCtx(packMsg, msg.Properties())
					packMsg.SetTraceCtx(ctx)

					packMsg.SetPosition(&MsgPosition{
						ChannelName: filepath.Base(msg.Topic()),
						MsgID:       msg.ID().Serialize(),
					})
					ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], packMsg)
				} else {
					mlog.Info(ctx, "skip msg",
						// mlog.Int64("source", tsMsg.SourceID()), // TODO SOURCE ID ?
						mlog.String("type", packMsg.GetType().String()),
						mlog.Int("size", packMsg.GetSize()),
						mlog.Uint64("msgTs", packMsg.GetTimestamp()),
						mlog.Uint64("posTs", mp.GetTimestamp()),
					)
				}
			}
		}
	}
	return nil
}

func (ms *MqTtMsgStream) Chan() <-chan *ConsumeMsgPack {
	ms.onceChan.Do(func() {
		if ms.consumers != nil {
			go ms.bufMsgPackToChannel()
		}
	})

	return ms.receiveBuf
}
