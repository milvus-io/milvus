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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	uatomic "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	_             MsgStream = (*mqMsgStream)(nil)
	streamCounter uatomic.Int64
)

type mqMsgStream struct {
	ctx              context.Context
	client           mqwrapper.Client
	producers        map[string]mqwrapper.Producer
	producerChannels []string
	consumers        map[string]mqwrapper.Consumer
	consumerChannels []string

	repackFunc         RepackFunc
	unmarshal          UnmarshalDispatcher
	receiveBuf         chan *MsgPack
	closeRWMutex       *sync.RWMutex
	streamCancel       func()
	bufSize            int64
	producerLock       *sync.RWMutex
	consumerLock       *sync.Mutex
	closed             int32
	onceChan           sync.Once
	ttMsgEnable        atomic.Value
	forceEnableProduce atomic.Value
	configEvent        config.EventHandler

	replicateID string
	checkFunc   CheckReplicateMsgFunc
}

// NewMqMsgStream is used to generate a new mqMsgStream object
func NewMqMsgStream(ctx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client mqwrapper.Client,
	unmarshal UnmarshalDispatcher,
) (*mqMsgStream, error) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	producers := make(map[string]mqwrapper.Producer)
	consumers := make(map[string]mqwrapper.Consumer)
	producerChannels := make([]string, 0)
	consumerChannels := make([]string, 0)
	receiveBuf := make(chan *MsgPack, receiveBufSize)

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
	ctxLog := log.Ctx(ctx)
	stream.forceEnableProduce.Store(false)
	stream.ttMsgEnable.Store(paramtable.Get().CommonCfg.TTMsgEnabled.GetAsBool())
	stream.configEvent = config.NewHandler("enable send tt msg "+fmt.Sprint(streamCounter.Inc()), func(event *config.Event) {
		value, err := strconv.ParseBool(event.Value)
		if err != nil {
			ctxLog.Warn("Failed to parse bool value", zap.String("v", event.Value), zap.Error(err))
			return
		}
		stream.ttMsgEnable.Store(value)
		ctxLog.Info("Msg Stream state updated", zap.Bool("can_produce", stream.isEnabledProduce()))
	})
	paramtable.Get().Watch(paramtable.Get().CommonCfg.TTMsgEnabled.Key, stream.configEvent)
	ctxLog.Info("Msg Stream state", zap.Bool("can_produce", stream.isEnabledProduce()))

	return stream, nil
}

// AsProducer create producer to send message to channels
func (ms *mqMsgStream) AsProducer(ctx context.Context, channels []string) {
	for _, channel := range channels {
		if len(channel) == 0 {
			log.Ctx(ms.ctx).Error("MsgStream asProducer's channel is an empty string")
			break
		}

		fn := func() error {
			pp, err := ms.client.CreateProducer(ctx, common.ProducerOptions{Topic: channel, EnableCompression: true})
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
		err := retry.Do(context.TODO(), fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (ms *mqMsgStream) GetLatestMsgID(channel string) (MessageID, error) {
	lastMsg, err := ms.consumers[channel].GetLatestMsgID()
	if err != nil {
		errMsg := "Failed to get latest MsgID from channel: " + channel + ", error = " + err.Error()
		return nil, errors.New(errMsg)
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
				return errors.New("Consumer is nil")
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
		log.Ctx(ms.ctx).Info("Successfully create consumer", zap.String("channel", channel), zap.String("subname", subName))
	}
	return nil
}

func (ms *mqMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	ms.repackFunc = repackFunc
}

func (ms *mqMsgStream) Close() {
	log.Ctx(ms.ctx).Info("start to close mq msg stream",
		zap.Int("producer num", len(ms.producers)),
		zap.Int("consumer num", len(ms.consumers)))
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
	paramtable.Get().Unwatch(paramtable.Get().CommonCfg.TTMsgEnabled.Key, ms.configEvent)
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

func (ms *mqMsgStream) ForceEnableProduce(can bool) {
	ms.forceEnableProduce.Store(can)
}

func (ms *mqMsgStream) isEnabledProduce() bool {
	return ms.forceEnableProduce.Load().(bool) || ms.ttMsgEnable.Load().(bool)
}

func (ms *mqMsgStream) isSkipSystemTT() bool {
	return ms.replicateID != ""
}

// checkReplicateID check the replicate id of the message, return values: isMatch, isReplicate
func (ms *mqMsgStream) checkReplicateID(msg TsMsg) (bool, bool) {
	if !ms.isSkipSystemTT() {
		return true, false
	}
	msgBase, ok := msg.(interface{ GetBase() *commonpb.MsgBase })
	if !ok {
		log.Warn("fail to get msg base, please check it", zap.Any("type", msg.Type()))
		return false, false
	}
	return msgBase.GetBase().GetReplicateInfo().GetReplicateID() == ms.replicateID, true
}

func (ms *mqMsgStream) Produce(ctx context.Context, msgPack *MsgPack) error {
	if !ms.isEnabledProduce() {
		log.Ctx(ms.ctx).Warn("can't produce the msg in the backup instance", zap.Stack("stack"))
		return merr.ErrDenyProduceMsg
	}
	if msgPack == nil || len(msgPack.Msgs) <= 0 {
		log.Ctx(ms.ctx).Debug("Warning: Receive empty msgPack")
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
				return errors.New("producer not found for channel: " + channel)
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

				msg := &common.ProducerMessage{Payload: m, Properties: map[string]string{
					common.MsgTypeKey: v.Msgs[i].Type().String(),
				}}
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
		return ids, errors.New("empty msgs")
	}
	// Only allow to create collection msg in backup instance
	// However, there may be a problem of ts disorder here, but because the start position of the collection only uses offsets, not time, there is no problem for the time being
	isCreateCollectionMsg := len(msgPack.Msgs) == 1 && msgPack.Msgs[0].Type() == commonpb.MsgType_CreateCollection

	if !ms.isEnabledProduce() && !isCreateCollectionMsg {
		log.Ctx(ms.ctx).Warn("can't broadcast the msg in the backup instance", zap.Stack("stack"))
		return ids, merr.ErrDenyProduceMsg
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

		msg := &common.ProducerMessage{Payload: m, Properties: map[string]string{}}
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

func (ms *mqMsgStream) getTsMsgFromConsumerMsg(msg common.Message) (TsMsg, error) {
	return GetTsMsgFromConsumerMsg(ms.unmarshal, msg)
}

// GetTsMsgFromConsumerMsg get TsMsg from consumer message
func GetTsMsgFromConsumerMsg(unmarshalDispatcher UnmarshalDispatcher, msg common.Message) (TsMsg, error) {
	msgType, err := common.GetMsgType(msg)
	if err != nil {
		return nil, err
	}
	tsMsg, err := unmarshalDispatcher.Unmarshal(msg.Payload(), msgType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal tsMsg, err %s", err.Error())
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
				log.Ctx(ms.ctx).Warn("MqMsgStream get msg whose payload is nil")
				continue
			}
			// not need to check the preCreatedTopic is empty, related issue: https://github.com/milvus-io/milvus/issues/27295
			// if the message not belong to the topic, will skip it
			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				log.Ctx(ms.ctx).Warn("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}
			pos := tsMsg.Position()
			tsMsg.SetPosition(&MsgPosition{
				ChannelName: pos.ChannelName,
				MsgID:       pos.MsgID,
				MsgGroup:    consumer.Subscription(),
				Timestamp:   tsMsg.BeginTs(),
			})

			ctx, _ := ExtractCtx(tsMsg, msg.Properties())
			tsMsg.SetTraceCtx(ctx)

			msgPack := MsgPack{
				Msgs:           []TsMsg{tsMsg},
				StartPositions: []*msgpb.MsgPosition{tsMsg.Position()},
				EndPositions:   []*msgpb.MsgPosition{tsMsg.Position()},
				BeginTs:        tsMsg.BeginTs(),
				EndTs:          tsMsg.EndTs(),
			}
			select {
			case ms.receiveBuf <- &msgPack:
			case <-ms.ctx.Done():
				return
			}
		}
	}
}

func (ms *mqMsgStream) Chan() <-chan *MsgPack {
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
			return fmt.Errorf("channel %s not subscribed", mp.ChannelName)
		}
		messageID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			if paramtable.Get().MQCfg.IgnoreBadPosition.GetAsBool() {
				// try to use latest message ID first
				messageID, err = consumer.GetLatestMsgID()
				if err != nil {
					log.Ctx(ctx).Warn("Ignoring bad message id", zap.Error(err))
					continue
				}
			} else {
				return err
			}
		}

		log.Ctx(ctx).Info("MsgStream seek begin", zap.String("channel", mp.ChannelName), zap.Any("MessageID", mp.MsgID), zap.Bool("includeCurrentMsg", includeCurrentMsg))
		err = consumer.Seek(messageID, includeCurrentMsg)
		if err != nil {
			log.Ctx(ctx).Warn("Failed to seek", zap.String("channel", mp.ChannelName), zap.Error(err))
			return err
		}
		log.Ctx(ctx).Info("MsgStream seek finished", zap.String("channel", mp.ChannelName))
	}
	return nil
}

var _ MsgStream = (*MqTtMsgStream)(nil)

// MqTtMsgStream is a msgstream that contains timeticks
type MqTtMsgStream struct {
	*mqMsgStream
	chanMsgBuf         map[mqwrapper.Consumer][]TsMsg
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
	chanMsgBuf := make(map[mqwrapper.Consumer][]TsMsg)
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
	ms.chanMsgBuf[consumer] = make([]TsMsg, 0)
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
				return errors.New("Consumer is nil")
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

			panic(fmt.Sprintf("%s, errors = %s", errMsg, err.Error()))
		}
	}

	return nil
}

// Close will stop goroutine and free internal producers and consumers
func (ms *MqTtMsgStream) Close() {
	close(ms.syncConsumer)
	ms.mqMsgStream.Close()
}

func isDMLMsg(msg TsMsg) bool {
	return msg.Type() == commonpb.MsgType_Insert || msg.Type() == commonpb.MsgType_Delete
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
	if !paramtable.Get().ServiceParam.MQCfg.EnablePursuitMode.GetAsBool() {
		return false
	}

	// buffer full
	if size > paramtable.Get().ServiceParam.MQCfg.PursuitBufferSize.GetAsUint64() {
		return false
	}

	if time.Since(startTime) > paramtable.Get().ServiceParam.MQCfg.PursuitBufferTime.GetAsDuration(time.Second) {
		return false
	}

	endTime, _ := tsoutil.ParseTS(endTs)
	return time.Since(endTime) > paramtable.Get().ServiceParam.MQCfg.PursuitLag.GetAsDuration(time.Second)
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
		log.Ctx(ms.ctx).Warn("consumer closed!")
		return
	}

	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
			timeTickBuf := make([]TsMsg, 0)
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
					tempBuffer := make([]TsMsg, 0)
					var timeTickMsg TsMsg
					for _, v := range msgs {
						if v.Type() == commonpb.MsgType_TimeTick {
							timeTickMsg = v
							continue
						}
						if v.EndTs() <= currTs ||
							GetReplicateID(v) != "" {
							size += uint64(v.Size())
							timeTickBuf = append(timeTickBuf, v)
						} else {
							tempBuffer = append(tempBuffer, v)
						}
						// when drop collection, force to exit the buffer loop
						if v.Type() == commonpb.MsgType_DropCollection || v.Type() == commonpb.MsgType_Replicate {
							containsEndBufferMsg = true
						}
					}
					ms.chanMsgBuf[consumer] = tempBuffer

					//	startMsgPosition = append(startMsgPosition, proto.Clone(ms.chanMsgPos[consumer]).(*msgpb.MsgPosition))
					var newPos *msgpb.MsgPosition
					if len(tempBuffer) > 0 {
						// if tempBuffer is not empty, use tempBuffer[0] to seek
						newPos = &msgpb.MsgPosition{
							ChannelName: tempBuffer[0].Position().ChannelName,
							MsgID:       tempBuffer[0].Position().MsgID,
							Timestamp:   currTs,
							MsgGroup:    consumer.Subscription(),
						}
						endPositions[channelName] = newPos
					} else if timeTickMsg != nil {
						// if tempBuffer is empty, use timeTickMsg to seek
						newPos = &msgpb.MsgPosition{
							ChannelName: timeTickMsg.Position().ChannelName,
							MsgID:       timeTickMsg.Position().MsgID,
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

			idset := make(typeutil.UniqueSet)
			uniqueMsgs := make([]TsMsg, 0, len(timeTickBuf))
			for _, msg := range timeTickBuf {
				if isDMLMsg(msg) && idset.Contain(msg.ID()) {
					log.Ctx(ms.ctx).Warn("mqTtMsgStream, found duplicated msg", zap.Int64("msgID", msg.ID()))
					continue
				}
				idset.Insert(msg.ID())
				uniqueMsgs = append(uniqueMsgs, msg)
			}

			// skip endTs = 0 (no run for ctx error)
			if endTs > 0 {
				msgPack := MsgPack{
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
	log := log.Ctx(ms.ctx)
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

			if msg.Payload() == nil {
				log.Warn("MqTtMsgStream get msg whose payload is nil")
				continue
			}
			// not need to check the preCreatedTopic is empty, related issue: https://github.com/milvus-io/milvus/issues/27295
			// if the message not belong to the topic, will skip it
			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				log.Warn("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}

			ms.chanMsgBufMutex.Lock()
			ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
			ms.chanMsgBufMutex.Unlock()

			if tsMsg.Type() == commonpb.MsgType_TimeTick {
				ms.chanTtMsgTimeMutex.Lock()
				ms.chanTtMsgTime[consumer] = tsMsg.(*TimeTickMsg).Base.Timestamp
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
	log := log.Ctx(ctx)
	fn := func() (bool, error) {
		var ok bool
		consumer, ok = ms.consumers[mp.ChannelName]
		if !ok {
			return false, fmt.Errorf("please subcribe the channel, channel name =%s", mp.ChannelName)
		}

		if consumer == nil {
			return false, fmt.Errorf("consumer is nil")
		}

		seekMsgID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			if paramtable.Get().MQCfg.IgnoreBadPosition.GetAsBool() {
				// try to use latest message ID first
				seekMsgID, err = consumer.GetLatestMsgID()
				if err != nil {
					log.Warn("Ignoring bad message id", zap.Error(err))
					return false, nil
				}
			} else {
				return false, err
			}
		}

		log.Info("MsgStream begin to seek start msg: ", zap.String("channel", mp.ChannelName), zap.Any("MessageID", mp.MsgID))
		err = consumer.Seek(seekMsgID, true)
		if err != nil {
			log.Warn("Failed to seek", zap.String("channel", mp.ChannelName), zap.Error(err))
			// stop retry if consumer topic not exist
			if errors.Is(err, merr.ErrMqTopicNotFound) {
				return false, err
			}
			return true, err
		}
		log.Info("MsgStream seek finished", zap.String("channel", mp.ChannelName))

		return false, nil
	}

	ms.consumerLock.Lock()
	defer ms.consumerLock.Unlock()

	loopTick := time.NewTicker(5 * time.Second)
	defer loopTick.Stop()

	for idx := range msgPositions {
		mp = msgPositions[idx]
		if len(mp.MsgID) == 0 {
			return fmt.Errorf("when msgID's length equal to 0, please use AsConsumer interface")
		}
		err = retry.Handle(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		// err = retry.Do(ctx, fn, retry.Attempts(20), retry.Sleep(time.Millisecond*200), retry.MaxSleepTime(5*time.Second))
		if err != nil {
			return fmt.Errorf("failed to seek, error %s", err.Error())
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
				log.Info("seek loop tick", zap.Int("loopMsgCnt", loopMsgCnt), zap.String("channel", mp.ChannelName))
			case msg, ok := <-consumer.Chan():
				if !ok {
					return fmt.Errorf("consumer closed")
				}
				loopMsgCnt++
				consumer.Ack(msg)

				headerMsg := commonpb.MsgHeader{}
				err := proto.Unmarshal(msg.Payload(), &headerMsg)
				if err != nil {
					return fmt.Errorf("failed to unmarshal message header, err %s", err.Error())
				}
				tsMsg, err := ms.unmarshal.Unmarshal(msg.Payload(), headerMsg.Base.MsgType)
				if err != nil {
					return fmt.Errorf("failed to unmarshal tsMsg, err %s", err.Error())
				}
				// skip the replicate msg because it must have been consumed
				if GetReplicateID(tsMsg) != "" {
					continue
				}
				if tsMsg.Type() == commonpb.MsgType_TimeTick && tsMsg.BeginTs() >= mp.Timestamp {
					runLoop = false
					if time.Since(loopStarTime) > 30*time.Second {
						log.Info("seek loop finished long time",
							zap.Int("loopMsgCnt", loopMsgCnt),
							zap.String("channel", mp.ChannelName),
							zap.Duration("cost", time.Since(loopStarTime)))
					}
				} else if tsMsg.BeginTs() > mp.Timestamp {
					ctx, _ := ExtractCtx(tsMsg, msg.Properties())
					tsMsg.SetTraceCtx(ctx)

					tsMsg.SetPosition(&MsgPosition{
						ChannelName: filepath.Base(msg.Topic()),
						MsgID:       msg.ID().Serialize(),
					})
					ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
				} else {
					log.Info("skip msg",
						zap.Int64("source", tsMsg.SourceID()),
						zap.String("type", tsMsg.Type().String()),
						zap.Int("size", tsMsg.Size()),
						zap.Uint64("msgTs", tsMsg.BeginTs()),
						zap.Uint64("posTs", mp.GetTimestamp()),
					)
				}
			}
		}
	}
	return nil
}

func (ms *MqTtMsgStream) Chan() <-chan *MsgPack {
	ms.onceChan.Do(func() {
		if ms.consumers != nil {
			go ms.bufMsgPackToChannel()
		}
	})

	return ms.receiveBuf
}
