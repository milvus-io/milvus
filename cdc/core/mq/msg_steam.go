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

package mq

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/util"
	"go.uber.org/zap"
)

var ErrTopicNotExist = errors.New("topic not exist")

var _ api.MsgStream = (*MqMsgStream)(nil)

type MqMsgStream struct {
	util.CDCMark

	ctx              context.Context
	client           api.Client
	consumers        map[string]api.Consumer
	consumerChannels []string

	repackFunc   api.RepackFunc
	unmarshal    api.UnmarshalDispatcher
	receiveBuf   chan *api.MsgPack
	closeRWMutex *sync.RWMutex
	streamCancel func()
	bufSize      int64
	producerLock *sync.Mutex
	consumerLock *sync.Mutex
	closed       int32
	onceChan     sync.Once
}

// NewMqMsgStream is used to generate a new mqMsgStream object
func NewMqMsgStream(ctx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client api.Client,
	unmarshal api.UnmarshalDispatcher) (*MqMsgStream, error) {

	streamCtx, streamCancel := context.WithCancel(ctx)
	consumers := make(map[string]api.Consumer)
	consumerChannels := make([]string, 0)
	receiveBuf := make(chan *api.MsgPack, receiveBufSize)

	stream := &MqMsgStream{
		ctx:              streamCtx,
		client:           client,
		consumers:        consumers,
		consumerChannels: consumerChannels,

		unmarshal:    unmarshal,
		bufSize:      bufSize,
		receiveBuf:   receiveBuf,
		streamCancel: streamCancel,
		producerLock: &sync.Mutex{},
		consumerLock: &sync.Mutex{},
		closeRWMutex: &sync.RWMutex{},
		closed:       0,
	}

	return stream, nil
}

func (ms *MqMsgStream) GetLatestMsgID(channel string) (api.MessageID, error) {
	lastMsg, err := ms.consumers[channel].GetLatestMsgID()
	if err != nil {
		errMsg := "Failed to get latest MsgID from channel: " + channel + ", error = " + err.Error()
		return nil, errors.New(errMsg)
	}
	return lastMsg, nil
}

// AsConsumerWithPosition Create consumer to receive message from channels, with initial position
// if initial position is set to latest, last message in the channel is exclusive
func (ms *MqMsgStream) AsConsumer(channels []string, subName string, position api.SubscriptionInitialPosition) {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			pc, err := ms.client.Subscribe(api.ConsumerOptions{
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
		// TODO if know the former subscribe is invalid, should we use pulsarctl to accelerate recovery speed
		err := util.Do(context.TODO(), fn, util.Attempts(50), util.Sleep(time.Millisecond*200), util.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
		util.Log.Info("Successfully create consumer", zap.String("channel", channel), zap.String("subname", subName))
	}
}

func (ms *MqMsgStream) Close() {
	util.Log.Info("start to close mq msg stream",
		zap.Int("consumer num", len(ms.consumers)))
	ms.streamCancel()
	ms.closeRWMutex.Lock()
	defer ms.closeRWMutex.Unlock()
	if !atomic.CompareAndSwapInt32(&ms.closed, 0, 1) {
		return
	}
	for _, consumer := range ms.consumers {
		if consumer != nil {
			consumer.Close()
		}
	}

	ms.client.Close()

	close(ms.receiveBuf)

}

func (ms *MqMsgStream) getTsMsgFromConsumerMsg(msg api.Message) (api.TsMsg, error) {
	header := commonpb.MsgHeader{}
	if msg.Payload() == nil {
		return nil, fmt.Errorf("failed to unmarshal message header, payload is empty")
	}
	err := proto.Unmarshal(msg.Payload(), &header)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message header, err %s", err.Error())
	}
	if header.Base == nil {
		return nil, fmt.Errorf("failed to unmarshal message, header is uncomplete")
	}
	tsMsg, err := ms.unmarshal.Unmarshal(msg.Payload(), header.Base.MsgType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal tsMsg, err %s", err.Error())
	}

	// set msg info to tsMsg
	tsMsg.SetPosition(&api.MsgPosition{
		ChannelName: filepath.Base(msg.Topic()),
		MsgID:       msg.ID().Serialize(),
	})

	return tsMsg, nil
}

func (ms *MqMsgStream) receiveMsg(consumer api.Consumer) {
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
				util.Log.Warn("MqMsgStream get msg whose payload is nil")
				continue
			}
			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				util.Log.Error("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}
			pos := tsMsg.Position()
			tsMsg.SetPosition(&api.MsgPosition{
				ChannelName: pos.ChannelName,
				MsgID:       pos.MsgID,
				MsgGroup:    consumer.Subscription(),
				Timestamp:   tsMsg.BeginTs(),
			})

			msgPack := api.MsgPack{
				Msgs:           []api.TsMsg{tsMsg},
				StartPositions: []*pb.MsgPosition{tsMsg.Position()},
				EndPositions:   []*pb.MsgPosition{tsMsg.Position()},
			}
			select {
			case ms.receiveBuf <- &msgPack:
			case <-ms.ctx.Done():
				return
			}
		}
	}
}

func (ms *MqMsgStream) Chan() <-chan *api.MsgPack {
	ms.onceChan.Do(func() {
		for _, c := range ms.consumers {
			go ms.receiveMsg(c)
		}
	})

	return ms.receiveBuf
}

// Seek reset the subscription associated with this consumer to a specific position, the seek position is exclusive
// User has to ensure mq_msgstream is not closed before seek, and the seek position is already written.
func (ms *MqMsgStream) Seek(msgPositions []*pb.MsgPosition) error {
	for _, mp := range msgPositions {
		consumer, ok := ms.consumers[mp.ChannelName]
		if !ok {
			return fmt.Errorf("channel %s not subscribed", mp.ChannelName)
		}
		messageID, err := ms.client.BytesToMsgID(mp.MsgID)
		if err != nil {
			return err
		}

		util.Log.Info("MsgStream seek begin", zap.String("channel", mp.ChannelName), zap.Any("MessageID", mp.MsgID))
		err = consumer.Seek(messageID, false)
		if err != nil {
			util.Log.Warn("Failed to seek", zap.String("channel", mp.ChannelName), zap.Error(err))
			return err
		}
		util.Log.Info("MsgStream seek finished", zap.String("channel", mp.ChannelName))
	}
	return nil
}

var _ api.MsgStream = (*MqTtMsgStream)(nil)

// MqTtMsgStream is a msgstream that contains timeticks
type MqTtMsgStream struct {
	MqMsgStream
	chanMsgBuf         map[api.Consumer][]api.TsMsg
	chanMsgPos         map[api.Consumer]*pb.MsgPosition
	chanStopChan       map[api.Consumer]chan bool
	chanTtMsgTime      map[api.Consumer]api.Timestamp
	chanMsgBufMutex    *sync.Mutex
	chanTtMsgTimeMutex *sync.RWMutex
	chanWaitGroup      *sync.WaitGroup
	lastTimeStamp      api.Timestamp
	syncConsumer       chan int
}

// NewMqTtMsgStream is used to generate a new MqTtMsgStream object
func NewMqTtMsgStream(ctx context.Context,
	receiveBufSize int64,
	bufSize int64,
	client api.Client,
	unmarshal api.UnmarshalDispatcher) (*MqTtMsgStream, error) {
	msgStream, err := NewMqMsgStream(ctx, receiveBufSize, bufSize, client, unmarshal)
	if err != nil {
		return nil, err
	}
	chanMsgBuf := make(map[api.Consumer][]api.TsMsg)
	chanMsgPos := make(map[api.Consumer]*pb.MsgPosition)
	chanStopChan := make(map[api.Consumer]chan bool)
	chanTtMsgTime := make(map[api.Consumer]api.Timestamp)
	syncConsumer := make(chan int, 1)

	return &MqTtMsgStream{
		MqMsgStream:        *msgStream,
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

func (ms *MqTtMsgStream) addConsumer(consumer api.Consumer, channel string) {
	if len(ms.consumers) == 0 {
		ms.syncConsumer <- 1
	}
	ms.consumers[channel] = consumer
	ms.consumerChannels = append(ms.consumerChannels, channel)
	ms.chanMsgBuf[consumer] = make([]api.TsMsg, 0)
	ms.chanMsgPos[consumer] = &pb.MsgPosition{
		ChannelName: channel,
		MsgID:       make([]byte, 0),
		Timestamp:   ms.lastTimeStamp,
	}
	ms.chanStopChan[consumer] = make(chan bool)
	ms.chanTtMsgTime[consumer] = 0
}

// AsConsumerWithPosition subscribes channels as consumer for a MsgStream and seeks to a certain position.
func (ms *MqTtMsgStream) AsConsumer(channels []string, subName string, position api.SubscriptionInitialPosition) {
	for _, channel := range channels {
		if _, ok := ms.consumers[channel]; ok {
			continue
		}
		fn := func() error {
			pc, err := ms.client.Subscribe(api.ConsumerOptions{
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
		err := util.Do(context.TODO(), fn, util.Attempts(20), util.Sleep(time.Millisecond*200), util.MaxSleepTime(5*time.Second))
		if err != nil {
			errMsg := "Failed to create consumer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

// Close will stop goroutine and free internal producers and consumers
func (ms *MqTtMsgStream) Close() {
	close(ms.syncConsumer)
	ms.MqMsgStream.Close()
}

func isDMLMsg(msg api.TsMsg) bool {
	return msg.Type() == commonpb.MsgType_Insert || msg.Type() == commonpb.MsgType_Delete
}

func (ms *MqTtMsgStream) bufMsgPackToChannel() {
	ms.closeRWMutex.RLock()
	defer ms.closeRWMutex.RUnlock()
	if atomic.LoadInt32(&ms.closed) != 0 {
		return
	}
	chanTtMsgSync := make(map[api.Consumer]bool)

	// block here until addConsumer
	if _, ok := <-ms.syncConsumer; !ok {
		util.Log.Warn("consumer closed!")
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

			timeTickBuf := make([]api.TsMsg, 0)
			startMsgPosition := make([]*pb.MsgPosition, 0)
			endMsgPositions := make([]*pb.MsgPosition, 0)
			ms.chanMsgBufMutex.Lock()
			for consumer, msgs := range ms.chanMsgBuf {
				if len(msgs) == 0 {
					continue
				}
				tempBuffer := make([]api.TsMsg, 0)
				var timeTickMsg api.TsMsg
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

				startMsgPosition = append(startMsgPosition, proto.Clone(ms.chanMsgPos[consumer]).(*pb.MsgPosition))
				var newPos *pb.MsgPosition
				if len(tempBuffer) > 0 {
					// if tempBuffer is not empty, use tempBuffer[0] to seek
					newPos = &pb.MsgPosition{
						ChannelName: tempBuffer[0].Position().ChannelName,
						MsgID:       tempBuffer[0].Position().MsgID,
						Timestamp:   currTs,
						MsgGroup:    consumer.Subscription(),
					}
					endMsgPositions = append(endMsgPositions, newPos)
				} else if timeTickMsg != nil {
					// if tempBuffer is empty, use timeTickMsg to seek
					newPos = &pb.MsgPosition{
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

			idset := make(map[api.UniqueID]struct{})
			uniqueMsgs := make([]api.TsMsg, 0, len(timeTickBuf))
			for _, msg := range timeTickBuf {
				_, isContains := idset[msg.ID()]
				if isDMLMsg(msg) && isContains {
					util.Log.Warn("mqTtMsgStream, found duplicated msg", zap.Int64("msgID", msg.ID()))
					continue
				}
				idset[msg.ID()] = struct{}{}
				uniqueMsgs = append(uniqueMsgs, msg)
			}

			msgPack := api.MsgPack{
				BeginTs:        ms.lastTimeStamp,
				EndTs:          currTs,
				Msgs:           uniqueMsgs,
				StartPositions: startMsgPosition,
				EndPositions:   endMsgPositions,
			}

			//log.Debug("send msg pack", zap.Int("len", len(msgPack.Msgs)), zap.Uint64("currTs", currTs))
			select {
			case ms.receiveBuf <- &msgPack:
			case <-ms.ctx.Done():
				return
			}
			ms.lastTimeStamp = currTs
		}
	}
}

// Save all msgs into chanMsgBuf[] till receive one ttMsg
func (ms *MqTtMsgStream) consumeToTtMsg(consumer api.Consumer) {
	defer ms.chanWaitGroup.Done()
	for {
		select {
		case <-ms.ctx.Done():
			return
		case <-ms.chanStopChan[consumer]:
			return
		case msg, ok := <-consumer.Chan():
			if !ok {
				util.Log.Debug("consumer closed!")
				return
			}
			consumer.Ack(msg)

			if msg.Payload() == nil {
				util.Log.Warn("MqTtMsgStream get msg whose payload is nil")
				continue
			}
			tsMsg, err := ms.getTsMsgFromConsumerMsg(msg)
			if err != nil {
				util.Log.Error("Failed to getTsMsgFromConsumerMsg", zap.Error(err))
				continue
			}

			ms.chanMsgBufMutex.Lock()
			ms.chanMsgBuf[consumer] = append(ms.chanMsgBuf[consumer], tsMsg)
			ms.chanMsgBufMutex.Unlock()

			if tsMsg.Type() == commonpb.MsgType_TimeTick {
				ms.chanTtMsgTimeMutex.Lock()
				ms.chanTtMsgTime[consumer] = tsMsg.(*api.TimeTickMsg).Base.Timestamp
				ms.chanTtMsgTimeMutex.Unlock()
				return
			}
		}
	}
}

// return true only when all channels reach same timetick
func (ms *MqTtMsgStream) allChanReachSameTtMsg(chanTtMsgSync map[api.Consumer]bool) (api.Timestamp, bool) {
	tsMap := make(map[api.Timestamp]int)
	var maxTime api.Timestamp
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
func (ms *MqTtMsgStream) Seek(msgPositions []*pb.MsgPosition) error {
	var consumer api.Consumer
	var mp *api.MsgPosition
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
			util.Log.Warn("fubang error", zap.Error(err))
			return err
		}
		util.Log.Info("MsgStream begin to seek start msg: ", zap.String("channel", mp.ChannelName), zap.Any("MessageID", mp.MsgID))
		err = consumer.Seek(seekMsgID, true)
		if err != nil {
			util.Log.Warn("Failed to seek", zap.String("channel", mp.ChannelName), zap.Error(err))
			// stop retry if consumer topic not exist
			if errors.Is(err, ErrTopicNotExist) {
				return util.Unrecoverable(err)
			}
			return err
		}
		util.Log.Info("MsgStream seek finished", zap.String("channel", mp.ChannelName))

		return nil
	}

	ms.consumerLock.Lock()
	defer ms.consumerLock.Unlock()

	for idx := range msgPositions {
		mp = msgPositions[idx]
		if len(mp.MsgID) == 0 {
			return fmt.Errorf("when msgID's length equal to 0, please use AsConsumer interface")
		}
		err = util.Do(context.TODO(), fn, util.Attempts(20), util.Sleep(time.Millisecond*200), util.MaxSleepTime(5*time.Second))
		if err != nil {
			return fmt.Errorf("failed to seek, error %s", err.Error())
		}
		ms.addConsumer(consumer, mp.ChannelName)
		ms.chanMsgPos[consumer] = (proto.Clone(mp)).(*api.MsgPosition)

		// skip all data before current tt
		runLoop := true
		for runLoop {
			select {
			case <-ms.ctx.Done():
				return ms.ctx.Err()
			case msg, ok := <-consumer.Chan():
				if !ok {
					return fmt.Errorf("consumer closed")
				}
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
				if tsMsg.Type() == commonpb.MsgType_TimeTick && tsMsg.BeginTs() >= mp.Timestamp {
					runLoop = false
					break
				} else if tsMsg.BeginTs() > mp.Timestamp {
					tsMsg.SetPosition(&api.MsgPosition{
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

func (ms *MqTtMsgStream) Chan() <-chan *api.MsgPack {
	ms.onceChan.Do(func() {
		if ms.consumers != nil {
			go ms.bufMsgPackToChannel()
		}
	})

	return ms.receiveBuf
}
