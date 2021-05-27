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
	"log"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

type MemMsgStream struct {
	ctx          context.Context
	streamCancel func()

	repackFunc RepackFunc

	consumers []*MemConsumer
	producers []string

	receiveBuf chan *MsgPack

	wait sync.WaitGroup
}

func NewMemMsgStream(ctx context.Context, receiveBufSize int64) (*MemMsgStream, error) {
	streamCtx, streamCancel := context.WithCancel(ctx)
	receiveBuf := make(chan *MsgPack, receiveBufSize)
	channels := make([]string, 0)
	consumers := make([]*MemConsumer, 0)

	stream := &MemMsgStream{
		ctx:          streamCtx,
		streamCancel: streamCancel,
		receiveBuf:   receiveBuf,
		consumers:    consumers,
		producers:    channels,
	}

	return stream, nil
}

func (mms *MemMsgStream) Start() {
}

func (mms *MemMsgStream) Close() {
	for _, consumer := range mms.consumers {
		Mmq.DestroyConsumerGroup(consumer.GroupName, consumer.ChannelName)
	}

	mms.streamCancel()
	mms.wait.Wait()
}

func (mms *MemMsgStream) SetRepackFunc(repackFunc RepackFunc) {
	mms.repackFunc = repackFunc
}

func (mms *MemMsgStream) GetProduceChannels() []string {
	return mms.producers
}

func (mms *MemMsgStream) AsProducer(channels []string) {
	for _, channel := range channels {
		err := Mmq.CreateChannel(channel)
		if err == nil {
			mms.producers = append(mms.producers, channel)
		} else {
			errMsg := "Failed to create producer " + channel + ", error = " + err.Error()
			panic(errMsg)
		}
	}
}

func (mms *MemMsgStream) ComputeProduceChannelIndexes(tsMsgs []TsMsg) [][]int32 {
	if len(tsMsgs) <= 0 {
		return nil
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	channelNum := uint32(len(mms.producers))
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

func (mms *MemMsgStream) AsConsumer(channels []string, groupName string) {
	for _, channelName := range channels {
		consumer, err := Mmq.CreateConsumerGroup(groupName, channelName)
		if err == nil {
			mms.consumers = append(mms.consumers, consumer)

			mms.wait.Add(1)
			go mms.receiveMsg(*consumer)
		}
	}
}

func (mms *MemMsgStream) Produce(pack *MsgPack) error {
	tsMsgs := pack.Msgs
	if len(tsMsgs) <= 0 {
		log.Printf("Warning: Receive empty msgPack")
		return nil
	}
	if len(mms.producers) <= 0 {
		return errors.New("nil producer in msg stream")
	}
	reBucketValues := mms.ComputeProduceChannelIndexes(pack.Msgs)
	var result map[int32]*MsgPack
	var err error
	if mms.repackFunc != nil {
		result, err = mms.repackFunc(tsMsgs, reBucketValues)
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
		err := Mmq.Produce(mms.producers[k], v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mms *MemMsgStream) Broadcast(msgPack *MsgPack) error {
	for _, channelName := range mms.producers {
		err := Mmq.Produce(channelName, msgPack)
		if err != nil {
			return err
		}
	}

	return nil
}

func (mms *MemMsgStream) Consume() *MsgPack {
	for {
		select {
		case cm, ok := <-mms.receiveBuf:
			if !ok {
				log.Println("buf chan closed")
				return nil
			}
			return cm
		case <-mms.ctx.Done():
			log.Printf("context closed")
			return nil
		}
	}
}

/**
receiveMsg func is used to solve search timeout problem
which is caused by selectcase
*/
func (mms *MemMsgStream) receiveMsg(consumer MemConsumer) {
	defer mms.wait.Done()
	for {
		select {
		case <-mms.ctx.Done():
			return
		case msg := <-consumer.MsgChan:
			if msg == nil {
				return
			}

			mms.receiveBuf <- msg
		}
	}
}

func (mms *MemMsgStream) Chan() <-chan *MsgPack {
	return mms.receiveBuf
}

func (mms *MemMsgStream) Seek(offset *MsgPosition) error {
	return errors.New("MemMsgStream seek not implemented")
}
