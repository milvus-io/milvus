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
	"errors"
	"sync"
)

var Mmq *MemMQ
var once sync.Once

type MemConsumer struct {
	GroupName   string
	ChannelName string
	MsgChan     chan *MsgPack
}

type ChannelMsg struct {
	ChannelName string
	Msg         *MsgPack
}

type MemMQ struct {
	consumers  map[string][]*MemConsumer
	consumerMu sync.Mutex

	// The msgBuffer is to handle this case: producer produce message before consumer is created
	// How it works:
	//    when producer try produce message in MemMQ, if there is no consumser for this channel,
	//    the message will be put into msgBuffer. Once a consumer of this channel is created,
	//    the first consumer will receive this message.
	// Note:
	//    To simplify the logic, it only send message to the first consumer.
	//    Since the MemMQ is only used for Standalone mode.
	msgBuffer []*ChannelMsg
}

func (mmq *MemMQ) CreateChannel(channelName string) error {
	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	if _, ok := mmq.consumers[channelName]; !ok {
		consumers := make([]*MemConsumer, 0)
		mmq.consumers[channelName] = consumers
	}

	return nil
}

func (mmq *MemMQ) DestroyChannel(channelName string) error {
	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	consumers, ok := mmq.consumers[channelName]
	if ok {
		// send nil to consumer so that client can close it self
		for _, consumer := range consumers {
			consumer.MsgChan <- nil
		}
	}

	delete(mmq.consumers, channelName)
	return nil
}

func (mmq *MemMQ) CreateConsumerGroup(groupName string, channelName string) (*MemConsumer, error) {
	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	consumers, ok := mmq.consumers[channelName]
	if !ok {
		consumers = make([]*MemConsumer, 0)
		mmq.consumers[channelName] = consumers
	}

	// exist?
	for _, consumer := range consumers {
		if consumer.GroupName == groupName {
			return consumer, nil
		}
	}

	// append new
	consumer := MemConsumer{
		GroupName:   groupName,
		ChannelName: channelName,
		MsgChan:     make(chan *MsgPack, 1024),
	}

	// consume messages of previous produce
	tempMsgBuf := make([]*ChannelMsg, 0)
	for _, msg := range mmq.msgBuffer {
		if msg.ChannelName == channelName {
			consumer.MsgChan <- msg.Msg
		} else {
			tempMsgBuf = append(tempMsgBuf, msg)
		}
	}
	mmq.msgBuffer = tempMsgBuf

	mmq.consumers[channelName] = append(mmq.consumers[channelName], &consumer)
	return &consumer, nil
}

func (mmq *MemMQ) DestroyConsumerGroup(groupName string, channelName string) error {
	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	consumers, ok := mmq.consumers[channelName]
	if !ok {
		return nil
	}

	tempConsumers := make([]*MemConsumer, 0)
	for _, consumer := range consumers {
		if consumer.GroupName == groupName {
			// send nil to consumer so that client can close it self
			consumer.MsgChan <- nil
		} else {
			tempConsumers = append(tempConsumers, consumer)
		}
	}
	mmq.consumers[channelName] = tempConsumers

	return nil
}

func (mmq *MemMQ) Produce(channelName string, msgPack *MsgPack) error {
	if msgPack == nil {
		return nil
	}

	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	consumers, ok := mmq.consumers[channelName]
	if !ok {
		return errors.New("Channel " + channelName + " doesn't exist")
	}

	if len(consumers) > 0 {
		// consumer already exist, send msg
		for _, consumer := range consumers {
			consumer.MsgChan <- msgPack
		}
	} else {
		// consumer not exist, put the msg to buffer, it will be consumed by the first consumer later
		msg := &ChannelMsg{
			ChannelName: channelName,
			Msg:         msgPack,
		}
		mmq.msgBuffer = append(mmq.msgBuffer, msg)
	}

	return nil
}

func (mmq *MemMQ) Broadcast(msgPack *MsgPack) error {
	if msgPack == nil {
		return nil
	}

	mmq.consumerMu.Lock()
	defer mmq.consumerMu.Unlock()

	for _, consumers := range mmq.consumers {
		for _, consumer := range consumers {
			consumer.MsgChan <- msgPack
		}
	}
	return nil
}

func (mmq *MemMQ) Consume(groupName string, channelName string) (*MsgPack, error) {
	var consumer *MemConsumer = nil
	mmq.consumerMu.Lock()

	consumers, ok := mmq.consumers[channelName]
	if !ok {
		return nil, errors.New("Channel " + channelName + " doesn't exist")
	}

	for _, c := range consumers {
		if c.GroupName == groupName {
			consumer = c
			break
		}
	}
	mmq.consumerMu.Unlock()

	msg, ok := <-consumer.MsgChan
	if !ok {
		return nil, nil
	}

	return msg, nil
}

func InitMmq() error {
	var err error
	once.Do(func() {
		Mmq = &MemMQ{
			consumerMu: sync.Mutex{},
		}
		Mmq.consumers = make(map[string][]*MemConsumer)
		Mmq.msgBuffer = make([]*ChannelMsg, 0)
	})
	return err
}
