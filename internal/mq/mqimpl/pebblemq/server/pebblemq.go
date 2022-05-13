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

package server

import "context"

// ProducerMessage that will be written to pebble
type ProducerMessage struct {
	Payload []byte
}

// Consumer is pebblemq consumer
type Consumer struct {
	Topic     string
	GroupName string
	MsgMutex  chan struct{}
	beginID   UniqueID
}

// ConsumerMessage that consumed from pebble
type ConsumerMessage struct {
	MsgID   UniqueID
	Payload []byte
}

// PebbleMQ is an interface thatmay be implemented by the application
// to do message queue operations based on pebble
type PebbleMQ interface {
	CreateTopic(topicName string) error
	DestroyTopic(topicName string) error
	CreateConsumerGroup(topicName string, groupName string) error
	DestroyConsumerGroup(topicName string, groupName string) error
	Close()

	RegisterConsumer(consumer *Consumer) error
	GetLatestMsg(topicName string) (int64, error)

	Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error)
	Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error)
	Seek(topicName string, groupName string, msgID UniqueID) error
	SeekToLatest(topicName, groupName string) error
	ExistConsumerGroup(topicName string, groupName string) (bool, *Consumer, error)

	Notify(topicName, groupName string)

	CreateReader(topicName string, startMsgID UniqueID, messageIDInclusive bool, subscriptionRolePrefix string) (string, error)
	ReaderSeek(topicName string, readerName string, msgID UniqueID) error
	Next(ctx context.Context, topicName string, readerName string) (*ConsumerMessage, error)
	HasNext(topicName string, readerName string) bool
	CloseReader(topicName string, readerName string)
}
