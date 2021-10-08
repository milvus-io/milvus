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

package rocksmq

// ProducerMessage that will be write to rocksdb
type ProducerMessage struct {
	Payload []byte
}

// Consumer is rocksmq consumer
type Consumer struct {
	Topic     string
	GroupName string
	MsgMutex  chan struct{}
}

// ConsumerMessage that consumed from rocksdb
type ConsumerMessage struct {
	MsgID   UniqueID
	Payload []byte
}

// RocksMQ is an interface thatmay be implemented by the application
// to do message queue operations based ion rocksdb
type RocksMQ interface {
	CreateTopic(topicName string) error
	DestroyTopic(topicName string) error
	CreateConsumerGroup(topicName string, groupName string) error
	DestroyConsumerGroup(topicName string, groupName string) error
	Close()

	RegisterConsumer(consumer *Consumer)

	Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error)
	Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error)
	Seek(topicName string, groupName string, msgID UniqueID) error
	SeekToLatest(topicName, groupName string) error
	ExistConsumerGroup(topicName string, groupName string) (bool, *Consumer)

	Notify(topicName, groupName string)
}
