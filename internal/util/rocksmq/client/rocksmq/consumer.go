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

import server "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"

// SubscriptionInitialPosition is the initial subscription position
type SubscriptionInitialPosition int

// UniqueID is the type of message ID
type UniqueID = server.UniqueID

// List 2 kinds of SubscriptionInitialPosition
const (
	SubscriptionPositionLatest SubscriptionInitialPosition = iota
	SubscriptionPositionEarliest
)

// EarliestMessageID is used to get the earliest message ID, default -1
func EarliestMessageID() UniqueID {
	return -1
}

// ConsumerOptions is the options of a consumer
type ConsumerOptions struct {
	// The topic that this consumer will subscribe on
	Topic string

	// The subscription name for this consumer
	SubscriptionName string

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Message for this consumer
	// When a message is received, it will be pushed to this channel for consumption
	MessageChannel chan ConsumerMessage
}

// ConsumerMessage is the message content of a consumer message
type ConsumerMessage struct {
	Consumer
	MsgID   UniqueID
	Topic   string
	Payload []byte
}

// Consumer interface provide operations for a consumer
type Consumer interface {
	// returns the subscription for the consumer
	Subscription() string

	// returns the topic for the consumer
	Topic() string

	// Signal channel
	MsgMutex() chan struct{}

	// Message channel
	Chan() <-chan ConsumerMessage

	// Seek to the uniqueID position
	Seek(UniqueID) error //nolint:govet

	// Close consumer
	Close()
}
