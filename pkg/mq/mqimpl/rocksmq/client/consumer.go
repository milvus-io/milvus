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

package client

import (
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
)

// UniqueID is the type of message ID
type UniqueID = server.UniqueID

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
	common.SubscriptionInitialPosition

	// Message for this consumer
	// When a message is received, it will be pushed to this channel for consumption
	MessageChannel chan common.Message
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
	Chan() <-chan common.Message

	// Seek to the uniqueID position
	Seek(UniqueID) error //nolint:govet

	// Close consumer
	Close()

	// GetLatestMsgID get the latest msgID
	GetLatestMsgID() (int64, error)

	// check created topic whether vaild or not
	CheckTopicValid(topic string) error
}
