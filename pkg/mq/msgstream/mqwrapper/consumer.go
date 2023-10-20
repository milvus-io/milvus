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

package mqwrapper

// SubscriptionInitialPosition is the type of a subscription initial position
type SubscriptionInitialPosition int

const (
	// SubscriptionPositionLatest is latest position which means the start consuming position will be the last message
	SubscriptionPositionLatest SubscriptionInitialPosition = iota

	// SubscriptionPositionEarliest is earliest position which means the start consuming position will be the first message
	SubscriptionPositionEarliest

	// SubscriptionPositionUnkown indicates we don't care about the consumer location, since we are doing another seek or only some meta api over that
	SubscriptionPositionUnknown
)

const DefaultPartitionIdx = 0

// UniqueID is the type of message id
type UniqueID = int64

// ConsumerOptions contains the options of a consumer
type ConsumerOptions struct {
	// The topic that this consumer will subscribe on
	Topic string

	// The subscription name for this consumer
	SubscriptionName string

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Set receive channel size
	BufSize int64
}

// Consumer is the interface that provides operations of a consumer
type Consumer interface {
	// returns the subscription for the consumer
	Subscription() string

	// Get Message channel, once you chan you can not seek again
	Chan() <-chan Message

	// Seek to the uniqueID position, the second bool param indicates whether the message is included in the position
	Seek(MessageID, bool) error //nolint:govet

	// Ack make sure that msg is received
	Ack(Message)

	// Close consumer
	Close()

	// GetLatestMsgID return the latest message ID
	GetLatestMsgID() (MessageID, error)

	// check created topic whether vaild or not
	CheckTopicValid(channel string) error
}
