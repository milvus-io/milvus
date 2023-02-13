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

package api

import (
	"context"

	"github.com/milvus-io/milvus/cdc/core/pb"
	"github.com/milvus-io/milvus/cdc/core/util"
)

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
	util.CDCMark

	// returns the subscription for the consumer
	Subscription() string

	// Get Message channel, once you chan you can not seek again
	Chan() <-chan Message

	// Seek to the uniqueID position
	Seek(MessageID, bool) error //nolint:govet

	// Ack make sure that msg is received
	Ack(Message)

	// Close consumer
	Close()

	// GetLatestMsgID return the latest message ID
	GetLatestMsgID() (MessageID, error)
}

// MsgPack represents a batch of msg in msgstream
type MsgPack struct {
	BeginTs        Timestamp
	EndTs          Timestamp
	Msgs           []TsMsg
	StartPositions []*MsgPosition
	EndPositions   []*MsgPosition
}

// RepackFunc is a function type which used to repack message after hash by primary key
type RepackFunc func(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error)

// MsgStream is an interface that can be used to produce and consume message on message queue
type MsgStream interface {
	util.CDCMark
	Close()

	//AsProducer(channels []string)
	//Produce(*MsgPack) error
	//SetRepackFunc(repackFunc RepackFunc)
	//GetProduceChannels() []string
	//Broadcast(*MsgPack) (map[string][]MessageID, error)

	AsConsumer(channels []string, subName string, position SubscriptionInitialPosition)
	Chan() <-chan *MsgPack
	Seek(offset []*pb.MsgPosition) error

	GetLatestMsgID(channel string) (MessageID, error)
}

type Factory interface {
	util.CDCMark
	NewMsgStream(ctx context.Context) (MsgStream, error)
	NewMsgStreamDisposer(ctx context.Context) func([]string, string) error
}
