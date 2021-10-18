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

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is an alias for short
type UniqueID = typeutil.UniqueID

// Timestamp is an alias for short
type Timestamp = typeutil.Timestamp

// IntPrimaryKey is an alias for short
type IntPrimaryKey = typeutil.IntPrimaryKey

// MsgPosition is an alias for short
type MsgPosition = internalpb.MsgPosition

// MessageID is an alias for short
type MessageID = mqclient.MessageID

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
	Start()
	Close()
	Chan() <-chan *MsgPack
	AsProducer(channels []string)
	AsConsumer(channels []string, subName string)
	AsConsumerWithPosition(channels []string, subName string, position mqclient.SubscriptionInitialPosition)
	SetRepackFunc(repackFunc RepackFunc)
	ComputeProduceChannelIndexes(tsMsgs []TsMsg) [][]int32
	GetProduceChannels() []string
	Produce(*MsgPack) error
	ProduceMark(*MsgPack) (map[string][]MessageID, error)
	Broadcast(*MsgPack) error
	BroadcastMark(*MsgPack) (map[string][]MessageID, error)
	Consume() *MsgPack
	Seek(offset []*MsgPosition) error
}

// Factory is an interface that can be used to generate a new msgstream object
type Factory interface {
	SetParams(params map[string]interface{}) error
	NewMsgStream(ctx context.Context) (MsgStream, error)
	NewTtMsgStream(ctx context.Context) (MsgStream, error)
	NewQueryMsgStream(ctx context.Context) (MsgStream, error)
}
