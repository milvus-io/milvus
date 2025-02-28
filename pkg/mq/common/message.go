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

package common

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// ProducerOptions contains the options of a producer
type ProducerOptions struct {
	// The topic that this Producer will publish
	Topic string

	// Enable compression
	// For Pulsar, this enables ZSTD compression with default compression level
	EnableCompression bool
}

// ProducerMessage contains the messages of a producer
type ProducerMessage struct {
	// Payload get the payload of the message
	Payload []byte
	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties map[string]string
}

// Message is the interface that provides operations of a consumer
type Message interface {
	// Topic get the topic from which this message originated from
	Topic() string

	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties() map[string]string

	// Payload get the payload of the message
	Payload() []byte

	// ID get the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	ID() MessageID
}

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

const (
	MsgTypeKey          = "msg_type"
	MsgIdTypeKey        = "msg_id"
	TimestampTypeKey    = "timestamp"
	ChannelTypeKey      = "vchannel"
	CollectionIDTypeKey = "collection_id"
	ReplicateIDTypeKey  = "replicate_id"
)

// GetMsgType gets the message type from message.
func GetMsgType(msg Message) (commonpb.MsgType, error) {
	return GetMsgTypeFromRaw(msg.Payload(), msg.Properties())
}

// GetMsgTypeFromRaw gets the message type from payload and properties.
func GetMsgTypeFromRaw(payload []byte, properties map[string]string) (commonpb.MsgType, error) {
	msgType := commonpb.MsgType_Undefined
	if properties != nil {
		if val, ok := properties[MsgTypeKey]; ok {
			msgType = commonpb.MsgType(commonpb.MsgType_value[val])
		}
	}
	if msgType == commonpb.MsgType_Undefined {
		header := commonpb.MsgHeader{}
		if payload == nil {
			return msgType, fmt.Errorf("failed to unmarshal message header, payload is empty")
		}
		err := proto.Unmarshal(payload, &header)
		if err != nil {
			return msgType, fmt.Errorf("failed to unmarshal message header, err %s", err.Error())
		}
		if header.Base == nil {
			return msgType, fmt.Errorf("failed to unmarshal message, header is uncomplete")
		}
		msgType = header.Base.MsgType
	}
	return msgType, nil
}
