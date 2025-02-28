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
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Check rmqMessage implements ConsumerMessage
var _ common.Message = (*RmqMessage)(nil)

// rmqMessage wraps the message for rocksmq
type RmqMessage struct {
	msgID      typeutil.UniqueID
	topic      string
	payload    []byte
	properties map[string]string
}

// Topic returns the topic name of rocksmq message
func (rm *RmqMessage) Topic() string {
	return rm.topic
}

// Properties returns the properties of rocksmq message
func (rm *RmqMessage) Properties() map[string]string {
	return rm.properties
}

// Payload returns the payload of rocksmq message
func (rm *RmqMessage) Payload() []byte {
	return rm.payload
}

// ID returns the id of rocksmq message
func (rm *RmqMessage) ID() common.MessageID {
	return &server.RmqID{MessageID: rm.msgID}
}
