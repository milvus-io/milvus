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

package rmq

import (
	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/client"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

// Check rmqMessage implements ConsumerMessage
var _ mqwrapper.Message = (*rmqMessage)(nil)

// rmqMessage wraps the message for pebblemq
type rmqMessage struct {
	msg client.Message
}

// Topic returns the topic name of pebblemq message
func (rm *rmqMessage) Topic() string {
	return rm.msg.Topic
}

// Properties returns the properties of pebblemq message
func (rm *rmqMessage) Properties() map[string]string {
	return nil
}

// Payload returns the payload of pebblemq message
func (rm *rmqMessage) Payload() []byte {
	return rm.msg.Payload
}

// ID returns the id of pebblemq message
func (rm *rmqMessage) ID() mqwrapper.MessageID {
	return &rmqID{messageID: rm.msg.MsgID}
}
