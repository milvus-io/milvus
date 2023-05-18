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

package pmq

import (
	"github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/client"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// Check pmqMessage implements ConsumerMessage
var _ mqwrapper.Message = (*pmqMessage)(nil)

// pmqMessage wraps the message for pebblemq
type pmqMessage struct {
	msg client.Message
}

// Topic returns the topic name of pebblemq message
func (pm *pmqMessage) Topic() string {
	return pm.msg.Topic
}

// Properties returns the properties of pebblemq message
func (pm *pmqMessage) Properties() map[string]string {
	return pm.msg.Properties
}

// Payload returns the payload of pebblemq message
func (pm *pmqMessage) Payload() []byte {
	return pm.msg.Payload
}

// ID returns the id of pebblemq message
func (pm *pmqMessage) ID() mqwrapper.MessageID {
	return &pmqID{messageID: pm.msg.MsgID}
}
