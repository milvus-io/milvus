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

package mqclient

import (
	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
)

// Check rmqMessage implements ConsumerMessage
var _ ConsumerMessage = (*rmqMessage)(nil)

type rmqMessage struct {
	msg rocksmq.ConsumerMessage
}

func (rm *rmqMessage) Topic() string {
	return rm.msg.Topic
}

func (rm *rmqMessage) Properties() map[string]string {
	return nil
}

func (rm *rmqMessage) Payload() []byte {
	return rm.msg.Payload
}

func (rm *rmqMessage) ID() MessageID {
	return &rmqID{messageID: rm.msg.MsgID}
}
