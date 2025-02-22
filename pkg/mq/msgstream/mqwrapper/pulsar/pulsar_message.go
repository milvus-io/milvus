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

package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

// Check pulsarMessage implements ConsumerMessage
var _ common.Message = (*pulsarMessage)(nil)

type pulsarMessage struct {
	msg pulsar.Message
}

func (pm *pulsarMessage) Topic() string {
	return pm.msg.Topic()
}

func (pm *pulsarMessage) Properties() map[string]string {
	return pm.msg.Properties()
}

func (pm *pulsarMessage) Payload() []byte {
	return pm.msg.Payload()
}

func (pm *pulsarMessage) ID() common.MessageID {
	id := pm.msg.ID()
	pid := &pulsarID{messageID: id}
	return pid
}
