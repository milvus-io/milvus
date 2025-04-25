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

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

// Producer is the interface that provides operations of producer
type Producer interface {
	// return the topic which producer is publishing to
	// Topic() string

	// publish a message
	Send(ctx context.Context, message *common.ProducerMessage) (common.MessageID, error)

	Close()

	Healthy() bool
}
