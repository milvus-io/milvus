/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package msghandlerimpl

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type msgHandlerImpl struct {
	broker broker.Broker
}

func (m *msgHandlerImpl) HandleCreateSegment(ctx context.Context, createSegmentMsg message.ImmutableCreateSegmentMessageV2) error {
	panic("unreachable code")
}

func (m *msgHandlerImpl) HandleFlush(flushMsg message.ImmutableFlushMessageV2) error {
	panic("unreachable code")
}

func (m *msgHandlerImpl) HandleManualFlush(flushMsg message.ImmutableManualFlushMessageV2) error {
	panic("unreachable code")
}

func (impl *msgHandlerImpl) HandleSchemaChange(ctx context.Context, msg message.ImmutableSchemaChangeMessageV2) error {
	panic("unreachable code")
}

func NewMsgHandlerImpl(broker broker.Broker) *msgHandlerImpl {
	return &msgHandlerImpl{
		broker: broker,
	}
}
