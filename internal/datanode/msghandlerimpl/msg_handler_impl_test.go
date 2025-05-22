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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMsgHandlerImpl(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	b := broker.NewMockBroker(t)
	m := NewMsgHandlerImpl(b)
	assert.Panics(t, func() {
		m.HandleCreateSegment(nil, nil)
	})
	assert.Panics(t, func() {
		m.HandleFlush(nil)
	})
	assert.Panics(t, func() {
		m.HandleManualFlush(nil)
	})
	t.Run("HandleImport success", func(t *testing.T) {
		wal := mock_streaming.NewMockWALAccesser(t)
		bo := mock_streaming.NewMockBroadcast(t)
		wal.EXPECT().Broadcast().Return(bo)
		bo.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil)
		streaming.SetWALForTest(wal)
		defer streaming.RecoverWALForTest()

		b.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()
		b.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil, nil).Once()
		err := m.HandleImport(ctx, "", nil)
		assert.NoError(t, err)
	})
}
