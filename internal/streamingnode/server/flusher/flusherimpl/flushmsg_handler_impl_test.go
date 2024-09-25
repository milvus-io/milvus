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

package flusherimpl

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestFlushMsgHandler_HandleFlush(t *testing.T) {
	vchannel := "ch-0"

	// test failed
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))

	msg, err := message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{
			CollectionId: 0,
			SegmentId:    []int64{1, 2, 3},
		}).
		BuildMutable()
	assert.NoError(t, err)

	handler := newFlushMsgHandler(wbMgr)
	msgID := mock_message.NewMockMessageID(t)
	im, err := message.AsImmutableFlushMessageV2(msg.IntoImmutableMessage(msgID))
	assert.NoError(t, err)
	err = handler.HandleFlush(vchannel, im)
	assert.Error(t, err)

	// test normal
	wbMgr = writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	handler = newFlushMsgHandler(wbMgr)
	err = handler.HandleFlush(vchannel, im)
	assert.NoError(t, err)
}

func TestFlushMsgHandler_HandleManualFlush(t *testing.T) {
	vchannel := "ch-0"

	// test failed
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().FlushChannel(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))

	msg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 0,
			FlushTs:      1000,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)

	handler := newFlushMsgHandler(wbMgr)
	msgID := mock_message.NewMockMessageID(t)
	im, err := message.AsImmutableManualFlushMessageV2(msg.IntoImmutableMessage(msgID))
	assert.NoError(t, err)
	err = handler.HandleManualFlush(vchannel, im)
	assert.Error(t, err)

	// test normal
	wbMgr = writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().FlushChannel(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	handler = newFlushMsgHandler(wbMgr)
	err = handler.HandleManualFlush(vchannel, im)
	assert.NoError(t, err)
}
