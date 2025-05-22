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
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestFlushMsgHandler_HandleFlush(t *testing.T) {
	vchannel := "ch-0"

	// test failed
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))

	msg := message.NewFlushMessageBuilderV2().
		WithBroadcast([]string{vchannel}).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 0,
			SegmentId:    1,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()[0]

	handler := newMsgHandler(wbMgr)
	msgID := mock_message.NewMockMessageID(t)
	im, err := message.AsImmutableFlushMessageV2(msg.IntoImmutableMessage(msgID))
	assert.NoError(t, err)
	err = handler.HandleFlush(im)
	assert.Error(t, err)

	// test normal
	wbMgr = writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	handler = newMsgHandler(wbMgr)
	err = handler.HandleFlush(im)
	assert.NoError(t, err)
}

func TestFlushMsgHandler_HandleManualFlush(t *testing.T) {
	vchannel := "ch-0"

	// test failed
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))
	wbMgr.EXPECT().FlushChannel(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))

	msg := message.NewManualFlushMessageBuilderV2().
		WithBroadcast([]string{vchannel}).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 0,
			FlushTs:      1000,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()[0]

	handler := newMsgHandler(wbMgr)
	msgID := mock_message.NewMockMessageID(t)
	im, err := message.AsImmutableManualFlushMessageV2(msg.IntoImmutableMessage(msgID))
	assert.NoError(t, err)
	err = handler.HandleManualFlush(im)
	assert.Error(t, err)

	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Unset()
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = handler.HandleManualFlush(im)
	assert.Error(t, err)

	// test normal
	wbMgr.EXPECT().FlushChannel(mock.Anything, mock.Anything, mock.Anything).Unset()
	wbMgr.EXPECT().FlushChannel(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err = handler.HandleManualFlush(im)
	assert.NoError(t, err)
}

func TestFlushMsgHandler_HandlSchemaChange(t *testing.T) {
	vchannel := "ch-0"

	w := mock_streaming.NewMockWALAccesser(t)
	b := mock_streaming.NewMockBroadcast(t)
	w.EXPECT().Broadcast().Return(b)
	b.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil)
	streaming.SetWALForTest(w)

	// test failed
	wbMgr := writebuffer.NewMockBufferManager(t)
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))

	msg := message.NewSchemaChangeMessageBuilderV2().
		WithBroadcast([]string{vchannel}).
		WithHeader(&message.SchemaChangeMessageHeader{
			CollectionId:      0,
			FlushedSegmentIds: []int64{1},
		}).
		WithBody(&message.SchemaChangeMessageBody{}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()[0]

	handler := newMsgHandler(wbMgr)
	msgID := mock_message.NewMockMessageID(t)
	im := message.MustAsImmutableCollectionSchemaChangeV2(msg.IntoImmutableMessage(msgID))
	err := handler.HandleSchemaChange(context.Background(), im)
	assert.Error(t, err)

	// test normal
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Unset()
	wbMgr.EXPECT().SealSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err = handler.HandleSchemaChange(context.Background(), im)
	assert.NoError(t, err)
}
