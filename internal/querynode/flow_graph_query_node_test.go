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

package querynode

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestQueryNodeFlowGraph_consumerFlowGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSafe := newTSafeReplica()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()

	fg, err := newQueryNodeFlowGraph(ctx,
		defaultCollectionID,
		streamingReplica,
		tSafe,
		defaultDMLChannel,
		fac)
	assert.NoError(t, err)

	err = fg.consumeFlowGraph(defaultDMLChannel, defaultSubName)
	assert.NoError(t, err)

	fg.close()
}

func TestQueryNodeFlowGraph_seekQueryNodeFlowGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac := genFactory()

	tSafe := newTSafeReplica()

	fg, err := newQueryNodeFlowGraph(ctx,
		defaultCollectionID,
		streamingReplica,
		tSafe,
		defaultDMLChannel,
		fac)
	assert.NoError(t, err)

	position := &internalpb.MsgPosition{
		ChannelName: defaultDMLChannel,
		MsgID:       []byte{},
		MsgGroup:    defaultSubName,
		Timestamp:   0,
	}
	err = fg.seekQueryNodeFlowGraph(position)
	assert.Error(t, err)

	fg.close()
}

func Test_queryNodeFlowGraph_seekToLatest(t *testing.T) {
	t.Run("null dml message stream", func(t *testing.T) {
		fg := &queryNodeFlowGraph{dmlStream: nil}
		err := fg.seekToLatest("pchannel", "subname")
		assert.Error(t, err)
	})

	t.Run("failed to get latest message id", func(t *testing.T) {
		stream := newMockMsgStream()
		stream.getLatestMsgID = func(channel string) (msgstream.MessageID, error) {
			return nil, errors.New("mock")
		}
		fg := &queryNodeFlowGraph{dmlStream: stream}
		err := fg.seekToLatest("pchannel", "subname")
		assert.Error(t, err)
	})

	t.Run("at earliest position", func(t *testing.T) {
		earliestMsgID := newMockMessageID()
		earliestMsgID.atEarliestPosition = true
		stream := newMockMsgStream()
		stream.getLatestMsgID = func(channel string) (msgstream.MessageID, error) {
			return earliestMsgID, nil
		}
		fg := &queryNodeFlowGraph{dmlStream: stream}
		err := fg.seekToLatest("pchannel", "subname")
		assert.NoError(t, err)
	})

	t.Run("failed to seek", func(t *testing.T) {
		msgID := newMockMessageID()
		msgID.atEarliestPosition = false
		stream := newMockMsgStream()
		stream.getLatestMsgID = func(channel string) (msgstream.MessageID, error) {
			return msgID, nil
		}
		stream.seek = func(positions []*internalpb.MsgPosition) error {
			return errors.New("mock")
		}
		fg := &queryNodeFlowGraph{dmlStream: stream}
		err := fg.seekToLatest("pchannel", "subname")
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		msgID := newMockMessageID()
		msgID.atEarliestPosition = false
		stream := newMockMsgStream()
		stream.getLatestMsgID = func(channel string) (msgstream.MessageID, error) {
			return msgID, nil
		}
		stream.seek = func(positions []*internalpb.MsgPosition) error {
			return nil
		}
		fg := &queryNodeFlowGraph{dmlStream: stream}
		err := fg.seekToLatest("pchannel", "subname")
		assert.NoError(t, err)
	})
}
