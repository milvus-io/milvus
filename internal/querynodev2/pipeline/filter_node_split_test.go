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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func buildSplitShardTsMsg(t *testing.T, collectionID int64, sourceVChannel string, targetVChannels ...string) msgstream.TsMsg {
	targets := make([]*message.SplitShardTarget, 0, len(targetVChannels))
	for _, vchannel := range targetVChannels {
		targets = append(targets, &message.SplitShardTarget{Vchannel: vchannel})
	}
	mutableMsg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{CollectionId: collectionID, Targets: targets}).
		WithBody(&message.SplitShardMessageBody{}).
		WithVChannel(sourceVChannel).
		BuildMutable()
	require.NoError(t, err)
	immutableMsg := mutableMsg.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(1))
	tsMsg, err := adaptor.NewSplitShardMessageBody(immutableMsg)
	require.NoError(t, err)
	return tsMsg
}

func TestFilterNodeSplitShard(t *testing.T) {
	const collectionID = int64(111)

	t.Run("dispatches the fence to the delegator's ProcessSplitShard", func(t *testing.T) {
		mockDelegator := delegator.NewMockShardDelegator(t)
		mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.MatchedBy(func(targets []*messagespb.SplitShardTarget) bool {
			return len(targets) == 2 && targets[0].GetVchannel() == "v1" && targets[1].GetVchannel() == "v2"
		})).Return(nil).Once()

		fNode := newFilterNode(collectionID, "v0", nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, collectionID, "v0", "v1", "v2"))
		assert.NoError(t, err)
	})

	t.Run("a fence for another collection is rejected", func(t *testing.T) {
		mockDelegator := delegator.NewMockShardDelegator(t)
		// ProcessSplitShard must not be called for a mismatched collection.

		fNode := newFilterNode(collectionID, "v0", nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, collectionID+1, "v0", "v1"))
		assert.Error(t, err)
	})
}
