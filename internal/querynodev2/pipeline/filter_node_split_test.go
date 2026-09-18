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
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

const (
	splitTestCollectionID = int64(111)
	splitTestSource       = "by-dev-rootcoord-dml_0_111v0"
	splitTestTarget1      = "by-dev-rootcoord-dml_1_111v1"
	splitTestTarget2      = "by-dev-rootcoord-dml_2_111v2"
	splitTestBystander    = "by-dev-rootcoord-dml_3_111v3"
)

// buildSplitShardTsMsg builds the replica of one SplitShard broadcast that lands
// on deliveredOn. Every replica of the broadcast carries the same header; only
// the vchannel differs.
func buildSplitShardTsMsg(t *testing.T, collectionID int64, deliveredOn string, source string, targetVChannels ...string) msgstream.TsMsg {
	mutableMsg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			SplitTaskId:     7,
			SourceVchannel:  source,
			TargetVchannels: targetVChannels,
		}).
		WithBody(&message.SplitShardMessageBody{}).
		WithVChannel(deliveredOn).
		BuildMutable()
	require.NoError(t, err)
	immutableMsg := mutableMsg.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(1))
	tsMsg, err := adaptor.NewSplitShardMessageBody(immutableMsg)
	require.NoError(t, err)
	return tsMsg
}

func TestFilterNodeSplitShard(t *testing.T) {
	source := splitTestSource

	t.Run("dispatches the fence to the source delegator's ProcessSplitShard", func(t *testing.T) {
		mockDelegator := delegator.NewMockShardDelegator(t)
		mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.MatchedBy(func(targets []string) bool {
			return len(targets) == 2 && targets[0] == splitTestTarget1 && targets[1] == splitTestTarget2
		})).Return(nil).Once()

		fNode := newFilterNode(splitTestCollectionID, splitTestSource, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID, splitTestSource, source, splitTestTarget1, splitTestTarget2))
		assert.NoError(t, err)
	})

	t.Run("a spawn refusal on the source is surfaced", func(t *testing.T) {
		mockDelegator := delegator.NewMockShardDelegator(t)
		mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()

		fNode := newFilterNode(splitTestCollectionID, splitTestSource, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID, splitTestSource, source, splitTestTarget1))
		assert.Error(t, err)
	})

	t.Run("a fence for another collection is rejected", func(t *testing.T) {
		mockDelegator := delegator.NewMockShardDelegator(t)
		// ProcessSplitShard must not be called for a mismatched collection.

		fNode := newFilterNode(splitTestCollectionID, splitTestSource, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID+1, splitTestSource, source, splitTestTarget1))
		assert.Error(t, err)
	})
}

// TestFilterNodeSplitShardOnlySourceSpawns delivers every replica of ONE
// SplitShard broadcast -- the source's fence, a target's genesis and a
// bystander's no-op -- to a delegator on that vchannel, and asserts that only the
// source's delegator spawns children. A target spawning children would front
// itself, and a bystander would front targets it does not own, returning their
// rows twice through two parents.
func TestFilterNodeSplitShardOnlySourceSpawns(t *testing.T) {
	source := splitTestSource
	targets := []string{splitTestTarget1, splitTestTarget2}
	spawns := map[string]int{}

	for _, deliveredOn := range []string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestBystander} {
		mockDelegator := delegator.NewMockShardDelegator(t)
		mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ []string) error {
				spawns[deliveredOn]++
				return nil
			}).Maybe()

		fNode := newFilterNode(splitTestCollectionID, deliveredOn, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID, deliveredOn, source, targets...))
		assert.NoError(t, err, "replica on %s", deliveredOn)
	}

	assert.Equal(t, map[string]int{splitTestSource: 1}, spawns)
}
