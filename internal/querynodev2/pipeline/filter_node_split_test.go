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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
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

	t.Run("a spawn refusal is logged at warn and still returned", func(t *testing.T) {
		// The generic filtrate error path logs at Debug, which production
		// clusters never show: a fence that cannot be fronted has to be visible,
		// since until it is fronted the split key range is unreadable.
		var warned []string
		warnMock := mockey.Mock(mlog.RatedWarn).To(func(_ context.Context, _ rate.Limit, msg string, _ ...mlog.Field) {
			warned = append(warned, msg)
		}).Build()
		defer warnMock.UnPatch()

		refusal := errors.New("spawner is not configured")
		mockDelegator := delegator.NewMockShardDelegator(t)
		mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).Return(refusal).Once()

		fNode := newFilterNode(splitTestCollectionID, splitTestSource, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID, splitTestSource, source, splitTestTarget1))
		assert.ErrorIs(t, err, refusal)
		assert.Len(t, warned, 1, "the refusal must be logged above debug")
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

// TestFilterNodeSplitShardHeaderShape pins the fence the replicated SplitShard
// broadcast carries: one source_vchannel and the target vchannel names. The
// residues and the modulus live only in the body's routing post-image, which
// the querynode never reads, so an empty body still fronts both targets, in
// the header's order.
func TestFilterNodeSplitShardHeaderShape(t *testing.T) {
	var got []string
	mockDelegator := delegator.NewMockShardDelegator(t)
	mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, targets []string) error {
			got = targets
			return nil
		}).Once()

	fNode := newFilterNode(splitTestCollectionID, splitTestSource, nil, mockDelegator, 8)
	tsMsg := buildSplitShardTsMsg(t, splitTestCollectionID, splitTestSource, splitTestSource, splitTestTarget2, splitTestTarget1)
	header := tsMsg.(*adaptor.SplitShardMessageBody).SplitShardMessage.Header()
	require.Equal(t, splitTestSource, header.GetSourceVchannel())
	require.Nil(t, tsMsg.(*adaptor.SplitShardMessageBody).SplitShardMessage.MustBody().GetRouting())

	assert.NoError(t, fNode.filtrate(nil, tsMsg))
	assert.Equal(t, []string{splitTestTarget2, splitTestTarget1}, got)
}

// TestFilterNodeSplitShardMisrouteIsWarnedNotSpawned: the broadcast reaches
// only the source, the targets and the control channel. A replica on any other
// vchannel is a misroute; it passes through without spawning, and is logged
// above Debug, unlike a target's genesis replica.
func TestFilterNodeSplitShardMisrouteIsWarnedNotSpawned(t *testing.T) {
	var warned []string
	warnMock := mockey.Mock(mlog.RatedWarn).To(func(_ context.Context, _ rate.Limit, msg string, _ ...mlog.Field) {
		warned = append(warned, msg)
	}).Build()
	defer warnMock.UnPatch()

	for _, deliveredOn := range []string{splitTestTarget1, splitTestBystander} {
		mockDelegator := delegator.NewMockShardDelegator(t)
		fNode := newFilterNode(splitTestCollectionID, deliveredOn, nil, mockDelegator, 8)
		err := fNode.filtrate(nil, buildSplitShardTsMsg(t, splitTestCollectionID, deliveredOn, splitTestSource, splitTestTarget1, splitTestTarget2))
		assert.NoError(t, err, "replica on %s", deliveredOn)
	}
	assert.Len(t, warned, 1, "only the misrouted replica is warned, not the target's genesis")
}

// TestFilterNodeOperateConsumesSplitShard drives the fence through Operate, the
// path a message takes once the delegator msgstream whitelist lets SplitShard
// through. Before this layer the filter node's default branch dropped it; now
// it reaches ProcessSplitShard and the message is kept without adding any
// insert or delete to the pipeline.
func TestFilterNodeOperateConsumesSplitShard(t *testing.T) {
	collection := segments.NewTestCollection(splitTestCollectionID, querypb.LoadType_LoadCollection, nil)
	collectionManager := segments.NewMockCollectionManager(t)
	collectionManager.EXPECT().Get(splitTestCollectionID).Return(collection)
	manager := &segments.Manager{Collection: collectionManager}

	mockDelegator := delegator.NewMockShardDelegator(t)
	mockDelegator.EXPECT().ProcessSplitShard(mock.Anything, []string{splitTestTarget1, splitTestTarget2}).Return(nil).Once()
	mockDelegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Return()

	fNode := newFilterNode(splitTestCollectionID, splitTestSource, manager, mockDelegator, 8)
	fence := buildSplitShardTsMsg(t, splitTestCollectionID, splitTestSource, splitTestSource, splitTestTarget1, splitTestTarget2)
	out := fNode.Operate(&msgstream.MsgPack{BeginTs: 90, EndTs: 100, Msgs: []msgstream.TsMsg{fence}})

	nodeMsg, ok := out.(*insertNodeMsg)
	require.True(t, ok)
	assert.Empty(t, nodeMsg.insertMsgs)
	assert.Empty(t, nodeMsg.deleteMsgs)
	assert.NoError(t, (&insertNodeMsg{}).append(fence), "the insert node message must accept a consumed fence")
}
