// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package messageutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const (
	testSplitSource  = "by-dev-rootcoord-dml_0_10v0"
	testSplitTarget1 = "by-dev-rootcoord-dml_1_10v1"
	testSplitTarget2 = "by-dev-rootcoord-dml_2_10v2"
	testSplitOther   = "by-dev-rootcoord-dml_3_10v3"
	testSplitControl = "by-dev-rootcoord-dml_0_vcchan"
)

func newTestSplitShardMutable(t *testing.T, vchannel string) message.MutableMessage {
	t.Helper()
	return message.NewSplitShardMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    10,
			SplitTaskId:     99,
			SourceVchannel:  testSplitSource,
			TargetVchannels: []string{testSplitTarget1, testSplitTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{
			GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 10, PartitionIds: []int64{11, 12}, DbId: 1},
			Genesis:       &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: &schemapb.CollectionSchema{Name: "c"}, PartitionIDs: []int64{11, 12}},
			Routing:       &message.SplitShardRouting{RoutingModulus: 2},
		}).MustBuildMutable()
}

func newTestSplitShardImmutable(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	return newTestSplitShardMutable(t, vchannel).
		WithTimeTick(123).WithLastConfirmed(walimplstest.NewTestMessageID(7)).IntoImmutableMessage(walimplstest.NewTestMessageID(8))
}

func TestSplitShardRoleOf(t *testing.T) {
	header := &message.SplitShardMessageHeader{
		SourceVchannel:  testSplitSource,
		TargetVchannels: []string{testSplitTarget1, testSplitTarget2},
	}
	require.Equal(t, SplitShardRoleSource, SplitShardRoleOf(header, testSplitSource))
	require.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, testSplitTarget1))
	require.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, testSplitTarget2))
	require.Equal(t, SplitShardRoleControl, SplitShardRoleOf(header, testSplitControl))
	require.Equal(t, SplitShardRoleNone, SplitShardRoleOf(header, testSplitOther))
	require.Equal(t, SplitShardRoleNone, SplitShardRoleOf(header, ""))
	// An empty source never matches an empty vchannel.
	require.Equal(t, SplitShardRoleNone, SplitShardRoleOf(&message.SplitShardMessageHeader{}, ""))
}

func TestSplitShardFenceAndGenesisClassification(t *testing.T) {
	for _, tc := range []struct {
		vchannel string
		fence    bool
		genesis  bool
	}{
		{testSplitSource, true, false},
		{testSplitTarget1, false, true},
		{testSplitTarget2, false, true},
		{testSplitControl, false, false},
		{testSplitOther, false, false},
	} {
		// The classification must agree on the message before and after append.
		mutable := newTestSplitShardMutable(t, tc.vchannel)
		immutable := newTestSplitShardImmutable(t, tc.vchannel)
		for _, msg := range []message.BasicMessage{mutable, immutable} {
			require.Equal(t, tc.fence, IsSplitShardFence(msg), tc.vchannel)
			require.Equal(t, tc.genesis, IsSplitShardGenesis(msg), tc.vchannel)
			require.Equal(t, tc.fence || tc.genesis, AdvancesQueryTransformMVCC(msg), tc.vchannel)
		}
	}

	// Any other message type is neither.
	other := message.NewManualFlushMessageBuilderV2().WithVChannel(testSplitSource).
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
	require.False(t, IsSplitShardFence(other))
	require.False(t, IsSplitShardGenesis(other))
}

func TestSplitShardGenesisMessageKeepsWALOrigin(t *testing.T) {
	// Only a copy on a target vchannel is a genesis.
	for _, vchannel := range []string{testSplitSource, testSplitControl, testSplitOther} {
		require.Nil(t, SplitShardGenesisMessage(newTestSplitShardImmutable(t, vchannel)), vchannel)
	}
	notSplit := message.NewManualFlushMessageBuilderV2().WithVChannel(testSplitTarget1).
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().
		WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	require.Nil(t, SplitShardGenesisMessage(notSplit))

	for _, vchannel := range []string{testSplitTarget1, testSplitTarget2} {
		msg := newTestSplitShardImmutable(t, vchannel)
		created := SplitShardGenesisMessage(msg)
		require.NotNil(t, created)
		require.Equal(t, message.MessageTypeCreateCollection, created.MessageType())
		require.Equal(t, uint64(123), created.TimeTick())
		require.Equal(t, msg.MessageID(), created.MessageID())
		require.Equal(t, msg.LastConfirmedMessageID(), created.LastConfirmedMessageID())
		require.Equal(t, vchannel, created.VChannel())
		require.Equal(t, int64(10), created.Header().GetCollectionId())
		require.Equal(t, []int64{11, 12}, created.Header().GetPartitionIds())
		require.Equal(t, "c", created.MustBody().GetCollectionSchema().GetName())
	}
}
