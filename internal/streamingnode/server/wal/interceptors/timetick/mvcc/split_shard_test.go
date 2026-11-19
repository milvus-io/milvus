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
package mvcc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	splitSource  = "p0_1v0"
	splitTarget1 = "p1_1v1"
	splitTarget2 = "p2_1v2"
	splitControl = "p0_vcchan"
)

// newSplitShardCopy builds the copy of a SplitShard message on one vchannel.
func newSplitShardCopy(t *testing.T, vchannel string, timetick uint64) message.MutableMessage {
	t.Helper()
	return message.NewSplitShardMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  splitSource,
			TargetVchannels: []string{splitTarget1, splitTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().WithTimeTick(timetick)
}

func TestQueryMVCCSplitShardGenesisOpensTheTargetFrontier(t *testing.T) {
	for _, callbackFirst := range []bool{false, true} {
		cm := NewQueryMVCCManager(10)
		genesis := newSplitShardCopy(t, splitTarget1, 120)
		if callbackFirst {
			cm.ObserveAppend(genesis)
			// The genesis is pending: a query on the target must wait for it.
			require.EqualValues(t, 120, cm.Minimum(splitTarget1, false))
			_, ready := cm.Snapshot(splitTarget1, cm.Minimum(splitTarget1, false))
			require.False(t, ready)
		}
		publishQueryTestPrefix(t, cm, 120, genesis)
		if !callbackFirst {
			cm.ObserveAppend(genesis)
		}
		// Both frontiers of the target start at its genesis tick.
		current, err := cm.WaitSnapshot(t.Context(), splitTarget1, 120)
		require.NoError(t, err)
		require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120}, current)
		require.Empty(t, cm.pending)

		// From then on the target behaves like any vchannel.
		insert := createQueryTestMessage(t, 130, splitTarget1, message.MessageTypeInsert, false, true)
		deletion := createQueryTestMessage(t, 140, splitTarget1, message.MessageTypeDelete, false, true)
		publishQueryTestPrefix(t, cm, 130, insert)
		current, err = cm.WaitSnapshot(t.Context(), splitTarget1, 130)
		require.NoError(t, err)
		require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120}, current)
		publishQueryTestPrefix(t, cm, 140, deletion)
		current, err = cm.WaitSnapshot(t.Context(), splitTarget1, 140)
		require.NoError(t, err)
		require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140}, current)
	}
}

func TestQueryMVCCSplitShardGenesisDuplicateDoesNotRegress(t *testing.T) {
	cm := NewQueryMVCCManager(10)
	genesis := newSplitShardCopy(t, splitTarget1, 120)
	insert := createQueryTestMessage(t, 130, splitTarget1, message.MessageTypeInsert, false, true)
	deletion := createQueryTestMessage(t, 140, splitTarget1, message.MessageTypeDelete, false, true)
	publishQueryTestPrefix(t, cm, 140, genesis, insert, deletion)
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140}, cm.frontiers[splitTarget1])

	// A second delivery of the published genesis is covered by the published
	// prefix and is not applied again.
	publishQueryTestPrefix(t, cm, 140, genesis)
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140}, cm.frontiers[splitTarget1])
	// A delayed append callback of the genesis cannot create pending state.
	cm.ObserveAppend(genesis)
	require.Empty(t, cm.pending)

	// The genesis appended again by the broadcaster takes a later time tick.
	// It moves the frontiers forward only.
	again := newSplitShardCopy(t, splitTarget1, 150)
	publishQueryTestPrefix(t, cm, 150, again)
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 150, TransformingTimetick: 150}, cm.frontiers[splitTarget1])
}

func TestQueryMVCCSplitShardRecoveryReplay(t *testing.T) {
	// The WAL is reopened. Recovery found the target in the vchannel metadata
	// and registers it at the opening fence, which is above the genesis.
	cm := NewQueryMVCCManager(200)
	cm.ApplyRecoveryBarrier(splitTarget1, 200)
	// The genesis is at or below the recovered prefix and is not applied.
	publishQueryTestPrefix(t, cm, 200, newSplitShardCopy(t, splitTarget1, 120))
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 200, TransformingTimetick: 200}, cm.frontiers[splitTarget1])

	// The WAL is reopened before the genesis was persisted in the metadata:
	// the genesis is above the recovered prefix and opens the frontier, exactly
	// as on the first delivery.
	cm = NewQueryMVCCManager(100)
	publishQueryTestPrefix(t, cm, 120, newSplitShardCopy(t, splitTarget1, 120))
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120}, cm.frontiers[splitTarget1])
}

func TestQueryMVCCSplitShardFenceAdvancesTheSourceAndCreatesNothing(t *testing.T) {
	cm := NewQueryMVCCManager(10)
	cm.ApplyRecoveryBarrier(splitSource, 10)
	insert := createQueryTestMessage(t, 90, splitSource, message.MessageTypeInsert, false, true)
	fence := newSplitShardCopy(t, splitSource, 100)
	cm.ObserveAppend(fence)
	require.EqualValues(t, 100, cm.Minimum(splitSource, true))
	publishQueryTestPrefix(t, cm, 100, insert, fence)
	// The fence is the last event of the source. It advances the transform
	// frontier and leaves the growing frontier at the last write.
	current, err := cm.WaitSnapshot(t.Context(), splitSource, 100)
	require.NoError(t, err)
	require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 90, TransformingTimetick: 100}, current)
	// The source is still a live vchannel after the fence.
	require.Contains(t, cm.frontiers, splitSource)

	// A copy on the control channel, on a vchannel the message does not name,
	// or the fence of a vchannel that is not live, opens no frontier.
	other := NewQueryMVCCManager(10)
	for i, vchannel := range []string{splitControl, "p3_1v3", splitSource} {
		// Each copy is above the published prefix, so it is really applied.
		tick := uint64(100 + 10*i)
		msg := newSplitShardCopy(t, vchannel, tick)
		other.ObserveAppend(msg)
		publishQueryTestPrefix(t, other, tick, msg)
	}
	require.Empty(t, other.frontiers)
	_, err = other.WaitSnapshot(t.Context(), splitSource, 120)
	require.ErrorIs(t, err, merr.ErrChannelNotFound)
}

func TestRetainedMVCCSplitShard(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier(splitSource, 100)

	// The genesis registers the target as a live shard.
	cm.UpdateMVCC(newSplitShardCopy(t, splitTarget1, 120))
	require.Equal(t, VChannelMVCC{Timetick: 120}, cm.GetMVCCOfVChannel(splitTarget1))
	cm.UpdateMVCC(createQueryTestMessage(t, 140, splitTarget1, message.MessageTypeInsert, false, true))
	require.Equal(t, VChannelMVCC{Timetick: 140}, cm.GetMVCCOfVChannel(splitTarget1))
	// A genesis appended again does not move the shard backwards, and one at a
	// later time tick moves it forward.
	cm.UpdateMVCC(newSplitShardCopy(t, splitTarget1, 120))
	require.Equal(t, VChannelMVCC{Timetick: 140}, cm.GetMVCCOfVChannel(splitTarget1))
	cm.UpdateMVCC(newSplitShardCopy(t, splitTarget1, 150))
	require.Equal(t, VChannelMVCC{Timetick: 150}, cm.GetMVCCOfVChannel(splitTarget1))

	// The fence is an ordinary message of the source, which stays live.
	cm.UpdateMVCC(newSplitShardCopy(t, splitSource, 110))
	require.Equal(t, VChannelMVCC{Timetick: 110}, cm.GetMVCCOfVChannel(splitSource))
	require.Len(t, cm.vchannelMVCCTimestamps, 2)

	// A copy on the control channel, or the fence of a shard that is not live,
	// registers nothing.
	cm.UpdateMVCC(newSplitShardCopy(t, splitControl, 160))
	other := NewMVCCManager(100)
	other.UpdateMVCC(newSplitShardCopy(t, splitSource, 110))
	require.Empty(t, other.vchannelMVCCTimestamps)
	require.Len(t, cm.vchannelMVCCTimestamps, 2)
}
