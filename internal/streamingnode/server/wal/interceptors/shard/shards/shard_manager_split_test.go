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
package shards

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newSplitFenceShardManager recovers a manager that holds collection 1 on
// vchannel v_split, with partition 2 and one growing segment.
func newSplitFenceShardManager(t *testing.T, splitTaskID int64, appendErr error) *shardManagerImpl {
	m, _ := newSplitFenceShardManagerWithAppends(t, splitTaskID, appendErr)
	return m
}

// newSplitFenceShardManagerWithAppends also returns a channel that receives
// one value each time an allocation worker reaches the WAL.
func newSplitFenceShardManagerWithAppends(t *testing.T, splitTaskID int64, appendErr error) (*shardManagerImpl, <-chan struct{}) {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)
	appends := make(chan struct{}, 16)
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Unavailable().RunAndReturn(func() <-chan struct{} { return make(chan struct{}) }).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ message.MutableMessage) (*types.AppendResult, error) {
		select {
		case appends <- struct{}{}:
		default:
		}
		// The create segment message of an allocation never lands.
		<-ctx.Done()
		return nil, appendErr
	}).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)
	m := RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test_split_fence", Term: 1},
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
				VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{
					"v_split": {VChannel: "v_split", CollectionID: 1, PartitionIDs: []int64{2}, SplitTaskID: splitTaskID},
				},
				GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{
					1001: {
						VChannel: "v_split", CollectionID: 1, PartitionID: 2, SegmentID: 1001,
						Stat: &streamingpb.SegmentAssignmentStat{MaxBinarySize: 200, ModifiedBinarySize: 100, CreateSegmentTimeTick: 50},
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &mockedTxnManager{},
	}).(*shardManagerImpl)
	t.Cleanup(m.Close)
	return m, appends
}

func TestFenceForSplit(t *testing.T) {
	m := newSplitFenceShardManager(t, 0, context.Canceled)
	key := PartitionUniqueKey{CollectionID: 1, PartitionID: 2}

	// The collection must be registered with exactly that vchannel.
	_, _, err := m.FenceForSplit(9, "v_split", 7, 200)
	require.ErrorIs(t, err, ErrCollectionNotFound)
	_, _, err = m.FenceForSplit(1, "v_other", 7, 200)
	require.ErrorIs(t, err, ErrCollectionNotFound)
	require.False(t, m.collections[1].SplitFenced())

	segmentIDs, alreadyFenced, err := m.FenceForSplit(1, "v_split", 7, 200)
	require.NoError(t, err)
	require.False(t, alreadyFenced)
	require.Equal(t, []int64{1001}, segmentIDs)
	require.True(t, m.collections[1].SplitFenced())

	// The registration is kept. Only DML and new segments are refused.
	require.NoError(t, m.CheckIfCollectionExists(1))
	require.NoError(t, m.CheckIfPartitionExists(key))
	vchannel, ok := m.CollectionVChannel(1)
	require.True(t, ok)
	require.Equal(t, "v_split", vchannel)
	_, ok = m.CollectionVChannel(9)
	require.False(t, ok)

	_, err = m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 1})
	require.ErrorIs(t, err, ErrShardFenced)
	_, err = m.AssignSegment(&AssignSegmentRequest{
		CollectionID: 1, PartitionID: 2, TimeTick: 300,
		ModifiedMetrics: stats.ModifiedMetrics{Rows: 1, BinarySize: 1},
	})
	require.ErrorIs(t, err, ErrShardFenced)
	_, err = m.WaitUntilGrowingSegmentReady(key)
	require.ErrorIs(t, err, ErrShardFenced)
	require.ErrorIs(t, m.CheckIfSegmentCanBeCreated(key, 2002), ErrShardFenced)
	deleteMsg := message.MustAsMutableDeleteMessageV1(message.NewDeleteMessageBuilderV1().WithVChannel("v_split").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&message.DeleteRequest{}).MustBuildMutable().WithTimeTick(300))
	require.ErrorIs(t, m.ApplyDelete(deleteMsg), ErrShardFenced)

	// A later fence that seals: there is nothing left, and it is still admitted.
	flushed, err := m.FlushAndFenceSegmentAllocUntil(1, 400)
	require.NoError(t, err)
	require.Empty(t, flushed)

	// The same split again changes nothing.
	segmentIDs, alreadyFenced, err = m.FenceForSplit(1, "v_split", 7, 500)
	require.NoError(t, err)
	require.True(t, alreadyFenced)
	require.Empty(t, segmentIDs)

	// Another split is refused, and the fence still belongs to the first.
	_, _, err = m.FenceForSplit(1, "v_split", 8, 600)
	require.ErrorIs(t, err, ErrShardFenced)
	require.Equal(t, int64(7), m.collections[1].SplitTaskID)
}

func TestFenceForSplitWakesAnInsertThatWaitsForASegment(t *testing.T) {
	m, appends := newSplitFenceShardManagerWithAppends(t, 0, context.Canceled)
	key := PartitionUniqueKey{CollectionID: 1, PartitionID: 2}

	// Seal the only segment, so that the next insert starts an allocation
	// whose create segment message never lands.
	_, err := m.FlushAndFenceSegmentAllocUntil(1, 150)
	require.NoError(t, err)
	_, err = m.AssignSegment(&AssignSegmentRequest{
		CollectionID: 1, PartitionID: 2, TimeTick: 160,
		ModifiedMetrics: stats.ModifiedMetrics{Rows: 1, BinarySize: 1},
	})
	require.ErrorIs(t, err, ErrWaitForNewSegment)
	ready, err := m.WaitUntilGrowingSegmentReady(key)
	require.NoError(t, err)
	select {
	case <-ready:
		t.Fatal("the allocation must be pending")
	default:
	}

	_, _, err = m.FenceForSplit(1, "v_split", 7, 200)
	require.NoError(t, err)

	// The waiting insert wakes up, and its next attempt is refused as fenced.
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("an insert that waits for a segment must be woken by the fence")
	}
	_, err = m.WaitUntilGrowingSegmentReady(key)
	require.ErrorIs(t, err, ErrShardFenced)

	// Let the allocation worker reach the WAL before the test ends. It has
	// then allocated its segment id from the resources of this test, and the
	// close of the manager stops it; otherwise it could start after the
	// resources are reset and dereference them.
	select {
	case <-appends:
	case <-time.After(5 * time.Second):
		t.Fatal("the allocation worker never reached the WAL")
	}
}

func TestRecoverShardManagerRestoresTheSplitFence(t *testing.T) {
	m := newSplitFenceShardManager(t, 7, context.Canceled)
	require.True(t, m.collections[1].SplitFenced())
	_, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 1})
	require.ErrorIs(t, err, ErrShardFenced)

	// The copy of the same split is recognized after recovery.
	_, alreadyFenced, err := m.FenceForSplit(1, "v_split", 7, 500)
	require.NoError(t, err)
	require.True(t, alreadyFenced)

	// A nil collection info is not fenced.
	var info *CollectionInfo
	require.False(t, info.SplitFenced())
}

func TestIsSplitFenced(t *testing.T) {
	m := newSplitFenceShardManager(t, 0, context.Canceled)
	require.False(t, IsSplitFenced(m, "v_split"))
	_, _, err := m.FenceForSplit(1, "v_split", 7, 200)
	require.NoError(t, err)
	require.True(t, IsSplitFenced(m, "v_split"))
	// A vchannel that holds no collection on this pchannel is not fenced.
	require.False(t, IsSplitFenced(m, "v_other"))
}
