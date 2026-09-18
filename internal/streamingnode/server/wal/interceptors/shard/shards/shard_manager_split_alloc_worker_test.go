package shards

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// TestShardManagerFenceStopsTheSourceSegmentAllocWorker: a segment-alloc
// worker the source started before the fence must not outlive the fence. It
// retries its CreateSegment until the append lands, and after the fence that
// append can only ever be refused (or, before the name gate, reach a split
// target registered on the same pchannel). The fence cancels it.
func TestShardManagerFenceStopsTheSourceSegmentAllocWorker(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	appendCtx := make(chan context.Context, 1)
	appends := atomic.NewInt32(0)
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Unavailable().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
			if appends.Inc() == 1 {
				appendCtx <- ctx
			}
			return nil, errors.New("wal busy")
		}).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	m := RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test_alloc_worker_channel", Term: 1},
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v1": newTestVChannelMeta("v1", 1, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0),
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &mockedTxnManager{},
	})
	t.Cleanup(m.Close)
	impl := m.(*shardManagerImpl)

	key := PartitionUniqueKey{CollectionID: 1, PartitionID: 2}
	impl.mu.Lock()
	impl.partitionManagers[key].asyncAllocSegment(0, false)
	impl.mu.Unlock()

	var ctx context.Context
	select {
	case ctx = <-appendCtx:
	case <-time.After(5 * time.Second):
		t.Fatal("the alloc worker never tried to append")
	}
	require.NoError(t, ctx.Err(), "the worker runs while the vchannel is live")

	fenceSourceForTest(m, "v1", 1, 2000)

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the fence must cancel the source's segment-alloc worker")
	}
	// the worker has stopped: no further append attempts.
	settled := appends.Load()
	time.Sleep(1200 * time.Millisecond)
	assert.Equal(t, settled, appends.Load(), "a canceled alloc worker must stop retrying")
}

// fenceSourceForTest applies the source replica of a split to the shard
// manager, as the interceptor does for a first fence.
func fenceSourceForTest(m ShardManager, vchannel string, collectionID int64, timetick uint64) {
	m.SplitShard(newTestSplitShardImmutableMessage(vchannel, collectionID, timetick))
}

// TestShardManagerSegmentApplyIgnoresAVChannelItDoesNotHold: CreateSegment and
// FlushSegment look the partition manager up by (collection, partition). After
// a fence a target of the same collection holds that key on this pchannel, so
// the apply must check the vchannel name too: neither message of the fenced
// source may reach the target's partition manager, and neither may panic.
func TestShardManagerSegmentApplyIgnoresAVChannelItDoesNotHold(t *testing.T) {
	m := newTestShardManagerFromSnapshot(t,
		map[string]*streamingpb.VChannelMeta{
			"v0": newTestVChannelMeta("v0", 1, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 150),
			"v1": newTestVChannelMeta("v1", 1, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0),
		}, nil)
	t.Cleanup(m.Close)
	key := PartitionUniqueKey{CollectionID: 1, PartitionID: 2}

	createSegment := func(vchannel string, segmentID int64) message.ImmutableCreateSegmentMessageV2 {
		msg := message.NewCreateSegmentMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 2, SegmentId: segmentID}).
			WithBody(&message.CreateSegmentMessageBody{}).
			MustBuildMutable().
			WithTimeTick(2000).
			WithLastConfirmedUseMessageID()
		return message.MustAsImmutableCreateSegmentMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
	}
	flush := func(vchannel string, segmentID int64) message.ImmutableFlushMessageV2 {
		msg := message.NewFlushMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.FlushMessageHeader{CollectionId: 1, PartitionId: 2, SegmentId: segmentID}).
			WithBody(&message.FlushMessageBody{}).
			MustBuildMutable().
			WithTimeTick(2100).
			WithLastConfirmedUseMessageID()
		return message.MustAsImmutableFlushMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(4)))
	}

	// the target's partition manager is idle: a CreateSegment reaching it would
	// panic on onAllocating == nil.
	require.NotPanics(t, func() { m.CreateSegment(createSegment("v0", 2000)) })
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(key, 2000), ErrSegmentNotFound)

	// the target's own segment exists and is sealed: a Flush addressed to the
	// fenced source must not remove it.
	impl := m.(*shardManagerImpl)
	impl.mu.Lock()
	impl.partitionManagers[key].onAllocating = make(chan struct{})
	impl.mu.Unlock()
	m.CreateSegment(createSegment("v1", 3000))
	impl.mu.Lock()
	impl.partitionManagers[key].segments[3000].Flush(policy.PolicyCapacity())
	impl.mu.Unlock()
	require.NoError(t, m.CheckIfSegmentCanBeFlushed(key, 3000))

	m.FlushSegment(flush("v0", 3000))
	assert.NoError(t, m.CheckIfSegmentCanBeFlushed(key, 3000), "a Flush of the fenced source must not reach the target")
	m.FlushSegment(flush("v1", 3000))
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(key, 3000), ErrSegmentNotFound)
}
