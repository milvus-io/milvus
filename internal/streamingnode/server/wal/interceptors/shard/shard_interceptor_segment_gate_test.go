package shard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// newTestCreateSegmentMutableMessage is the CreateSegment a partition manager's
// segment-alloc worker appends for (collection, partition) on vchannel.
func newTestCreateSegmentMutableMessage(vchannel string, collectionID int64, partitionID int64, segmentID int64) message.MutableMessage {
	return message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			SegmentId:    segmentID,
			Level:        datapb.SegmentLevel_L1,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(2000).
		WithLastConfirmedUseMessageID()
}

// newTestFlushMutableMessage is the Flush a segment flush worker appends.
func newTestFlushMutableMessage(vchannel string, collectionID int64, partitionID int64, segmentID int64) message.MutableMessage {
	return message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			SegmentId:    segmentID,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(2000).
		WithLastConfirmedUseMessageID()
}

// TestSegmentGateLateCreateSegmentFromAFencedSourceDoesNotTouchTheSplitTarget:
// the fence releases the source's partition managers, but a segment-alloc
// worker the source had already started keeps appending its CreateSegment. The
// split target of the same collection is registered on the same pchannel with
// the same partition, so a check keyed by (collection, partition) accepts that
// late message for the TARGET: with the target idle AddSegment panics
// ("onAllocating is nil"); with the target allocating the source's segment is
// adopted as the target's. The name gate refuses it as SHARD_FENCED, which the
// worker treats as terminal.
func TestSegmentGateLateCreateSegmentFromAFencedSourceDoesNotTouchTheSplitTarget(t *testing.T) {
	key := shards.PartitionUniqueKey{CollectionID: 1, PartitionID: 2}

	t.Run("target idle", func(t *testing.T) {
		m := newTestShardManagerWithSplitTarget(t)
		i := newTestShardInterceptorWithManager(t, m)

		var err error
		var appended message.MutableMessage
		require.NotPanics(t, func() {
			_, appended, _, err = appendAndCapture(t, i, newTestCreateSegmentMutableMessage("v0", 1, 2, 2000))
		})
		assert.Nil(t, appended, "a CreateSegment from a fenced source must not be appended")
		assert.True(t, status.AsStreamingError(err).IsShardFenced())
		assert.True(t, status.AsStreamingError(err).IsUnrecoverable(), "the alloc worker must stop on it")
		assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(key, 2000), shards.ErrSegmentNotFound)
		assertTargetUntouched(t, m)
	})

	t.Run("target allocating", func(t *testing.T) {
		m, allocs := newTestShardManagerWithSplitTargetAllocs(t, nil)
		i := newTestShardInterceptorWithManager(t, m)

		// Put the target's partition manager on allocating: it holds no growing
		// segment, so the first assignment asks for a new one.
		_, err := m.AssignSegment(&shards.AssignSegmentRequest{
			CollectionID:    1,
			PartitionID:     2,
			ModifiedMetrics: stats.ModifiedMetrics{Rows: 1, BinarySize: 1000},
			TimeTick:        3000,
		})
		require.ErrorIs(t, err, shards.ErrWaitForNewSegment)
		// The target's own worker appends straight to the mock WAL, so its
		// CreateSegment never reaches the manager and the target stays allocating.
		waitForSegmentAllocWorker(t, allocs, "v1")
		ready, err := m.WaitUntilGrowingSegmentReady(key)
		require.NoError(t, err)

		_, appended, _, err := appendAndCapture(t, i, newTestCreateSegmentMutableMessage("v0", 1, 2, 2000))
		assert.Nil(t, appended)
		assert.True(t, status.AsStreamingError(err).IsShardFenced())
		assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(key, 2000), shards.ErrSegmentNotFound,
			"the source's segment must not be adopted by the target")
		select {
		case <-ready:
			t.Fatal("the target's pending allocation must not be completed by the source's segment")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("never held", func(t *testing.T) {
		m := newTestShardManagerWithSplitTarget(t)
		i := newTestShardInterceptorWithManager(t, m)

		_, appended, _, err := appendAndCapture(t, i, newTestCreateSegmentMutableMessage("v-stale", 1, 2, 2000))
		assert.Nil(t, appended)
		assert.False(t, status.AsStreamingError(err).IsShardFenced())
		assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
		assertTargetUntouched(t, m)
	})
}

// TestSegmentGateLateFlushFromAFencedSourceIsRefused: a flush worker of the
// source is in the same position as its alloc worker. Its segment was sealed by
// the fence record itself, so the late Flush has nothing left to do; it is
// refused by name and never reaches the target.
func TestSegmentGateLateFlushFromAFencedSourceIsRefused(t *testing.T) {
	m := newTestShardManagerWithSplitTarget(t)
	i := newTestShardInterceptorWithManager(t, m)

	for _, segmentID := range []int64{1000, 3000} {
		_, appended, _, err := appendAndCapture(t, i, newTestFlushMutableMessage("v0", 1, 2, segmentID))
		assert.Nil(t, appended)
		assert.True(t, status.AsStreamingError(err).IsShardFenced())
		assertTargetUntouched(t, m)
	}
}

// TestSegmentGateHeldVChannelIsUnchanged: the target's own CreateSegment and
// Flush still pass the gate.
func TestSegmentGateHeldVChannelIsUnchanged(t *testing.T) {
	key := shards.PartitionUniqueKey{CollectionID: 1, PartitionID: 2}
	m, allocs := newTestShardManagerWithSplitTargetAllocs(t, nil)
	i := newTestShardInterceptorWithManager(t, m)

	_, err := m.AssignSegment(&shards.AssignSegmentRequest{
		CollectionID:    1,
		PartitionID:     2,
		ModifiedMetrics: stats.ModifiedMetrics{Rows: 1, BinarySize: 1000},
		TimeTick:        3000,
	})
	require.ErrorIs(t, err, shards.ErrWaitForNewSegment)
	waitForSegmentAllocWorker(t, allocs, "v1")

	_, appended, _, err := appendAndCapture(t, i, newTestCreateSegmentMutableMessage("v1", 1, 2, 2000))
	require.NoError(t, err)
	require.NotNil(t, appended)
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(key, 2000), shards.ErrSegmentOnGrowing)

	// a growing segment cannot be flushed by a Flush message: the handler's own
	// check still answers, the gate lets it through.
	_, appended, _, err = appendAndCapture(t, i, newTestFlushMutableMessage("v1", 1, 2, 2000))
	assert.Nil(t, appended)
	assert.False(t, status.AsStreamingError(err).IsShardFenced())
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

// TestSegmentGateOldArchFlushIsExempt: a flush from the old architecture is not
// managed by the shard manager, so the gate does not ask about its vchannel and
// it is appended as it always was.
func TestSegmentGateOldArchFlushIsExempt(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)

	ctx := utility.WithFlushFromOldArch(context.Background())
	var appended message.MutableMessage
	msgID, err := i.DoAppend(ctx, newTestFlushMutableMessage("v0", 1, 2, 1000),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = msg
			return rmq.NewRmqID(1), nil
		})
	require.NoError(t, err)
	require.NotNil(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	shardManager.AssertNotCalled(t, "CheckIfVChannelCanBeWritten", mock.Anything, mock.Anything)
}
