package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newLiveSourceSnapshot is the recovery snapshot of a pchannel holding the
// source "v0" of collection 1 live, with growing segment 1000 in partition 2.
func newLiveSourceSnapshot() *recovery.RecoverySnapshot {
	return &recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			"v0": {
				Vchannel: "v0",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
					Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 2}},
				},
			},
		},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1000: {
				CollectionId: 1,
				PartitionId:  2,
				SegmentId:    1000,
				Vchannel:     "v0",
				State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				Stat: &streamingpb.SegmentAssignmentStat{
					MaxBinarySize:         1 << 20,
					CreateSegmentTimeTick: 50,
				},
			},
		},
		Checkpoint: &recovery.WALCheckpoint{TimeTick: 60},
	}
}

// initFenceOrderTestResource initializes the node-wide resources once per
// test: re-initializing them while a shard manager is registered would lose
// its seal operator registration.
func initFenceOrderTestResource(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
}

// recoverTestShardManager recovers a real shard manager from snapshot, as a
// StreamingNode does when it opens the WAL. The caller closes it.
func recoverTestShardManager(t *testing.T, snapshot *recovery.RecoverySnapshot) shards.ShardManager {
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Unavailable().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1000,
	}, nil).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)
	return shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            types.PChannelInfo{Name: "test_fence_order_channel", Term: 1},
		WAL:                    f,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             recoveredTxnManager{},
	})
}

// newTestSourceFenceMessage is the source replica of split task 42 on "v0",
// with the tick the time tick interceptor assigned to this append attempt.
func newTestSourceFenceMessage(timetick uint64) message.MutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithVChannel("v0").
		WithHeader(newTestSplitShardHeader(1, 42, "v0", "v1", "v2")).
		WithBody(&message.SplitShardMessageBody{Genesis: &msgpb.CreateCollectionRequest{}}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
}

func newTestInsertAt(vchannel string, timetick uint64) message.MutableMessage {
	msg := newTestInsertMutableMessage(vchannel, 1, 2)
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(message.MustAsMutableInsertMessageV1(msg).Header()).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(timetick)
}

// TestSplitShardFenceIsKeptWhenTheAppendReportsAnError: the WAL adaptor returns
// a canceled or expired context, or a fenced WAL term, as it gets it -- after
// the backend may have written the record. The fence must hold whatever the
// append returned: DML after the fence tick is refused, and the broadcaster's
// re-drive is a re-fence reporting the FIRST attempt's tick on both ack paths,
// so DataCoord, the recovery storage (which reads the stamped record) and a
// restart all agree on T_switch = 100. Before the fix the fence was installed
// only after a successful append, so DML continued after 100 and the re-drive
// became a first fence at 200 while a persisted record said 100.
func TestSplitShardFenceIsKeptWhenTheAppendReportsAnError(t *testing.T) {
	for name, appendErr := range map[string]error{
		"deadline exceeded, possibly persisted": context.DeadlineExceeded,
		"canceled, possibly persisted":          context.Canceled,
		"wal term fenced":                       walimpls.ErrFenced,
		"definite failure":                      errors.New("backend refused the write"),
	} {
		t.Run(name, func(t *testing.T) {
			initFenceOrderTestResource(t)
			m := newLiveSourceShardManagerForFenceOrder(t)
			t.Cleanup(m.Close)
			i := newTestShardInterceptorWithManager(t, m)

			ctx, extra := newSplitAppendContext()
			var first message.MutableMessage
			_, err := i.DoAppend(ctx, newTestSourceFenceMessage(100),
				func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
					first = msg
					return nil, appendErr
				})
			require.ErrorIs(t, err, appendErr)
			require.NotNil(t, first)
			// what a persisted first record would carry: T_switch 100 and the seal.
			assertSplitSwitchTimeTickReported(t, extra, first, 100)
			assert.Equal(t, []int64{1000}, message.MustAsMutableSplitShardMessageV2(first).Header().GetFlushedSegmentIds())

			// the fence holds.
			assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v0"), shards.ErrVChannelFenced)
			assert.Equal(t, shards.SplitFence{TimeTick: 100, TaskID: 42}, m.GetSplitFence(1, "v0"))

			// DML after the fence tick is refused, never appended.
			for dmlName, dml := range map[string]message.MutableMessage{
				"Insert": newTestInsertAt("v0", 150),
				"Delete": newTestDeleteMutableMessage("v0", 1),
			} {
				_, appended, _, err := appendAndCapture(t, i, dml)
				assert.Nil(t, appended, dmlName)
				assert.True(t, status.AsStreamingError(err).IsShardFenced(), dmlName)
			}

			// the re-drive at a later tick reports the first attempt's tick.
			ctx, extra = newSplitAppendContext()
			var redrive message.MutableMessage
			_, err = i.DoAppend(ctx, newTestSourceFenceMessage(200),
				func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
					redrive = msg
					return rmq.NewRmqID(2), nil
				})
			require.NoError(t, err)
			require.NotNil(t, redrive)
			assert.Equal(t, uint64(200), redrive.TimeTick())
			assertSplitSwitchTimeTickReported(t, extra, redrive, 100)
			assert.Equal(t, shards.SplitFence{TimeTick: 100, TaskID: 42}, m.GetSplitFence(1, "v0"))
		})
	}
}

// TestSplitShardRestartAfterAFailedFenceAppendIsAFirstFence: the fence of a
// failed append lives only in memory. If the StreamingNode restarts before the
// re-drive and the record never persisted, the recovered state is the live
// source again (no SPLITTED meta, the segment still growing), no consumer ever
// saw tick 100, and the re-drive is a genuine first fence at its own tick that
// seals the segment again and reports that tick.
func TestSplitShardRestartAfterAFailedFenceAppendIsAFirstFence(t *testing.T) {
	initFenceOrderTestResource(t)
	m := newLiveSourceShardManagerForFenceOrder(t)
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ShardManager: m})
	failedCtx, _ := newSplitAppendContext()
	_, err := i.DoAppend(failedCtx, newTestSourceFenceMessage(100),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("backend refused the write")
		})
	require.Error(t, err)
	require.Equal(t, uint64(100), m.GetSplitFence(1, "v0").TimeTick)

	// restart: the node goes down with the fence in memory, and nothing
	// persisted, so it recovers from the same snapshot.
	i.Close()
	m.Close()
	restarted := newLiveSourceShardManagerForFenceOrder(t)
	t.Cleanup(restarted.Close)
	ri := newTestShardInterceptorWithManager(t, restarted)
	assert.NoError(t, restarted.CheckIfVChannelCanBeWritten(1, "v0"))
	assert.Zero(t, restarted.GetSplitFence(1, "v0").TimeTick)

	ctx, extra := newSplitAppendContext()
	var redrive message.MutableMessage
	_, err = ri.DoAppend(ctx, newTestSourceFenceMessage(300),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			redrive = msg
			return rmq.NewRmqID(3), nil
		})
	require.NoError(t, err)
	require.NotNil(t, redrive)
	assertSplitSwitchTimeTickReported(t, extra, redrive, 300)
	assert.Equal(t, []int64{1000}, message.MustAsMutableSplitShardMessageV2(redrive).Header().GetFlushedSegmentIds(),
		"a genuine first fence seals the growing segment again")
	assert.Equal(t, shards.SplitFence{TimeTick: 300, TaskID: 42}, restarted.GetSplitFence(1, "v0"))
}

func newLiveSourceShardManagerForFenceOrder(t *testing.T) shards.ShardManager {
	return recoverTestShardManager(t, newLiveSourceSnapshot())
}
