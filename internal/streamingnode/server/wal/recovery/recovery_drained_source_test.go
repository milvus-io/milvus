package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newDrainedSourceTestVChannel builds one vchannel of the recovery storage as
// the AlterWAL tests need it: its state, the fence ticks a SPLITTED source
// carries (split_time_tick and the checkpoint tick the fence record left), and
// the flusher checkpoint DataCoord last acked for it (nil when none yet).
func newDrainedSourceTestVChannel(name string, state streamingpb.VChannelState, splitTimeTick, checkpointTimeTick uint64, flusherCheckpoint *WALCheckpoint) *vchannelRecoveryInfo {
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:           name,
			State:              state,
			SplitTimeTick:      splitTimeTick,
			CheckpointTimeTick: checkpointTimeTick,
			CollectionInfo:     &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
		flusherCheckpoint: flusherCheckpoint,
	}
}

func newDrainedSourceTestRecoveryStorage(vchannels map[string]*vchannelRecoveryInfo) *recoveryStorageImpl {
	channel := types.PChannelInfo{Name: "drained-source-channel", Term: 1}
	rs := newRecoveryStorage(channel, &WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 1000})
	rs.vchannels = vchannels
	return rs
}

// TestAlterWALWaitSkipsAClosedSplitSource: "AlterWAL with a closed split
// source". The source's data sync service was closed once its acked
// checkpoint passed the fence gate, freezing its flusher checkpoint at 120 for
// as long as the split takes to be adopted -- hours. An AlterWAL at tick 140
// then waits in handleAlterWALFlushingStage until GetFlusherCheckpointByTimeTick
// reaches 140. Nothing will ever move the source's checkpoint again, so if the
// source takes part in that minimum the switch never completes, the RW WAL
// never reopens, and the adoption can never be appended to this pchannel.
//
// A source whose checkpoint has passed its fence gate has every byte it will
// ever hold persisted and every segment sealed: it contributes nothing to the
// switch and must not be waited on.
func TestAlterWALWaitSkipsAClosedSplitSource(t *testing.T) {
	rs := newDrainedSourceTestRecoveryStorage(map[string]*vchannelRecoveryInfo{
		"live": newDrainedSourceTestVChannel("live", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0, 0,
			&WALCheckpoint{MessageID: rmq.NewRmqID(15), TimeTick: 150}),
		// Fenced at 100 by a fence record at 100; the service closed once its
		// checkpoint reached 120 >= 100 and the checkpoint froze there.
		"source": newDrainedSourceTestVChannel("source", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 100, 100,
			&WALCheckpoint{MessageID: rmq.NewRmqID(12), TimeTick: 120}),
	})

	cp := rs.GetFlusherCheckpointByTimeTick(context.Background())
	require.NotNil(t, cp)
	assert.Equal(t, uint64(150), cp.TimeTick, "the frozen checkpoint of a drained split source must not bound the AlterWAL wait")
}

// TestAlterWALWaitStillWaitsForAnUndrainedSplitSource: a SPLITTED source whose
// checkpoint has NOT reached its fence gate still has data to flush -- or a
// seal record still to consume -- and stays in the minimum, in every shape the
// gate can take.
func TestAlterWALWaitStillWaitsForAnUndrainedSplitSource(t *testing.T) {
	live := func() *vchannelRecoveryInfo {
		return newDrainedSourceTestVChannel("live", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0, 0,
			&WALCheckpoint{MessageID: rmq.NewRmqID(15), TimeTick: 150})
	}

	t.Run("checkpoint before the fence", func(t *testing.T) {
		rs := newDrainedSourceTestRecoveryStorage(map[string]*vchannelRecoveryInfo{
			"live": live(),
			"source": newDrainedSourceTestVChannel("source", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 100, 100,
				&WALCheckpoint{MessageID: rmq.NewRmqID(9), TimeTick: 90}),
		})
		cp := rs.GetFlusherCheckpointByTimeTick(context.Background())
		require.NotNil(t, cp)
		assert.Equal(t, uint64(90), cp.TimeTick)
	})

	t.Run("checkpoint past T_switch but before the seal record of a re-driven fence", func(t *testing.T) {
		// The first fence append failed at T_switch=100 without persisting;
		// the re-drive at 110 is the only seal record in the WAL and the
		// checkpoint tick of the meta is that record's own tick. A checkpoint
		// at 105 proves the data is persisted but not that the data sync
		// service consumed the seal record: its segments may still be
		// growing, so the source is still waited on.
		rs := newDrainedSourceTestRecoveryStorage(map[string]*vchannelRecoveryInfo{
			"live": live(),
			"source": newDrainedSourceTestVChannel("source", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 100, 110,
				&WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 105}),
		})
		cp := rs.GetFlusherCheckpointByTimeTick(context.Background())
		require.NotNil(t, cp)
		assert.Equal(t, uint64(105), cp.TimeTick)
	})

	t.Run("no checkpoint yet", func(t *testing.T) {
		// A source rebuilt after a restart has no checkpoint until its first
		// ack; whether it is drained is not known yet, so the minimum is not
		// ready, exactly as for any other vchannel.
		rs := newDrainedSourceTestRecoveryStorage(map[string]*vchannelRecoveryInfo{
			"live":   live(),
			"source": newDrainedSourceTestVChannel("source", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 100, 100, nil),
		})
		assert.Nil(t, rs.GetFlusherCheckpointByTimeTick(context.Background()))
	})
}

// TestAlterWALWaitWithOnlyDrainedSplitSourcesIsTheConsumeCheckpoint: when every
// vchannel of the pchannel is a drained split source there is nothing left to
// flush, which is the same situation as a pchannel without any vchannel, and
// answered the same way: the recovery storage's own checkpoint.
func TestAlterWALWaitWithOnlyDrainedSplitSourcesIsTheConsumeCheckpoint(t *testing.T) {
	rs := newDrainedSourceTestRecoveryStorage(map[string]*vchannelRecoveryInfo{
		"source": newDrainedSourceTestVChannel("source", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 100, 100,
			&WALCheckpoint{MessageID: rmq.NewRmqID(12), TimeTick: 120}),
	})
	cp := rs.GetFlusherCheckpointByTimeTick(context.Background())
	require.NotNil(t, cp)
	assert.Equal(t, uint64(1000), cp.TimeTick)
}

// TestSplitFenceGateMirrorsTheFlusherCloseGate: the tick a source must drain
// past is the one the flusher closes its data sync service on. Observed live,
// that is the first fence record's own tick (RecordFence sees the same
// record); reloaded from the catalog, it is reseeded the same way the flusher
// reseeds its close gate, from split_time_tick and the checkpoint tick.
func TestSplitFenceGateMirrorsTheFlusherCloseGate(t *testing.T) {
	t.Run("reloaded", func(t *testing.T) {
		infos := newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
			{Vchannel: "normal", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 500},
			// T_switch 100 reported by a re-drive at 110, whose tick the
			// checkpoint kept: the seal record is at 110.
			{Vchannel: "redriven", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, SplitTimeTick: 100, CheckpointTimeTick: 110},
			// A meta whose checkpoint never moved past the fence.
			{Vchannel: "plain", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, SplitTimeTick: 100, CheckpointTimeTick: 100},
			{Vchannel: "dropped", State: streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, SplitTimeTick: 100, CheckpointTimeTick: 900},
		})
		assert.Equal(t, uint64(0), infos["normal"].fenceGate())
		assert.Equal(t, uint64(110), infos["redriven"].fenceGate())
		assert.Equal(t, uint64(100), infos["plain"].fenceGate())
		assert.Equal(t, uint64(0), infos["dropped"].fenceGate())
		assert.False(t, infos["normal"].DrainedPastFence(), "only a SPLITTED source can be drained")
		assert.False(t, infos["dropped"].DrainedPastFence(), "only a SPLITTED source can be drained")
	})

	t.Run("observed", func(t *testing.T) {
		info := newDrainedSourceTestVChannel("v1", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0, 0, nil)
		// A re-drive at 110 reporting T_switch=100 in its extra (see
		// TestObserveSplitShardKeepsTheFirstFenceTick for the extra itself):
		// the gate is the record's own tick, T_switch stays what it reports.
		info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 110))
		assert.Equal(t, uint64(110), info.fenceGate())
		assert.False(t, info.DrainedPastFence(), "no checkpoint yet")

		// A same-task re-fence at a later tick moves neither the state nor
		// the gate, exactly as RecordFence keeps the first record's tick.
		info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 130))
		assert.Equal(t, uint64(110), info.fenceGate())

		require.NoError(t, info.UpdateFlushCheckpoint(&WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 109}))
		assert.False(t, info.DrainedPastFence())
		require.NoError(t, info.UpdateFlushCheckpoint(&WALCheckpoint{MessageID: rmq.NewRmqID(11), TimeTick: 110}))
		assert.True(t, info.DrainedPastFence())

		// A DDL replica appended to the source with no effect after the
		// service closed bumps the checkpoint tick of the meta past the frozen
		// flusher checkpoint; the gate does not follow it, or the source would
		// stop counting as drained although nothing will move its checkpoint
		// again.
		info.meta.CheckpointTimeTick = 200
		assert.Equal(t, uint64(110), info.fenceGate())
		assert.True(t, info.DrainedPastFence())
	})
}

// newTwoSplitRecoveryStorage builds the Medium-3 shape: source A was closed
// at a and is still waiting for its adoption; source B was fenced at b > a,
// adopted, retired, and its own checkpoint has passed its fence. A third
// vchannel of an unrelated collection has no flusher checkpoint yet (its data
// sync service was just rebuilt). It wires the catalog mock so a persist
// round can actually run, recording what it wrote; the mixcoord mock has no
// DropVirtualChannel expectation, so reaching DataCoord fails the test.
func newTwoSplitRecoveryStorage(t *testing.T, withUnrelatedNilCheckpoint bool) (rs *recoveryStorageImpl, persisted map[string]*streamingpb.VChannelMeta, truncatedTo *message.MessageID) {
	persisted = make(map[string]*streamingpb.VChannelMeta)
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, snapshot *metastore.WALRecoverySnapshot) error {
			for k, v := range snapshot.VChannels {
				persisted[k] = v
			}
			return nil
		}).Maybe()
	mixCoord := mocks.NewMockMixCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(mixCoord)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog), resource.OptMixCoordClient(f))

	var truncated message.MessageID
	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id message.MessageID) error {
		truncated = id
		return nil
	}).Maybe()

	channel := types.PChannelInfo{Name: "two-split-pchannel"}
	rs = newRecoveryStorage(channel, &WALCheckpoint{MessageID: rmq.NewRmqID(50), TimeTick: 5000})
	rs.segments = map[int64]*segmentRecoveryInfo{}
	rs.vchannels = map[string]*vchannelRecoveryInfo{}
	rs.truncator = truncator
	rs.dirtyCounter = 1 // force the first consumeDirtySnapshot to run.

	a := newDrainedSourceTestVChannel("a", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 1000, 1000,
		&WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 1000})
	b := newDrainedSourceTestVChannel("b", streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 2000, 2000,
		&WALCheckpoint{MessageID: rmq.NewRmqID(20), TimeTick: 2000})
	b.meta.Retired = true
	rs.vchannels["a"] = a
	rs.vchannels["b"] = b
	rs.retiredVChannels["b"] = struct{}{}
	if withUnrelatedNilCheckpoint {
		rs.vchannels["c"] = newDrainedSourceTestVChannel("c", streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0, 0, nil)
	}
	return rs, persisted, &truncated
}

// TestRetiredSourceIsCollectedByItsOwnFlusherCheckpoint: with two splits on
// one pchannel, the retired source B is collected as soon as ITS OWN flusher
// checkpoint has passed its fence, not once the pchannel-wide minimum has --
// that minimum is A's frozen checkpoint, which stays below B's fence until A
// is adopted, and A's adoption may be hours away or never come. A is left
// exactly where it is: not retired, so not collectable.
func TestRetiredSourceIsCollectedByItsOwnFlusherCheckpoint(t *testing.T) {
	rs, persisted, _ := newTwoSplitRecoveryStorage(t, false)

	assert.True(t, rs.hasCollectableRetiredVChannelLocked(), "B has drained past its own fence and is retired")
	require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel))

	_, ok := rs.vchannels["b"]
	assert.False(t, ok, "B must be collected although A's frozen checkpoint is still the pchannel minimum")
	require.Contains(t, persisted, "b")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, persisted["b"].State)
	assert.True(t, persisted["b"].Retired)
	_, ok = rs.vchannels["a"]
	assert.True(t, ok, "A is not retired and stays")
	assert.False(t, rs.hasCollectableRetiredVChannelLocked())
}

// TestRetiredSourceCollectionIgnoresAnUnrelatedVChannelWithoutCheckpoint: a
// vchannel that has no flusher checkpoint yet used to make the pchannel-wide
// minimum unknown and pause every collection on the pchannel; it says nothing
// about whether B has drained.
func TestRetiredSourceCollectionIgnoresAnUnrelatedVChannelWithoutCheckpoint(t *testing.T) {
	rs, persisted, _ := newTwoSplitRecoveryStorage(t, true)

	assert.True(t, rs.hasCollectableRetiredVChannelLocked())
	require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel))
	_, ok := rs.vchannels["b"]
	assert.False(t, ok)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, persisted["b"].State)
}

// TestTruncationStillFollowsThePChannelWideMinimum: collection is judged per
// vchannel, but WAL truncation is not. A's frozen checkpoint is still the
// position the flusher rebuilds A from on restart, so the WAL is truncated no
// further than it; and while some vchannel has no checkpoint at all the bound
// is unknown and nothing is truncated.
func TestTruncationStillFollowsThePChannelWideMinimum(t *testing.T) {
	t.Run("bounded by the oldest frozen source", func(t *testing.T) {
		rs, _, truncatedTo := newTwoSplitRecoveryStorage(t, false)
		require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel))
		require.NotNil(t, *truncatedTo)
		assert.True(t, (*truncatedTo).EQ(rmq.NewRmqID(10)), "truncated to A's frozen checkpoint, not past it")
	})
	t.Run("unknown while a vchannel has no checkpoint", func(t *testing.T) {
		rs, _, truncatedTo := newTwoSplitRecoveryStorage(t, true)
		require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel))
		assert.Nil(t, *truncatedTo)
	})
}
