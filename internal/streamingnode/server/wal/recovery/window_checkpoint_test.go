package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEffectivePersistCheckpointUsesPChannelWindowAndFlushOnly(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}
	snapshot := &RecoverySnapshot{
		Checkpoint:                     testRecoveryCheckpoint(120, 120),
		pchannelWindowSourceCheckpoint: testRecoveryCheckpoint(110, 110),
		vchannelWindowMetaUpdates: map[string]*idempotencyWindowMetaUpdate{
			"v1": {
				meta: &streamingpb.VChannelWindowMeta{
					Pchannel:                    "p1",
					Vchannel:                    "v1",
					ViewType:                    common.VChannelWindowViewTypeIdempotency,
					SnapshotCheckpointMessageId: rmq.NewRmqID(10).IntoProto(),
					SnapshotCheckpointTimetick:  10,
				},
			},
		},
	}

	checkpoint := rs.windowManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(checkpoint.MessageID))
}

// The flusher clamp on the persisted consume checkpoint exists only for window
// replay. With idempotency disabled the checkpoint must not be pinned to the
// slowest vchannel's flusher — that would force every restart to replay the
// whole flusher-to-consume WAL span through recovery for no benefit (WAL
// truncation takes its own min against the flusher separately).
func TestEffectivePersistCheckpointNotFlusherClampedWhenIdempotencyDisabled(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}
	snapshot := &RecoverySnapshot{Checkpoint: testRecoveryCheckpoint(120, 120)}

	checkpoint := rs.windowManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(120).EQ(checkpoint.MessageID))
}

func TestEffectivePersistCheckpointPreservesReplicateAndAlterState(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}

	snapshotCheckpoint := testRecoveryCheckpoint(120, 120)
	snapshotCheckpoint.Magic = 42
	snapshotCheckpoint.ReplicateConfig = &commonpb.ReplicateConfiguration{}
	snapshotCheckpoint.AlterWalState = &streamingpb.AlterWALState{Stage: streamingpb.AlterWALStage_FLUSHING}
	snapshotCheckpoint.ReplicateCheckpoint = &utility.ReplicateCheckpoint{
		ClusterID: "source-cluster",
		PChannel:  "source-p1",
		MessageID: rmq.NewRmqID(118),
		TimeTick:  118,
	}

	snapshot := &RecoverySnapshot{
		Checkpoint:                     snapshotCheckpoint,
		pchannelWindowSourceCheckpoint: testRecoveryCheckpoint(110, 110),
	}

	checkpoint := rs.windowManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	// The consume position is clamped back to the oldest durability bound (flusher at 100)
	// so window/flusher state can still be rebuilt on restart.
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(checkpoint.MessageID))
	// The control-plane metadata belongs to the consume checkpoint and must survive the clamp,
	// otherwise replication config / in-progress AlterWAL state is silently lost on restart.
	require.Equal(t, int64(42), checkpoint.Magic)
	require.NotNil(t, checkpoint.ReplicateConfig)
	require.NotNil(t, checkpoint.AlterWalState)
	require.Equal(t, streamingpb.AlterWALStage_FLUSHING, checkpoint.AlterWalState.Stage)
	require.NotNil(t, checkpoint.ReplicateCheckpoint)
	require.Equal(t, "source-cluster", checkpoint.ReplicateCheckpoint.ClusterID)
	require.Equal(t, uint64(118), checkpoint.ReplicateCheckpoint.TimeTick)
}

func TestEffectivePersistCheckpointUsesPersistedPChannelWindowWhenWindowDirty(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.windowManager.advancePChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(120, 120))
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{
		"v1": {
			dirty: true,
		},
	})

	checkpoint := rs.windowManager.effectivePersistCheckpoint(&RecoverySnapshot{
		Checkpoint: testRecoveryCheckpoint(120, 120),
	}, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(10), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(10).EQ(checkpoint.MessageID))
}

func TestEffectivePersistCheckpointIgnoresPChannelWindowWhenWindowClean(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.windowManager.advancePChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(120, 120))

	checkpoint := rs.windowManager.effectivePersistCheckpoint(&RecoverySnapshot{
		Checkpoint: testRecoveryCheckpoint(120, 120),
	}, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(120).EQ(checkpoint.MessageID))
}

func TestRecoveryCheckpointBecomesDirtyAfterWindowSnapshotPersisted(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	window := newEmptyVChannelWindow("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, window.applyCommittedWriteRecord(*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 120,
		MessageId:      rmq.NewRmqID(120).IntoProto(),
	}), true))
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{"v1": window})
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.windowManager.advancePChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(120, 120))
	rs.checkpoint = testRecoveryCheckpoint(120, 120)
	rs.dirtyCounter = 1

	recoverySnapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, recoverySnapshot)
	effectiveCheckpoint := rs.windowManager.effectivePersistCheckpoint(recoverySnapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(10), effectiveCheckpoint.TimeTick)
	rs.windowManager.markConsumeCheckpointPersisted(effectiveCheckpoint)
	require.False(t, rs.isDirty())

	idempotencySnapshot := rs.windowManager.consumeIdempotencySnapshot()
	require.NotNil(t, idempotencySnapshot)
	rs.windowManager.markVChannelWindowsPersisted(
		idempotencySnapshot.pchannelWindowRecords,
		nil,
		1,
		idempotencySnapshot.pchannelWindowSourceCheckpoint,
	)

	require.True(t, rs.isDirty())
	recoverySnapshot = rs.consumeDirtySnapshot()
	require.NotNil(t, recoverySnapshot)
	effectiveCheckpoint = rs.windowManager.effectivePersistCheckpoint(recoverySnapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), effectiveCheckpoint.TimeTick)
}

func TestFlusherCheckpointIgnoresWindowSnapshotCheckpoint(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))

	byTimeTick := rs.GetFlusherCheckpointByTimeTick(context.Background())
	require.Equal(t, uint64(100), byTimeTick.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(byTimeTick.MessageID))

	byMessageID := rs.getFlusherCheckpoint()
	require.Equal(t, uint64(100), byMessageID.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(byMessageID.MessageID))
}

// WAL truncation must never pass the durable window source checkpoint: that is
// the position a restart rewinds the consume stream to.
func TestSimpleTruncateCheckpointClampedByWindowSnapshotCheckpoint(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, rmq.NewRmqID(10)).Return(nil).Once()
	rs.truncator = truncator

	rs.simpleTruncateCheckpoint(context.Background(), testRecoveryCheckpoint(120, 120))
}

// An idle pchannel takes no window snapshot (only committed write records mark a
// window dirty), so its durable source checkpoint freezes while timeticks keep
// advancing the consume and flusher checkpoints. Truncation must stay clamped to
// the frozen position, otherwise the restart rewind lands outside the WAL.
func TestSimpleTruncateCheckpointClampedWhilePChannelIsIdle(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(200, 200),
		},
	}
	// the last snapshot persisted the source checkpoint at 10...
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	// ...and then only timeticks arrived: the in-memory position moves on, the
	// persisted one stays where the last chunk was written.
	rs.windowManager.advancePChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(200, 200))
	require.Equal(t, uint64(10), rs.windowManager.truncateClampCheckpoint().TimeTick)

	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, rmq.NewRmqID(10)).Return(nil).Once()
	rs.truncator = truncator

	rs.simpleTruncateCheckpoint(context.Background(), testRecoveryCheckpoint(220, 220))
}

// With idempotency disabled there is no window store to replay, so truncation
// keeps taking min(flusher, consume) only.
func TestSimpleTruncateCheckpointNotWindowClampedWhenIdempotencyDisabled(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.vchannels = map[string]*vchannelRecoveryInfo{
		"v1": {
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			},
			flusherCheckpoint: testRecoveryCheckpoint(100, 100),
		},
	}
	rs.windowManager.setPChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, rmq.NewRmqID(100)).Return(nil).Once()
	rs.truncator = truncator

	rs.simpleTruncateCheckpoint(context.Background(), testRecoveryCheckpoint(120, 120))
}

func testRecoveryCheckpoint(messageID int64, timetick uint64) *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: rmq.NewRmqID(messageID),
		TimeTick:  timetick,
	}
}
