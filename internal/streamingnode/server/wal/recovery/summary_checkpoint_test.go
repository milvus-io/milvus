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

func enableRecoveryIdempotency(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true"))
	t.Cleanup(func() { _ = params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })
}

func TestEffectivePersistCheckpointUsesPChannelSummaryAndFlushOnly(t *testing.T) {
	enableRecoveryIdempotency(t)
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
		Checkpoint:                      testRecoveryCheckpoint(120, 120),
		pchannelSummarySourceCheckpoint: testRecoveryCheckpoint(110, 110),
		vchannelSummaryMetaUpdates: map[string]*summaryMetaUpdate{
			"v1": {
				meta: &streamingpb.VChannelSummaryMeta{
					Pchannel:                   "p1",
					Vchannel:                   "v1",
					ViewType:                   common.VChannelSummaryViewTypeIdempotency,
					SnapshotCheckpointTimetick: 10,
				},
			},
		},
	}

	checkpoint := rs.summaryManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(checkpoint.MessageID))
}

// The flusher clamp on the persisted consume checkpoint exists only for summary
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

	checkpoint := rs.summaryManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(120).EQ(checkpoint.MessageID))
}

func TestEffectivePersistCheckpointPreservesReplicateAndAlterState(t *testing.T) {
	enableRecoveryIdempotency(t)
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
		Checkpoint:                      snapshotCheckpoint,
		pchannelSummarySourceCheckpoint: testRecoveryCheckpoint(110, 110),
	}

	checkpoint := rs.summaryManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	// The consume position is clamped back to the oldest durability bound (flusher at 100)
	// so summary/flusher state can still be rebuilt on restart.
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

func TestEffectivePersistCheckpointUsesPersistedPChannelSummaryWhenSummaryDirty(t *testing.T) {
	enableRecoveryIdempotency(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(120, 120))
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{
		"v1": {
			dirty: true,
		},
	})

	checkpoint := rs.summaryManager.effectivePersistCheckpoint(&RecoverySnapshot{
		Checkpoint: testRecoveryCheckpoint(120, 120),
	}, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(10), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(10).EQ(checkpoint.MessageID))
}

func TestEffectivePersistCheckpointIgnoresPChannelSummaryWhenSummaryClean(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(1, 1))
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(120, 120))

	checkpoint := rs.summaryManager.effectivePersistCheckpoint(&RecoverySnapshot{
		Checkpoint: testRecoveryCheckpoint(120, 120),
	}, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), checkpoint.TimeTick)
	require.True(t, rmq.NewRmqID(120).EQ(checkpoint.MessageID))
}

func TestRecoveryCheckpointBecomesDirtyAfterSummarySnapshotPersisted(t *testing.T) {
	enableRecoveryIdempotency(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	summary := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, summary.applyCommittedWriteRecord(committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 120,
		MessageId:      rmq.NewRmqID(120).IntoProto(),
	}), true))
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{"v1": summary})
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(120, 120))
	rs.checkpoint = testRecoveryCheckpoint(120, 120)
	rs.dirtyCounter = 1

	recoverySnapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, recoverySnapshot)
	effectiveCheckpoint := rs.summaryManager.effectivePersistCheckpoint(recoverySnapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(10), effectiveCheckpoint.TimeTick)
	rs.summaryManager.markConsumeCheckpointPersisted(effectiveCheckpoint)
	require.False(t, rs.isDirty())

	idempotencySnapshot := rs.summaryManager.consumeIdempotencySnapshot()
	require.NotNil(t, idempotencySnapshot)
	rs.summaryManager.markVChannelSummariesPersisted(
		idempotencySnapshot.pchannelSummaryRecords,
		nil,
		1,
		idempotencySnapshot.pchannelSummarySourceCheckpoint,
	)

	require.True(t, rs.isDirty())
	recoverySnapshot = rs.consumeDirtySnapshot()
	require.NotNil(t, recoverySnapshot)
	effectiveCheckpoint = rs.summaryManager.effectivePersistCheckpoint(recoverySnapshot, rs.getFlusherCheckpoint())
	require.Equal(t, uint64(120), effectiveCheckpoint.TimeTick)
}

func TestFlusherCheckpointIgnoresSummarySnapshotCheckpoint(t *testing.T) {
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
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))

	byTimeTick := rs.GetFlusherCheckpointByTimeTick(context.Background())
	require.Equal(t, uint64(100), byTimeTick.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(byTimeTick.MessageID))

	byMessageID := rs.getFlusherCheckpoint()
	require.Equal(t, uint64(100), byMessageID.TimeTick)
	require.True(t, rmq.NewRmqID(100).EQ(byMessageID.MessageID))
}

// WAL truncation must never pass the durable summary source checkpoint: that is
// the position a restart rewinds the consume stream to.
func TestSimpleTruncateCheckpointClampedBySummarySnapshotCheckpoint(t *testing.T) {
	enableRecoveryIdempotency(t)
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
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, rmq.NewRmqID(10)).Return(nil).Once()
	rs.truncator = truncator

	rs.simpleTruncateCheckpoint(context.Background(), testRecoveryCheckpoint(120, 120))
}

// An idle pchannel takes no summary snapshot (only committed write records mark a
// summary dirty), so its durable source checkpoint freezes while timeticks keep
// advancing the consume and flusher checkpoints. Truncation must stay clamped to
// the frozen position, otherwise the restart rewind lands outside the WAL.
func TestSimpleTruncateCheckpointClampedWhilePChannelIsIdle(t *testing.T) {
	enableRecoveryIdempotency(t)
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
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
	// ...and then only timeticks arrived: the in-memory position moves on, the
	// persisted one stays where the last chunk was written.
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(200, 200))
	require.Equal(t, uint64(10), rs.summaryManager.truncateClampCheckpoint().TimeTick)

	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, rmq.NewRmqID(10)).Return(nil).Once()
	rs.truncator = truncator

	rs.simpleTruncateCheckpoint(context.Background(), testRecoveryCheckpoint(220, 220))
}

// With idempotency disabled there is no summary store to replay, so truncation
// keeps taking min(flusher, consume) only.
func TestSimpleTruncateCheckpointNotSummaryClampedWhenIdempotencyDisabled(t *testing.T) {
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
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(10, 10))
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
