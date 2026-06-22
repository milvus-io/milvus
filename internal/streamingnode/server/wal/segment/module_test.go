package segment

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestCleanupDeleteSnapshotKeepsStableInFlightView(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              1,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil)
	module.NotifyCheckpointPersisted(11, 11)

	first := module.ConsumeDirtySnapshots()
	require.Len(t, first, 1)
	assert.Equal(t, moduleapi.SnapshotOpDelete, first[0].Op())

	second := module.ConsumeDirtySnapshots()
	require.Len(t, second, 1)
	assert.Same(t, first[0], second[0])
	assert.Len(t, module.snapshotSegments(), 1)

	first[0].MarkPersisted()
	assert.Empty(t, module.ConsumeDirtySnapshots())
	assert.Empty(t, module.snapshotSegments())
}

func TestCleanupEmitsDataVersionSummaryBeforeDeletingSegment(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              1,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			SealedAtDataVersion:    &viewpb.DataVersion{StreamingVersion: 5, CompactVersion: 1},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil)
	module.NotifyCheckpointPersisted(11, 11)

	snapshots := module.ConsumeDirtySnapshots()

	require.Len(t, snapshots, 2)
	assert.Equal(t, moduleapi.SnapshotOpUpsert, snapshots[0].Op())
	assert.Equal(t, "v1", snapshots[0].Key().VChannel)
	summary, ok := snapshots[0].Payload().(*streamingpb.SegmentDataVersionSummary)
	require.True(t, ok)
	assert.Equal(t, int64(5), summary.GetDataVersion().GetStreamingVersion())
	assert.Equal(t, int64(1), summary.GetDataVersion().GetCompactVersion())
	assert.Equal(t, moduleapi.SnapshotOpDelete, snapshots[1].Op())
	assert.Equal(t, int64(1), snapshots[1].Key().SegmentID)
}

func TestBuildCommitL1SegmentRequestUsesCreateSegmentTimetickForDeleteApply(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            10,
		SegmentId:              100,
		Vchannel:               "v1",
		DataCheckpointTimeTick: 3000,
		StorageVersion:         3,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"},
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: 1000,
			ModifiedRows:          10,
			Level:                 datapb.SegmentLevel_L1,
		},
	}

	req := buildCommitL1SegmentRequest(1, meta)

	assert.Equal(t, uint64(1000), req.GetDeleteApplyStartAfterTimetick())
	assert.Equal(t, int64(10), req.GetCheckPoints()[0].GetNumOfRows())
}

func TestNewGrowingSegmentInfoUsesCreateSegmentTimetickForDeleteApply(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            10,
		SegmentId:              100,
		Vchannel:               "v1",
		DataCheckpointTimeTick: 3000,
		StorageVersion:         3,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: 1000,
			ModifiedRows:          10,
			Level:                 datapb.SegmentLevel_L1,
		},
	}

	segmentInfo := newGrowingSegmentInfo(meta)

	assert.Equal(t, uint64(1000), segmentInfo.GetDeleteApplyStartAfterTimetick())
	assert.Equal(t, uint64(1000), segmentInfo.GetStartPosition().GetTimestamp())
	assert.Equal(t, uint64(3000), segmentInfo.GetDmlPosition().GetTimestamp())
}

func TestVisibleSnapshotIncludesPendingInsertMessages(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1,
			PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil)
	module.SwitchIntoMetaAndData()

	msg := newSegmentModuleTestInsertMessage(t, "v1", 10)
	module.ObserveMessage(context.Background(), msg)

	assert.Equal(t, uint64(10), module.LatestInsertTimeTick("v1"))
	snapshot := module.VisibleSnapshot("v1", 10)
	require.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(1), snapshot.Segments[0].SegmentID)
	assert.Equal(t, uint64(10), snapshot.BaseGrowingTimeTick)
	require.Len(t, snapshot.Segments[0].Data.InsertMessages, 1)
	assert.True(t, msg.MessageID().EQ(snapshot.Segments[0].Data.InsertMessages[0].MessageID()))
	assert.Equal(t, msg.TimeTick(), snapshot.Segments[0].Data.InsertMessages[0].TimeTick())
}

func TestVisibleSnapshotKeepsRawTxnInsertMessage(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1,
			PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil)
	module.SwitchIntoMetaAndData()

	txnMsg := newSegmentModuleTestTxnInsertMessage(t, "v1", 20)
	module.ObserveMessage(context.Background(), txnMsg)

	assert.Equal(t, uint64(20), module.LatestInsertTimeTick("v1"))
	snapshot := module.VisibleSnapshot("v1", 20)
	require.Len(t, snapshot.Segments, 1)
	require.Len(t, snapshot.Segments[0].Data.InsertMessages, 1)
	assert.Equal(t, message.MessageTypeTxn, snapshot.Segments[0].Data.InsertMessages[0].MessageType())
	assert.True(t, txnMsg.MessageID().EQ(snapshot.Segments[0].Data.InsertMessages[0].MessageID()))
}

func TestVisibleSnapshotSelectsDataVersionFromSegmentModuleState(t *testing.T) {
	module := NewModule("p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1,
			PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
		2: {
			CollectionId:           100,
			PartitionId:            200,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     20,
			DataCheckpointTimeTick: 20,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			SealedAtDataVersion:    &viewpb.DataVersion{StreamingVersion: 5, CompactVersion: 1},
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 1,
			},
		},
	}, nil, nil, WithDataVersionSummaries(map[string]*streamingpb.SegmentDataVersionSummary{
		"v1": {DataVersion: &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 9}},
	}))

	snapshot := module.VisibleSnapshot("v1", 100)

	assert.Equal(t, int64(5), snapshot.DataVersion.StreamingVersion)
	assert.Equal(t, int64(1), snapshot.DataVersion.CompactVersion)
	require.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(1), snapshot.Segments[0].SegmentID)
}

func TestVisibleSnapshotUsesSummaryWhenNoFlushedSegmentRemains(t *testing.T) {
	module := NewModule("p1", nil, nil, nil, WithDataVersionSummaries(map[string]*streamingpb.SegmentDataVersionSummary{
		"v1": {DataVersion: &viewpb.DataVersion{StreamingVersion: 7, CompactVersion: 2}},
	}))

	snapshot := module.VisibleSnapshot("v1", 100)

	assert.Equal(t, int64(7), snapshot.DataVersion.StreamingVersion)
	assert.Equal(t, int64(2), snapshot.DataVersion.CompactVersion)
	assert.Empty(t, snapshot.Segments)
}

func newSegmentModuleTestInsertMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 100,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 200,
					Rows:        1,
					BinarySize:  100,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: 1,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 100)))
}

func newSegmentModuleTestTxnInsertMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	txnCtx := message.TxnContext{
		TxnID:     message.TxnID(timetick),
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	beginMsg := message.MustAsImmutableBeginTxnMessageV2(
		begin.WithTxnContext(txnCtx).
			WithTimeTick(timetick - 2).
			WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 2))).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick - 2))),
	)

	insertMutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 100,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 200,
					Rows:        1,
					BinarySize:  100,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: 1,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)
	insert := insertMutable.WithTxnContext(txnCtx).
		WithTimeTick(timetick - 1).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 2))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick - 1)))

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()
	commitMsg := message.MustAsImmutableCommitTxnMessageV2(
		commit.WithTxnContext(txnCtx).
			WithTimeTick(timetick).
			WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 100))),
	)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).Add(insert).Build(commitMsg)
	require.NoError(t, err)
	return txnMsg
}
