package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoveryAccountsNewRowWritePartitions(t *testing.T) {
	paramtable.Init()
	const channel = "test_1v0"
	assignments := []*message.PartitionSegmentAssignment{
		{PartitionId: 10, Rows: 2, BinarySize: 100, SegmentAssignment: &message.SegmentAssignment{SegmentId: 100}},
		{PartitionId: 20, Rows: 1, BinarySize: 60, SegmentAssignment: &message.SegmentAssignment{SegmentId: 200}},
	}
	insert := message.NewInsertMessageBuilderV2().WithVChannel(channel).
		WithHeader(&messagespb.InsertMessageHeaderV2{CollectionId: 1, Partitions: assignments}).
		WithBody(&messagespb.InsertMessageBody{}).MustBuildMutable().WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(10))
	upsert := message.NewUpsertMessageBuilderV2().WithVChannel(channel).
		WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, Partitions: assignments, DeleteRows: 3}).
		WithBody(&messagespb.UpsertMessageBody{}).MustBuildMutable().WithTimeTick(11).
		WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(11))

	for _, transaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "transaction"}[transaction], func(t *testing.T) {
			rs := newRecoveryStorage(types.PChannelInfo{Name: "test"}, &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1})
			t.Cleanup(rs.metrics.Close)
			rs.vchannels = map[string]*vchannelRecoveryInfo{channel: {}}
			rs.segments = make(map[int64]*segmentRecoveryInfo)
			for _, assignment := range assignments {
				rs.segments[assignment.SegmentAssignment.SegmentId] = &segmentRecoveryInfo{meta: &streamingpb.SegmentAssignmentMeta{
					SegmentId: assignment.SegmentAssignment.SegmentId, PartitionId: assignment.PartitionId,
					State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					Stat:  &streamingpb.SegmentAssignmentStat{},
				}}
			}
			messages := []message.ImmutableMessage{insert, upsert}
			expectedTimeTick := uint64(11)
			if transaction {
				ctx := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.BeginTxnMessageHeader{}).
					WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(9).WithTxnContext(ctx).
					WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(9))
				commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.CommitTxnMessageHeader{}).
					WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTimeTick(20).WithTxnContext(ctx).
					WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(20))
				txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(insert).Add(upsert).
					Build(message.MustAsImmutableCommitTxnMessageV2(commit))
				require.NoError(t, err)
				messages = []message.ImmutableMessage{txn}
				expectedTimeTick = 20
			}
			for _, msg := range messages {
				require.NoError(t, rs.ObserveMessage(context.Background(), msg))
			}
			for _, assignment := range assignments {
				segment := rs.segments[assignment.SegmentAssignment.SegmentId]
				require.Equal(t, 2*assignment.Rows, segment.Rows())
				require.Equal(t, 2*assignment.BinarySize, segment.BinarySize())
				require.Equal(t, expectedTimeTick, segment.meta.CheckpointTimeTick)
			}
			require.Equal(t, expectedTimeTick, rs.checkpoint.TimeTick)
			// An overlapping replay prefix must not charge the same rows again.
			for _, msg := range messages {
				require.NoError(t, rs.ObserveMessage(context.Background(), msg))
			}
			for _, assignment := range assignments {
				require.Equal(t, 2*assignment.Rows, rs.segments[assignment.SegmentAssignment.SegmentId].Rows())
			}
		})
	}
}
