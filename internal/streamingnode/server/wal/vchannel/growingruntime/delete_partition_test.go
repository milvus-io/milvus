package growingruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDeletePartitionIsolation(t *testing.T) {
	for _, mode := range []string{"live", "live_txn", "bootstrap", "bootstrap_txn"} {
		for _, scope := range []struct {
			name       string
			partition  int64
			segmentIDs []int64
		}{
			{"partition_10", 10, []int64{1, 2}},
			{"partition_20", 20, []int64{3}},
			{"absent_partition", 30, nil},
			{"all_partitions", common.AllPartitionsID, []int64{1, 2, 3}},
		} {
			t.Run(mode+"/"+scope.name, func(t *testing.T) {
				r := newRuntime()
				defer r.Close()
				// Identical primary keys can exist in different partitions. Check
				// the recipients before segcore applies the delete to those keys.
				var visited []int64
				patch := mockey.Mock((*growingSegment).applyDelete).To(func(s *growingSegment, _ context.Context, keys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
					visited = append(visited, s.segmentID)
					require.Equal(t, int64(1), keys.Get(0).GetValue())
					require.Equal(t, []uint64{100}, timestamps, "transaction deletes use the commit timestamp")
					return nil
				}).Build()
				defer patch.UnPatch()
				msg := newPartitionDeleteMessage(t, scope.partition, strings.HasSuffix(mode, "_txn"))
				if strings.HasPrefix(mode, "bootstrap") {
					manager := walsummary.NewManager(walsummary.ManagerConfig{PChannel: "p1"})
					manager.ObserveMessage(context.Background(), msg)
					stream := walsummary.NewStream(manager)
					defer stream.Close()
					require.NoError(t, r.Prepare(context.Background(), walview.VChannelWALView{
						VChannel: "v1", CollectionID: 1,
						BaseGrowingTimeTick: 100, BaseTransformTimeTick: 100,
						TransformLogStream: stream,
						SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{
							{SegmentID: 1, PartitionID: 10},
							{SegmentID: 2, PartitionID: 10},
							{SegmentID: 3, PartitionID: 20},
						}},
					}))
				} else {
					for id, partition := range map[int64]int64{1: 10, 2: 10, 3: 20} {
						require.True(t, r.addSegment(newGrowingSegment(nil, id, partition)))
					}
					require.NoError(t, r.dispatchMessage(context.Background(), msg, false, true))
				}
				require.ElementsMatch(t, scope.segmentIDs, visited)
			})
		}
	}
}

func TestPartitionDeleteNoopInputs(t *testing.T) {
	r := newRuntime()
	defer r.Close()
	require.True(t, r.addSegment(newGrowingSegment(nil, 1, 10)))
	patch := mockey.Mock((*growingSegment).applyDelete).Return(context.Canceled).Build()
	defer patch.UnPatch()
	ctx := context.Background()
	require.NoError(t, r.applyDeleteRequest(ctx, 100, nil))
	require.NoError(t, r.applyDeleteRequest(ctx, 100, &msgpb.DeleteRequest{
		PartitionID: 10,
		PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
	}))
	require.NoError(t, r.applyTransformLogEntry(ctx, nil))
	require.NoError(t, r.applyTransformLogEntry(ctx, &streamingpb.TransformLogEntry{}))
	require.Zero(t, patch.Times(), "empty deletes must not reach segcore")
}

func TestPartitionDeletePropagatesFailure(t *testing.T) {
	for _, replay := range []bool{false, true} {
		r := newRuntime()
		require.True(t, r.addSegment(newGrowingSegment(nil, 1, 10)))
		require.True(t, r.addSegment(newGrowingSegment(nil, 2, 10)))
		patch := mockey.Mock((*growingSegment).applyDelete).Return(context.Canceled).Build()
		msg := newPartitionDeleteMessage(t, 10, false)
		var err error
		if replay {
			err = r.applyTransformLogEntry(context.Background(), messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{}))
		} else {
			err = r.dispatchMessage(context.Background(), msg, false, true)
		}
		calls := patch.Times()
		patch.UnPatch()
		r.Close()
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls, "stop applying the delete after the first failure")
	}
}

func newPartitionDeleteMessage(t *testing.T, partitionID int64, transactional bool) message.ImmutableMessage {
	t.Helper()
	timestamp := uint64(100)
	if transactional {
		timestamp = 99
	}
	deleted := message.NewDeleteMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 1,
			PartitionID:  partitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timestamp},
		}).MustBuildMutable().WithTimeTick(timestamp).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(timestamp)))
	if !transactional {
		return deleted
	}
	txnContext := message.TxnContext{TxnID: 100, Keepalive: time.Second}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(98).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(98))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(100).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(100))
	txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).
		Add(deleted).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return txn
}
