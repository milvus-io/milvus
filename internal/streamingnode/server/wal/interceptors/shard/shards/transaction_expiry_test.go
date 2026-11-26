package shards

import (
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSegmentAssignmentAfterConcurrentTransactionExpiry(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: t.Name()}
	manager := txn.NewTxnManager(channel, nil)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 1000}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(1)
	session, err := manager.BeginNewTxn(t.Context(), message.MustAsMutableBeginTxnMessageV2(begin))
	require.NoError(t, err)
	require.NoError(t, session.AddNewMessage(t.Context(), 2))
	expiration := tsoutil.AddPhysicalDurationOnTs(1, time.Second)
	require.True(t, status.AsStreamingError(session.AddNewMessage(t.Context(), expiration)).IsTxnExpired())
	// Isolate the unrelated stats allocator, but exercise real segment
	// assignment, reference accounting and TxnSession callback registration.
	patch := mockey.Mock((*stats.StatsManager).AllocRows).Return(nil).Build()
	defer patch.UnPatch()
	segment := newSegmentAllocManagerFromProto(channel, &streamingpb.SegmentAssignmentMeta{
		SegmentId: 3, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
	})
	var assigned *AssignSegmentResult
	require.NotPanics(t, func() {
		assigned, err = segment.AllocRows(&AssignSegmentRequest{TimeTick: 2, TxnSession: session})
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, segment.TxnSem())
	manager.CleanupTxnUntil(1)
	require.EqualValues(t, 1, segment.TxnSem(), "cleanup must not release the accepted body")
	assigned.Acknowledge.Dec()
	session.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
	manager.CleanupTxnUntil(expiration)
	manager.CleanupTxnUntil(expiration + 1)
	require.Zero(t, segment.TxnSem())
	require.Zero(t, segment.AckSem())
	require.NoError(t, manager.GracefulClose(t.Context()))
}
