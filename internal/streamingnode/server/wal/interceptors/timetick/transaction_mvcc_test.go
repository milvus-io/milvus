package timetick

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestTransactionQueryFrontiersPublishTogether(t *testing.T) {
	resource.InitForTest(t)
	for _, tc := range []struct {
		name      string
		bodies    []message.MessageType
		transform uint64
	}{
		{"insert-only", []message.MessageType{message.MessageTypeInsert, message.MessageTypeInsert}, 10},
		{"mixed", []message.MessageType{message.MessageTypeInsert, message.MessageTypeDelete}, 40},
		{"delete-only", []message.MessageType{message.MessageTypeDelete}, 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			manager := txn.NewTxnManager(types.PChannelInfo{Name: "frontier-test"}, nil)
			begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v").
				WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60000}).
				WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(10)
			session, err := manager.BeginNewTxn(ctx, message.MustAsMutableBeginTxnMessageV2(begin))
			require.NoError(t, err)
			cm := mvcc.NewMVCCManager(10)
			cm.ApplyRecoveryBarrier("v", 10)
			cm.ApplyRecoveryBarrier("other", 10)
			for i, typ := range tc.bodies {
				tick := uint64(20 + i)
				require.NoError(t, session.AddNewMessage(ctx, tick))
				session.AddNewMessageDoneAndKeepalive(tick, typ)
				body := createQueryTestMessage(t, tick, "v", typ, true, true)
				cm.UpdateMVCC(body)
				require.EqualValues(t, 10, queryFrontier(t, cm, "v").GrowingTimetick)
			}
			require.NoError(t, session.RequestCommitAndWait(ctx, 40, uint64(len(tc.bodies))))
			session.CommitDone()
			commit := createQueryTestMessage(t, 40, "v", message.MessageTypeCommitTxn, true, true)
			cm.UpdateMVCC(commit)
			confirmation := createQueryTestMessage(t, 50, "", message.MessageTypeTimeTick, false, true)
			cm.UpdateMVCC(confirmation)
			cm.QueryMVCC().Publish(func(yield func(message.ImmutableMessage, bool) bool) {
				yield(commit.IntoImmutableMessage(nil), session.IsInsertOnly())
			}, confirmation.IntoImmutableMessage(nil))
			frontier, err := cm.QueryMVCC().WaitSnapshot(ctx, "v", 50)
			require.NoError(t, err)
			require.Equal(t, mvcc.QueryVChannelMVCC{GrowingTimetick: 40, TransformingTimetick: tc.transform}, frontier)
			require.Equal(t, frontier, queryFrontier(t, cm, "v"))
			require.Equal(t, mvcc.QueryVChannelMVCC{GrowingTimetick: 10, TransformingTimetick: 10}, queryFrontier(t, cm, "other"))
		})
	}
}

func createQueryTestMessage(t *testing.T, tt uint64, vc string, typ message.MessageType, txn bool, persisted bool) message.MutableMessage {
	t.Helper()
	properties := map[string]string{"_t": strconv.FormatInt(int64(typ), 10), "_vc": vc}
	if !persisted {
		properties["_np"] = ""
	}
	msg := message.NewMutableMessageBeforeAppend(nil, properties).WithTimeTick(tt)
	if txn {
		msg = msg.WithTxnContext(message.TxnContext{})
	}
	return msg
}

func queryFrontier(t *testing.T, cm *mvcc.MVCCManager, channel string) mvcc.QueryVChannelMVCC {
	t.Helper()
	frontier, ready := cm.QueryMVCC().Snapshot(channel, 0)
	require.True(t, ready)
	return frontier
}
