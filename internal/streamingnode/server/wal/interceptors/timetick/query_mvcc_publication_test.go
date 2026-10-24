package timetick

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestQueryMVCCACKPublicationPrecedesAppendCallback(t *testing.T) {
	mockey.PatchConvey("actual append defers allow publication before the MVCC callback", t, func() {
		impl := newTestTimeTickAppendInterceptor(t)
		manager := impl.operator.MVCCManager()
		var original func(*mvcc.MVCCManager, message.MutableMessage)
		callbacks := 0
		mockey.Mock((*mvcc.MVCCManager).UpdateMVCC).Origin(&original).To(func(cm *mvcc.MVCCManager, msg message.MutableMessage) {
			// The earlier deferred Acker.Ack has completed, while UpdateMVCC has not.
			details, err := impl.operator.AckManager().SyncAndGetAcknowledged(t.Context())
			require.NoError(t, err)
			var published []message.ImmutableMessage
			var through uint64
			for _, detail := range details {
				through = max(through, detail.BeginTimestamp)
				if detail.Err == nil && !detail.IsSync {
					published = append(published, detail.Message)
				}
			}
			require.NotEmpty(t, published)
			require.GreaterOrEqual(t, through, msg.TimeTick())
			confirmation := NewTimeTickMsg(through, walimplstest.NewTestMessageID(0), 0, false).IntoImmutableMessage(nil)
			cm.QueryMVCC().Publish(queryMVCCMessages(published), confirmation)
			before, ready := cm.QueryMVCC().Snapshot("v", through)
			require.True(t, ready)
			original(cm, msg)
			after, ready := cm.QueryMVCC().Snapshot("v", through)
			require.True(t, ready)
			require.Equal(t, before, after)
			require.EqualValues(t, through, cm.QueryMVCC().Minimum("v", true), "late callbacks add no uncovered work")
			callbacks++
		}).Build()
		for _, kind := range []message.MessageType{message.MessageTypeCreateCollection, message.MessageTypeInsert, message.MessageTypeDropCollection} {
			msg := message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.FormatInt(int64(kind), 10), "_vc": "v"})
			ctx := utility.WithExtraAppendResult(t.Context(), &utility.ExtraAppendResult{})
			_, err := impl.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				return walimplstest.NewTestMessageID(int64(callbacks + 1)), nil
			})
			require.NoError(t, err)
		}
		require.Equal(t, 3, callbacks)
		got, _ := manager.QueryMVCC().Snapshot("v", 0)
		require.Zero(t, got)
	})
}

func queryMVCCMessages(messages []message.ImmutableMessage) func(func(message.ImmutableMessage, bool) bool) {
	return func(yield func(message.ImmutableMessage, bool) bool) {
		for _, msg := range messages {
			if !yield(msg, false) {
				return
			}
		}
	}
}
