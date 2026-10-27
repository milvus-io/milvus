package timetick

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestQueryMVCCPublicationFencedBeforeConfirmation(t *testing.T) {
	resource.InitForTest(t)
	for _, persisted := range []bool{false, true} {
		t.Run(strconv.FormatBool(persisted), func(t *testing.T) {
			mockey.PatchConvey("WAL and WAB visibility cannot expose an unfenced prefix", t, func() {
				cm := mvcc.NewMVCCManager(100)
				cm.ApplyRecoveryBarrier("v", 100)
				id := walimplstest.NewTestMessageID(1)
				initial := NewTimeTickMsg(100, id, 0, true).IntoImmutableMessage(id)
				buffer := wab.NewWriteAheadBuffer("publication-fence", resource.Resource().Logger().With(), 1024, time.Minute, initial)
				defer buffer.Close()
				operator := newTimeTickSyncOperator(&interceptors.InterceptorBuildParam{
					ChannelInfo: types.PChannelInfo{Name: "publication-fence"}, LastTimeTickMessage: initial,
					LastConfirmedMessageID: id, MVCCManager: cm, WriteAheadBuffer: buffer,
				})
				defer operator.Close()
				flush := createQueryTestMessage(t, 150, "v", message.MessageTypeFlush, false, true)
				operator.ackDetails.AddDetails([]*ack.AckDetail{{BeginTimestamp: 150, Message: flush.IntoImmutableMessage(id)}})
				var original func(*wab.WriteAheadBuffer, []message.ImmutableMessage, message.ImmutableMessage)
				appended := mockey.Mock((*wab.WriteAheadBuffer).Append).Origin(&original).To(func(w *wab.WriteAheadBuffer, msgs []message.ImmutableMessage, confirmation message.ImmutableMessage) {
					original(w, msgs, confirmation)
					_, ready := cm.QueryMVCC().SnapshotAt("v", 90)
					require.False(t, ready, "WAB readers may already be publishing a new View")
				}).Build()
				fail := true
				appendConfirmation := func(context.Context, message.MutableMessage) (message.MessageID, error) {
					_, ready := cm.QueryMVCC().SnapshotAt("v", 90)
					require.False(t, ready, "durable readers may observe confirmation before Append returns")
					if fail {
						return nil, context.DeadlineExceeded
					}
					return id, nil
				}
				require.Error(t, operator.sendTsMsgToWAL(t.Context(), 200, id, persisted, appendConfirmation))
				_, ready := cm.QueryMVCC().SnapshotAt("v", 90)
				require.False(t, ready, "ambiguous append failure must keep the fence")
				fail = false
				require.NoError(t, operator.sendTsMsgToWAL(t.Context(), 200, id, persisted, appendConfirmation))
				require.Equal(t, 1, appended.Times())
				got, ready := cm.QueryMVCC().SnapshotAt("v", 90)
				require.True(t, ready)
				require.Equal(t, mvcc.QueryVChannelMVCC{GrowingTimetick: 90, TransformingTimetick: 90}, got)
			})
		})
	}
}
