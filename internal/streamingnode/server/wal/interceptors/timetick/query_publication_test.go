package timetick

import (
	"context"
	"fmt"
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

func TestQueryMVCCPublishesBeforeConfirmationDelivery(t *testing.T) {
	resource.InitForTest(t)
	for _, persisted := range []bool{false, true} {
		for _, scoped := range []bool{false, true} {
			for _, stopped := range []bool{false, true} {
				t.Run(fmt.Sprintf("persisted=%v/scoped=%v/stopped=%v", persisted, scoped, stopped), func(t *testing.T) {
					mockey.PatchConvey("query targets precede WAL and WAB confirmation delivery", t, func() {
						cm := mvcc.NewMVCCManager(100)
						cm.ApplyRecoveryBarrier("v", 100)
						id := walimplstest.NewTestMessageID(1)
						initial := NewTimeTickMsg(100, id, 0, true).IntoImmutableMessage(id)
						buffer := wab.NewWriteAheadBuffer("publication-order", resource.Resource().Logger().With(), 1024, time.Minute, initial)
						defer buffer.Close()
						operator := newTimeTickSyncOperator(&interceptors.InterceptorBuildParam{
							ChannelInfo: types.PChannelInfo{Name: "publication-order"}, LastTimeTickMessage: initial,
							LastConfirmedMessageID: id, MVCCManager: cm, WriteAheadBuffer: buffer,
						})
						defer operator.Close()
						expected := mvcc.QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100}
						if persisted {
							flush := createQueryTestMessage(t, 150, "v", message.MessageTypeFlush, false, true)
							operator.ackDetails.AddDetails([]*ack.AckDetail{{BeginTimestamp: 150, Message: flush.IntoImmutableMessage(id)}})
							expected.TransformingTimetick = 150
						}
						if scoped {
							operator.queryBarriers = map[string]uint64{"v": 200}
							expected = mvcc.QueryVChannelMVCC{GrowingTimetick: 200, TransformingTimetick: 200}
						}
						checkPublication := func() {
							got, ready := cm.QueryMVCC().Snapshot("v", 200)
							require.True(t, ready)
							require.Equal(t, expected, got)
							old, ready := cm.QueryMVCC().SnapshotAt("v", 90)
							require.True(t, ready, "existing snapshots remain usable during confirmation append")
							require.Equal(t, mvcc.QueryVChannelMVCC{GrowingTimetick: 90, TransformingTimetick: 90}, old)
						}
						var original func(*wab.WriteAheadBuffer, []message.ImmutableMessage, message.ImmutableMessage)
						appended := mockey.Mock((*wab.WriteAheadBuffer).Append).Origin(&original).To(func(w *wab.WriteAheadBuffer, msgs []message.ImmutableMessage, confirmation message.ImmutableMessage) {
							checkPublication()
							require.Equal(t, persisted, confirmation.IsPersisted())
							require.Equal(t, scoped, confirmation.MessageType() == message.MessageTypeMVCCBarrier)
							original(w, msgs, confirmation)
						}).Build()
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						err := operator.sendTsMsgToWAL(ctx, 200, id, persisted, func(context.Context, message.MutableMessage) (message.MessageID, error) {
							checkPublication()
							require.Zero(t, appended.Times(), "WAB cannot deliver the prefix before confirmation append")
							if stopped {
								// A terminal append failure ends the writer; backend timeouts
								// on a live writer retry inside WAL.Append instead.
								cancel()
								return nil, ctx.Err()
							}
							return id, nil
						})
						if stopped {
							require.ErrorIs(t, err, context.Canceled)
							require.Zero(t, appended.Times())
							require.Zero(t, operator.lastPublishedTick, "scoped requests must not complete before delivery")
							require.Equal(t, persisted, !operator.ackDetails.Empty())
						} else {
							require.NoError(t, err)
							require.Equal(t, 1, appended.Times())
							require.True(t, operator.ackDetails.Empty())
							require.EqualValues(t, 200, operator.lastPublishedTick)
						}
					})
				})
			}
		}
	}
}
