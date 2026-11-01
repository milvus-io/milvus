package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestMVCCBarrierSyncPersistenceAndDelivery(t *testing.T) {
	resource.InitForTest(t)
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "pending-data"}[busy], func(t *testing.T) {
			mockey.PatchConvey("barrier uses existing ACK batch persistence policy", t, func() {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
				require.NoError(t, err)
				id := walimplstest.NewTestMessageID(1)
				initial := NewTimeTickMsg(ts, id, 0, true).IntoImmutableMessage(id)
				buffer := wab.NewWriteAheadBuffer("barrier", resource.Resource().Logger(), 4096, time.Minute, initial)
				defer buffer.Close()
				future := syncutil.NewFuture[wal.WAL]()
				future.Set(&mock_wal.MockWAL{})
				op := newTimeTickSyncOperator(&interceptors.InterceptorBuildParam{MVCCManager: mvcc.NewMVCCManager(ts), ChannelInfo: types.PChannelInfo{Name: "barrier", Term: 1}, WAL: future, LastTimeTickMessage: initial, LastConfirmedMessageID: id, WriteAheadBuffer: buffer})
				defer op.Close()
				if busy {
					acker, err := op.AckManager().Allocate(ctx)
					require.NoError(t, err)
					msg := message.NewInsertMessageBuilderV1().WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).WithVChannel("v").MustBuildMutable().WithTimeTick(acker.Timestamp()).WithLastConfirmed(id).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
					acker.Ack(ack.OptImmutableMessage(msg))
				}
				target, err := resource.Resource().TSOAllocator().Allocate(ctx)
				require.NoError(t, err)
				failed := true
				patch := mockey.Mock((*mock_wal.MockWAL).Append).To(func(_ *mock_wal.MockWAL, appendCtx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
					require.Equal(t, message.MessageTypeMVCCBarrier, msg.MessageType())
					require.Equal(t, busy, msg.IsPersisted())
					require.Equal(t, !busy, utility.GetNotPersisted(appendCtx) != nil)
					require.GreaterOrEqual(t, msg.TimeTick(), target)
					require.Equal(t, []string{"v", "v2"}, message.MustAsMutableMVCCBarrierMessageV2(msg).Header().GetVchannels())
					if failed {
						return nil, context.DeadlineExceeded
					}
					return &types.AppendResult{MessageID: walimplstest.NewTestMessageID(3), TimeTick: msg.TimeTick()}, nil
				}).Build()
				published, err := op.SyncWithQueryBarriers(ctx, false, map[string]uint64{"v": target, "v2": target})
				require.Error(t, err)
				require.Empty(t, published)
				failed = false
				published, err = op.SyncWithQueryBarriers(ctx, false, map[string]uint64{"v": target, "v2": target})
				require.NoError(t, err)
				require.GreaterOrEqual(t, published["v"], target)
				require.Equal(t, published["v"], published["v2"])
				require.Equal(t, 2, patch.Times())
				reader, err := buffer.ReadFromExclusiveTimeTick(ctx, ts)
				require.NoError(t, err)
				msg, err := reader.Next(ctx)
				require.NoError(t, err)
				if busy {
					require.Equal(t, message.MessageTypeInsert, msg.MessageType())
					msg, err = reader.Next(ctx)
					require.NoError(t, err)
				}
				require.Equal(t, message.MessageTypeMVCCBarrier, msg.MessageType())
				require.Equal(t, published["v"], msg.TimeTick())
				require.Empty(t, msg.VChannel())
				require.Equal(t, []string{"v", "v2"}, message.MustAsImmutableMVCCBarrierMessageV2(msg).Header().GetVchannels())
			})
		})
	}
}
