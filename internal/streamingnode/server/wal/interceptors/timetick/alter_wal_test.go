package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
)

func TestAlterWALPublishesPrefixAndResetsNativeIDs(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "uncommitted_transaction"}[pending], func(t *testing.T) {
			impl := newTestTimeTickAppendInterceptor(t)
			param := impl.operator.interceptorBuildParam
			param.WriteAheadBuffer = wab.NewWriteAheadBuffer("test", mlog.With(), 1<<20, time.Minute, param.LastTimeTickMessage)
			defer param.WriteAheadBuffer.Close()
			if pending {
				_, session := newCommitTxnMessageForExistingSession(t, impl, "v1", message.MessageTypeInsert)
				defer func() { require.Equal(t, message.TxnStateRollbacked, session.State()) }()
			}
			a := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
			target := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
			boundary := walimplstest.NewTestMessageID(100)
			ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
			id, err := impl.DoAppend(ctx, a, func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
				message.SetWALSwitchStart(msg, target)
				return boundary, nil
			})
			require.NoError(t, err)
			require.True(t, id.EQ(boundary))
			require.Equal(t, a.TimeTick(), impl.operator.MVCCManager().GetConfirmedTimeTick())
			reader, err := param.WriteAheadBuffer.ReadFromExclusiveTimeTick(ctx, 1)
			require.NoError(t, err)
			readCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			var observed []message.ImmutableMessage
			for {
				msg, err := reader.Next(readCtx)
				require.NoError(t, err)
				observed = append(observed, msg)
				if msg.MessageType() == message.MessageTypeAlterWAL {
					break
				}
			}
			require.True(t, message.IsWALSwitch(observed[len(observed)-1]))
			if pending {
				require.Greater(t, len(observed), 1, "old transaction fragments must reach the scanner before its abort boundary")
			} else {
				require.Len(t, observed, 1)
			}
			require.True(t, impl.operator.ackDetails.Empty())
			// The first new allocation must not refer to the old backend.
			next, err := impl.operator.AckManager().Allocate(ctx)
			require.NoError(t, err)
			require.Greater(t, next.Timestamp(), a.TimeTick())
			require.True(t, next.LastConfirmedMessageID().EQ(target))
			// Completing and syncing a real new-native message exercises the heap
			// comparator that would panic if any old ID survived the boundary.
			data := message.NewFlushAllMessageBuilderV2().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(next.Timestamp()).WithLastConfirmed(target)
			next.Ack(ack.OptImmutableMessage(data.IntoImmutableMessage(wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 1}))))
			details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, details)
			require.Equal(t, message.WALNameWoodpecker, details[0].LastConfirmedMessageID.WALName())
		})
	}
}

type capabilityStorage struct{ recovery.RecoveryStorage }

func (*capabilityStorage) EnableWALSwitch(context.Context) error { panic("mockey") }

func TestAlterWALStopsBeforeUnsafeProgress(t *testing.T) {
	for _, failure := range []string{"capability", "timestamp", "append", "legacy"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("failure does not publish a new WAL boundary", t, func() {
				impl := newTestTimeTickAppendInterceptor(t)
				impl.operator.interceptorBuildParam.RecoveryStorage = &capabilityStorage{}
				gateError := error(nil)
				if failure == "capability" {
					gateError = context.DeadlineExceeded
				}
				mockey.Mock((*capabilityStorage).EnableWALSwitch).Return(gateError).Build()
				if failure == "timestamp" {
					mockey.Mock((*ack.AckManager).AllocateForAppend).Return(nil, context.DeadlineExceeded).Build()
				}
				a := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
				called := false
				ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
				_, err := impl.DoAppend(ctx, a, func(context.Context, message.MutableMessage) (message.MessageID, error) {
					called = true
					if failure == "append" {
						return nil, context.DeadlineExceeded
					}
					return walimplstest.NewTestMessageID(10), nil
				})
				if failure == "legacy" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
				require.Equal(t, failure == "append" || failure == "legacy", called)
				require.Equal(t, uint64(1), impl.operator.MVCCManager().GetConfirmedTimeTick())
				require.Zero(t, impl.operator.lastPublishedTick)
			})
		})
	}
}
