package adaptor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAlterWALConfirmsAndAbortsWithoutNextTimeTick(t *testing.T) {
	for _, committed := range []bool{false, true} {
		s := newTraceTestScannerAdaptor()
		txn := buildTraceTestTxnImmutableMessages(t, context.Background())
		for _, m := range txn[:2] {
			require.NoError(t, s.handleUpstream(m))
		}
		if committed {
			require.NoError(t, s.handleUpstream(txn[2]))
		}
		ordinary := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(103).WithLastConfirmed(walimplstest.NewTestMessageID(0)).IntoImmutableMessage(walimplstest.NewTestMessageID(4))
		require.NoError(t, s.handleUpstream(ordinary))
		boundary := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(104).WithLastConfirmed(walimplstest.NewTestMessageID(0))
		message.SetWALSwitchStart(boundary, wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0}))
		require.NoError(t, s.handleUpstream(tailingImmutableMesasge{boundary.IntoImmutableMessage(walimplstest.NewTestMessageID(5))}))
		var got []message.MessageType
		for s.pendingQueue.Len() > 0 {
			got = append(got, s.pendingQueue.Next().MessageType())
			s.pendingQueue.UnsafeAdvance()
		}
		expected := []message.MessageType{message.MessageTypeInsert, message.MessageTypeTimeTick}
		if committed {
			expected = append([]message.MessageType{message.MessageTypeTxn}, expected...)
		}
		require.Equal(t, expected, got)
		require.Empty(t, s.txnBuffer.GetUncommittedMessageBuilder())
		require.Zero(t, s.reorderBuffer.Bytes())
		s.metrics.Close()
	}
}

func TestAlterWALScannerHonorsDeliverFilters(t *testing.T) {
	for _, tc := range []struct {
		name       string
		filter     options.DeliverFilter
		wantPrefix []uint64
		wantSuffix []uint64
	}{
		{"unfiltered", nil, []uint64{20, 30}, []uint64{40, 50}},
		{"resume after boundary", options.DeliverFilterTimeTickGT(31), nil, []uint64{40, 50}},
		{"GT excludes boundary", options.DeliverFilterTimeTickGT(30), nil, []uint64{40, 50}},
		{"GT includes boundary", options.DeliverFilterTimeTickGT(29), []uint64{30}, []uint64{40, 50}},
		{"GTE excludes boundary", options.DeliverFilterTimeTickGTE(31), nil, []uint64{40, 50}},
		{"GTE includes boundary", options.DeliverFilterTimeTickGTE(30), []uint64{30}, []uint64{40, 50}},
		{"GTE includes prefix", options.DeliverFilterTimeTickGTE(20), []uint64{20, 30}, []uint64{40, 50}},
		{"insert only", options.DeliverFilterMessageType(message.MessageTypeInsert), []uint64{20, 30}, []uint64{40, 50}},
		{"alter WAL only", options.DeliverFilterMessageType(message.MessageTypeAlterWAL), []uint64{30}, []uint64{50}},
	} {
		for _, tailing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tailing=%t", tc.name, tailing), func(t *testing.T) {
				s := newTraceTestScannerAdaptor()
				t.Cleanup(s.metrics.Close)
				if tc.filter != nil {
					s.filterFunc = options.GetFilterFunc([]options.DeliverFilter{tc.filter})
				}
				drain := func() []uint64 {
					var ticks []uint64
					for s.pendingQueue.Len() > 0 {
						ticks = append(ticks, s.pendingQueue.Next().TimeTick())
						s.pendingQueue.UnsafeAdvance()
					}
					return ticks
				}
				require.NoError(t, s.handleUpstream(newScannerTestMessage(t, 20, "v1", message.MessageTypeInsert, true)))
				boundary := newScannerTestWALSwitchMessage(30, 30)
				if tailing {
					boundary = tailingImmutableMesasge{boundary}
				}
				require.NoError(t, s.handleUpstream(boundary))
				require.Equal(t, tc.wantPrefix, drain())
				require.Zero(t, s.reorderBuffer.Len(), "a filtered marker must still confirm the prefix")
				require.NoError(t, s.handleUpstream(newScannerTestWALSwitchMessage(30, 31)))
				require.Empty(t, drain(), "a physical retry copy must not be delivered")
				require.NoError(t, s.handleUpstream(newScannerTestMessage(t, 40, "v1", message.MessageTypeInsert, true)))
				require.NoError(t, s.handleUpstream(newScannerTestMessage(t, 50, "", message.MessageTypeTimeTick, true)))
				require.Equal(t, tc.wantSuffix, drain())
			})
		}
	}
}

func TestAlterWALFilteredBoundaryAbortsUnfinishedTransactions(t *testing.T) {
	s := newTraceTestScannerAdaptor()
	t.Cleanup(s.metrics.Close)
	s.filterFunc = options.GetFilterFunc([]options.DeliverFilter{options.DeliverFilterTimeTickGT(105)})
	// Resume filters retain fragments until a commit determines the txn's
	// TimeTick. An earlier switch must still abort those unfinished fragments.
	for _, msg := range buildTraceTestTxnImmutableMessages(t, context.Background())[:2] {
		require.NoError(t, s.handleUpstream(msg))
	}
	require.Equal(t, 2, s.reorderBuffer.Len())
	require.NoError(t, s.handleUpstream(newScannerTestWALSwitchMessage(104, 5)))
	require.Zero(t, s.pendingQueue.Len())
	require.Zero(t, s.reorderBuffer.Len())
	require.Empty(t, s.txnBuffer.GetUncommittedMessageBuilder())
}

func TestAlterWALInternalAndPublicDelivery(t *testing.T) {
	for _, internal := range []bool{false, true} {
		for _, tailing := range []bool{false, true} {
			t.Run(fmt.Sprintf("internal=%t/tailing=%t", internal, tailing), func(t *testing.T) {
				s := newTraceTestScannerAdaptor()
				t.Cleanup(s.metrics.Close)
				s.deliverWALControl = internal
				broadcast := message.NewAlterWALMessageBuilderV2().
					WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_Test, Config: map[string]string{"target": "private"}}).
					WithBody(&message.AlterWALMessageBody{}).
					WithClusterLevelBroadcast(message.ClusterChannels{Channels: []string{"p1"}, ControlChannel: "p1_v0"}, message.OptBuildBroadcastAckSyncUp()).
					MustBuildBroadcast().WithBroadcastID(123)
				mutable := broadcast.SplitIntoMutableMessage()[0].WithTimeTick(30).WithLastConfirmed(walimplstest.NewTestMessageID(1))
				message.SetWALSwitchStart(mutable, walimplstest.NewTestMessageID(10))
				boundary := mutable.IntoImmutableMessage(walimplstest.NewTestMessageID(20))
				incoming := boundary
				if tailing {
					incoming = tailingImmutableMesasge{boundary}
				}
				require.NoError(t, s.handleUpstream(incoming))
				require.Equal(t, 1, s.pendingQueue.Len())
				got := s.pendingQueue.Next()
				require.Equal(t, uint64(30), got.TimeTick())
				require.True(t, got.MessageID().EQ(boundary.MessageID()))
				require.True(t, got.LastConfirmedMessageID().EQ(boundary.LastConfirmedMessageID()))
				if internal {
					require.Same(t, boundary, got, "recovery must keep the original flush/ACK record")
					require.Equal(t, uint64(123), got.BroadcastHeader().BroadcastID)
				} else {
					require.Equal(t, message.MessageTypeTimeTick, got.MessageType())
					require.Nil(t, got.BroadcastHeader())
					require.True(t, got.MessageType().IsSelfControlled(), "CDC must not replicate the confirmation")
					body := message.MustAsImmutableTimeTickMessageV1(got).MustBody()
					require.Equal(t, uint64(30), body.Base.Timestamp)
				}
				require.True(t, message.IsWALSwitch(boundary), "projection must not mutate WAB's shared record")
			})
		}
	}
}

func TestAlterWALBroadcastConfirmsCollectionScanner(t *testing.T) {
	broadcast := message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_Test}).
		WithBody(&message.AlterWALMessageBody{}).
		WithClusterLevelBroadcast(message.ClusterChannels{Channels: []string{"p1", "p2"}, ControlChannel: "p1_v0"}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().WithBroadcastID(123)
	for _, mutable := range broadcast.SplitIntoMutableMessage() {
		mutable = mutable.WithTimeTick(110).WithLastConfirmed(walimplstest.NewTestMessageID(0))
		message.SetWALSwitchStart(mutable, walimplstest.NewTestMessageID(10))
		boundary := mutable.IntoImmutableMessage(walimplstest.NewTestMessageID(20))
		for _, tailing := range []bool{false, true} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("channel=%s/tailing=%t/filtered=%t", boundary.VChannel(), tailing, filtered), func(t *testing.T) {
					s := newTraceTestScannerAdaptor()
					t.Cleanup(s.metrics.Close)
					s.readOption.VChannel = "v1"
					if filtered {
						s.filterFunc = options.GetFilterFunc([]options.DeliverFilter{options.DeliverFilterTimeTickGT(110)})
					}
					for _, msg := range buildTraceTestTxnImmutableMessages(t, context.Background())[:2] {
						require.NoError(t, s.handleUpstream(msg))
					}
					prefix := newScannerTestMessage(t, 103, "v1", message.MessageTypeInsert, true)
					require.NoError(t, s.handleUpstream(prefix))
					require.NoError(t, s.handleUpstream(newScannerTestMessage(t, 104, "v2", message.MessageTypeInsert, true)))
					require.Zero(t, s.pendingQueue.Len())
					incoming := boundary
					if tailing {
						incoming = tailingImmutableMesasge{boundary}
					}
					require.NoError(t, s.handleUpstream(incoming))
					require.Zero(t, s.reorderBuffer.Len())
					require.Empty(t, s.txnBuffer.GetUncommittedMessageBuilder())
					if !filtered {
						require.Equal(t, 2, s.pendingQueue.Len(), "only the subscribed collection's insert and confirmation are delivered")
						require.Same(t, prefix, s.pendingQueue.Next())
						s.pendingQueue.UnsafeAdvance()
						require.Equal(t, message.MessageTypeTimeTick, s.pendingQueue.Next().MessageType())
						require.Equal(t, uint64(110), s.pendingQueue.Next().TimeTick())
						s.pendingQueue.UnsafeAdvance()
					}
					require.Zero(t, s.pendingQueue.Len(), "the public confirmation still obeys delivery filters")
					// A retry copy with another physical ID must not deliver another
					// confirmation or consume data newer than the boundary.
					require.NoError(t, s.handleUpstream(newScannerTestMessage(t, 120, "v1", message.MessageTypeInsert, true)))
					duplicate := message.NewImmutableMesasge(walimplstest.NewTestMessageID(21), boundary.Payload(), boundary.Properties().ToRawMap())
					require.NoError(t, s.handleUpstream(duplicate))
					require.Zero(t, s.pendingQueue.Len())
					require.Equal(t, 1, s.reorderBuffer.Len())
				})
			}
		}
	}
}

func TestAlterWALConfirmationPreservesTransactionFilters(t *testing.T) {
	for _, threshold := range []uint64{101, 102} {
		for _, typ := range []message.MessageType{message.MessageTypeInsert, message.MessageTypeDelete} {
			t.Run(fmt.Sprintf("GT%d/%s", threshold, typ), func(t *testing.T) {
				s := newTraceTestScannerAdaptor()
				t.Cleanup(s.metrics.Close)
				s.filterFunc = options.GetFilterFunc([]options.DeliverFilter{
					options.DeliverFilterTimeTickGT(threshold), options.DeliverFilterMessageType(typ),
				})
				for _, msg := range buildTraceTestTxnImmutableMessages(t, context.Background()) {
					require.NoError(t, s.handleUpstream(msg))
				}
				require.NoError(t, s.handleUpstream(newScannerTestWALSwitchMessage(104, 5)))
				if threshold < 102 {
					got := s.pendingQueue.Next()
					require.Equal(t, message.MessageTypeTxn, got.MessageType())
					require.Equal(t, uint64(102), got.TimeTick())
					wantBodies := 0
					if typ == message.MessageTypeInsert {
						wantBodies = 1
					}
					require.Equal(t, wantBodies, message.AsImmutableTxnMessage(got).Size())
					s.pendingQueue.UnsafeAdvance()
				}
				require.Equal(t, 1, s.pendingQueue.Len())
				require.Equal(t, message.MessageTypeTimeTick, s.pendingQueue.Next().MessageType())
				require.Empty(t, s.txnBuffer.GetUncommittedMessageBuilder())
			})
		}
	}
}

func newScannerTestWALSwitchMessage(tick uint64, id int64) message.ImmutableMessage {
	msg := message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName(message.WALNameTest)}).
		WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().
		WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(0))
	message.SetWALSwitchStart(msg, walimplstest.NewTestMessageID(0))
	return msg.IntoImmutableMessage(walimplstest.NewTestMessageID(id))
}

func TestAlterWALScannerDeduplicatesPhysicalCopies(t *testing.T) {
	for _, tailing := range []bool{false, true} {
		t.Run(fmt.Sprintf("tailing=%t", tailing), func(t *testing.T) {
			s := newTraceTestScannerAdaptor()
			t.Cleanup(s.metrics.Close)
			boundary := func(tick uint64, id int64) message.ImmutableMessage {
				immutable := newScannerTestWALSwitchMessage(tick, id)
				if tailing {
					return tailingImmutableMesasge{immutable}
				}
				return immutable
			}
			prefix := newScannerTestMessage(t, 10, "v1", message.MessageTypeInsert, true)
			require.NoError(t, s.handleUpstream(prefix))
			// Ordinary retry copies must still be deduplicated before the
			// switch confirms their prefix, including different physical IDs.
			require.NoError(t, s.handleUpstream(prefix))
			require.NoError(t, s.handleUpstream(message.NewImmutableMesasge(
				walimplstest.NewTestMessageID(11), prefix.Payload(), prefix.Properties().ToRawMap())))
			require.NoError(t, s.handleUpstream(boundary(20, 20)))
			require.Equal(t, 2, s.pendingQueue.Len())
			require.Equal(t, prefix, s.pendingQueue.Next())
			s.pendingQueue.UnsafeAdvance()
			require.Equal(t, message.MessageTypeTimeTick, s.pendingQueue.Next().MessageType())
			s.pendingQueue.UnsafeAdvance()
			require.Zero(t, s.reorderBuffer.Len())

			// The first copy has already left the buffer. Retry may use either
			// the same physical ID or another ID with the same logical TimeTick.
			for _, id := range []int64{20, 21} {
				require.NoError(t, s.handleUpstream(boundary(20, id)))
				require.Zero(t, s.pendingQueue.Len())
				require.Zero(t, s.reorderBuffer.Len())
			}

			// A later boundary still confirms and delivers its prefix without
			// waiting for another TimeTick; replaying the older boundary is inert.
			suffix := newScannerTestMessage(t, 30, "v1", message.MessageTypeInsert, true)
			require.NoError(t, s.handleUpstream(suffix))
			require.NoError(t, s.handleUpstream(boundary(40, 40)))
			require.Equal(t, 2, s.pendingQueue.Len())
			require.Equal(t, suffix, s.pendingQueue.Next())
			s.pendingQueue.UnsafeAdvance()
			require.Equal(t, uint64(40), s.pendingQueue.Next().TimeTick())
			s.pendingQueue.UnsafeAdvance()
			require.NoError(t, s.handleUpstream(boundary(20, 22)))
			require.Zero(t, s.pendingQueue.Len())
			require.Zero(t, s.reorderBuffer.Bytes())

			// Even before observing any switch, a boundary older than an
			// already confirmed TimeTick must go through buffer validation.
			fresh := newTraceTestScannerAdaptor()
			t.Cleanup(fresh.metrics.Close)
			require.NoError(t, fresh.handleUpstream(newScannerTestMessage(t, 100, "", message.MessageTypeTimeTick, true)))
			fresh.pendingQueue.UnsafeAdvance()
			require.NoError(t, fresh.handleUpstream(boundary(90, 90)))
			require.Zero(t, fresh.pendingQueue.Len())
			require.Zero(t, fresh.reorderBuffer.Len())
			require.NoError(t, fresh.handleUpstream(boundary(110, 110)))
			require.Equal(t, 1, fresh.pendingQueue.Len())
			require.Equal(t, uint64(110), fresh.pendingQueue.Next().TimeTick())
		})
	}
}

func TestAlterWALIsOnePhysicalRecord(t *testing.T) {
	resource.InitForTest(t)
	cfg := &paramtable.Get().StreamingCfg.SplitChunkSN
	old := cfg.SwapTempValue("true")
	defer cfg.SwapTempValue(old)
	calls := 0
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{}, rwWALImpls: newFirstTimeTickWALImpls(func(_ context.Context, m message.MutableMessage) (message.MessageID, error) {
		calls++
		require.False(t, message.IsChunkedPayload(m))
		return walimplstest.NewTestMessageID(1), nil
	})}
	w.SetLogger(mlog.With())
	// Even a small configured payload budget must not split the control link.
	w.chunkPayloadSizeAt.Store(time.Now().UnixNano())
	w.chunkPayloadSizeValue.Store(1)
	m := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(20).WithLastConfirmedUseMessageID()
	_, err := w.appendWithOptionalChunking(context.Background(), m)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
