package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
		// System confirmation must bypass a user message filter.
		s.filterFunc = func(message.ImmutableMessage) bool { return false }
		boundary := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(104).WithLastConfirmed(walimplstest.NewTestMessageID(0))
		message.SetWALSwitchStart(boundary, wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0}))
		require.NoError(t, s.handleUpstream(tailingImmutableMesasge{boundary.IntoImmutableMessage(walimplstest.NewTestMessageID(5))}))
		var got []message.MessageType
		for s.pendingQueue.Len() > 0 {
			got = append(got, s.pendingQueue.Next().MessageType())
			s.pendingQueue.UnsafeAdvance()
		}
		expected := []message.MessageType{message.MessageTypeInsert, message.MessageTypeAlterWAL}
		if committed {
			expected = append([]message.MessageType{message.MessageTypeTxn}, expected...)
		}
		require.Equal(t, expected, got)
		require.Empty(t, s.txnBuffer.GetUncommittedMessageBuilder())
		require.Zero(t, s.reorderBuffer.Bytes())
		s.metrics.Close()
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
