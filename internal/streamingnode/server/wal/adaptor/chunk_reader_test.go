package adaptor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func readerPhysicalChunk(tt uint64, index, total, id int, payload string) message.ImmutableMessage {
	return message.NewMutableMessageBeforeAppend([]byte(payload), map[string]string{
		"_v": "2", "_t": strconv.Itoa(int(message.MessageTypeInsert)), "_tt": message.EncodeUint64(tt),
		"_ci": strconv.Itoa(index), "_ct": strconv.Itoa(total),
	}).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(id)))
}

func TestCatchupChunkReaderLogicalDelivery(t *testing.T) {
	end := errors.New("end of test records")
	input := make(chan message.ImmutableMessage, 5)
	input <- readerPhysicalChunk(10, 0, 2, 1, "abc")
	input <- readerPhysicalChunk(20, 0, 2, 2, "xy")
	input <- readerPhysicalChunk(10, 0, 2, 3, "abc") // persisted but unacknowledged head retry
	input <- readerPhysicalChunk(10, 1, 2, 4, "def")
	input <- readerPhysicalChunk(20, 1, 2, 5, "z")
	close(input)
	scanner := mock_walimpls.NewMockScannerImpls(t)
	scanner.EXPECT().Chan().Return(input)
	scanner.EXPECT().Error().Return(end).Once()
	scanner.EXPECT().Close().Return(nil).Once()
	output := make(chan message.ImmutableMessage, 5)
	s := &catchupScanner{switchableScannerImpl: switchableScannerImpl{msgChan: output}}
	_, err := s.consumeWithScanner(context.Background(), scanner)
	require.ErrorIs(t, err, end)
	require.Len(t, output, 2)
	first := <-output
	require.Equal(t, []byte("abcdef"), first.IntoImmutableMessageProto().Payload)
	require.True(t, first.MessageID().EQ(walimplstest.NewTestMessageID(3)))
	require.False(t, message.IsChunkedPayload(first))
	require.Equal(t, []byte("xyz"), (<-output).IntoImmutableMessageProto().Payload)
}

func TestCatchupChunkReaderSeekAndIncompleteTail(t *testing.T) {
	end := errors.New("end of test records")
	input := make(chan message.ImmutableMessage, 4)
	input <- readerPhysicalChunk(10, 1, 2, 1, "tail") // seek within a run: cannot emit it
	input <- readerPhysicalChunk(20, 0, 2, 2, "old")
	input <- readerPhysicalChunk(20, 1, 2, 3, "done") // fully assembled but filtered by resume TT
	input <- readerPhysicalChunk(30, 0, 2, 4, "unfinished")
	close(input)
	scanner := mock_walimpls.NewMockScannerImpls(t)
	scanner.EXPECT().Chan().Return(input)
	scanner.EXPECT().Error().Return(end).Once()
	scanner.EXPECT().Close().Return(nil).Once()
	output := make(chan message.ImmutableMessage, 4)
	s := &catchupScanner{switchableScannerImpl: switchableScannerImpl{msgChan: output}, exclusiveStartTimeTick: 20}
	_, err := s.consumeWithScanner(context.Background(), scanner)
	require.ErrorIs(t, err, end)
	require.Empty(t, output)
}

func TestScannerAdaptorStopsOnCorruptedChunk(t *testing.T) {
	resource.InitForTest(t)
	input := make(chan message.ImmutableMessage, 3)
	input <- readerPhysicalChunk(10, 0, 2, 1, "head")
	input <- readerPhysicalChunk(10, 0, 2, 2, "changed")
	// No later logical record or confirmation may hide the corruption.
	input <- message.NewMutableMessageBeforeAppend(nil, map[string]string{
		"_v": "1", "_t": strconv.Itoa(int(message.MessageTypeTimeTick)), "_tt": message.EncodeUint64(20),
	}).IntoImmutableMessage(walimplstest.NewTestMessageID(3))
	scanner := mock_walimpls.NewMockScannerImpls(t)
	scanner.EXPECT().Chan().Return(input)
	scanner.EXPECT().Close().Return(nil).Once()
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{AccessMode: types.AccessModeRO})
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner, nil).Once()
	s := newScannerAdaptor("corrupt-chunk", l,
		wal.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(0)), IgnorePauseConsumption: true},
		metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(), func() {}, false)
	t.Cleanup(func() { _ = s.Close() })
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("corrupted chunk did not stop the scanner")
	}
	require.ErrorIs(t, s.Error(), message.ErrCorruptedChunk)
	_, ok := <-s.Chan()
	require.False(t, ok)
	require.ErrorIs(t, s.Close(), message.ErrCorruptedChunk)
}

func TestScannerConfirmationBarriersDeliverBufferedRows(t *testing.T) {
	for _, kind := range []message.MessageType{message.MessageTypeTimeTick, message.MessageTypeMVCCBarrier, message.MessageTypeRecoveryBarrier} {
		t.Run(kind.String(), func(t *testing.T) {
			metrics := metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()
			t.Cleanup(metrics.Close)
			scanner := &scannerAdaptorImpl{
				logger: mlog.With(), reorderBuffer: utility.NewReOrderBuffer(), pendingQueue: utility.NewPendingQueue(),
				txnBuffer: utility.NewTxnBuffer(mlog.With(), metrics), metrics: metrics,
				readRateCounter: utility.NewAverageRateCounter(time.Second),
			}
			row := message.NewInsertMessageBuilderV2().
				WithVChannel("test_1v0").WithHeader(&message.InsertMessageHeaderV2{}).
				WithBody(&message.InsertMessageBody{}).MustBuildMutable().
				WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
			var mutable message.MutableMessage
			switch kind {
			case message.MessageTypeTimeTick:
				mutable = message.NewTimeTickMessageBuilderV1().WithAllVChannel().
					WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable()
			case message.MessageTypeMVCCBarrier:
				mutable = message.NewMVCCBarrierMessageBuilderV2().WithAllVChannel().
					WithHeader(&message.MVCCBarrierMessageHeader{}).WithBody(&message.MVCCBarrierMessageBody{}).MustBuildMutable()
			case message.MessageTypeRecoveryBarrier:
				mutable = message.NewRecoveryBarrierMessageBuilderV2().WithAllVChannel().
					WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable()
			}
			barrier := mutable.WithTimeTick(20).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(2))

			scanner.handleUpstream(row)
			require.Equal(t, 1, scanner.reorderBuffer.Len())
			require.Zero(t, scanner.pendingQueue.Len())
			scanner.handleUpstream(barrier)
			require.Zero(t, scanner.reorderBuffer.Len())
			require.Equal(t, 2, scanner.pendingQueue.Len())
			require.Same(t, row, scanner.pendingQueue.Next())
			scanner.pendingQueue.UnsafeAdvance()
			require.Same(t, barrier, scanner.pendingQueue.Next())
		})
	}
}
