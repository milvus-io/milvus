package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/mock_wab"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestScannerAdaptorReadError(t *testing.T) {
	resource.InitForTest(t)

	sig1 := lifetime.NewSafeChan()
	sig2 := lifetime.NewSafeChan()
	backoffTime := atomic.NewInt32(0)

	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	operator.EXPECT().Channel().Return(types.PChannelInfo{})
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Run(func(ctx context.Context, forcePersisted bool) {
		sig1.Close()
	})
	wb := mock_wab.NewMockROWriteAheadBuffer(t)
	operator.EXPECT().WriteAheadBuffer().Return(wb)
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)

	err := errors.New("read error")
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ walimpls.ReadOption) (walimpls.ScannerImpls, error) {
		if backoffTime.Inc() > 1 {
			sig2.Close()
		}
		return nil, err
	})
	l.EXPECT().Channel().Return(types.PChannelInfo{})

	s := newScannerAdaptor("scanner", l,
		wal.ReadOption{
			VChannel:      "test",
			DeliverPolicy: options.DeliverPolicyAll(),
			MessageFilter: nil,
		},
		metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
		func() {})
	// wait for timetick inspector first round
	<-sig1.CloseCh()
	// wait for scanner backoff 2 rounds
	<-sig2.CloseCh()
	s.Close()
	<-s.Chan()
	<-s.Done()
	assert.NoError(t, s.Error())
}

func TestScannerAdaptorWaitsForTimeTickOperator(t *testing.T) {
	resource.InitForTest(t)

	pchannel := types.PChannelInfo{Name: "test-pchannel", AccessMode: types.AccessModeRW}
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(pchannel)
	scanner := &scannerAdaptorImpl{
		logger:        mlog.With(),
		innerWAL:      l,
		ScannerHelper: helper.NewScannerHelper("test"),
	}

	done := make(chan wab.ROWriteAheadBuffer, 1)
	go func() {
		wb, err := scanner.waitWriteAheadBuffer()
		assert.NoError(t, err)
		done <- wb
	}()

	select {
	case <-done:
		t.Fatal("write ahead buffer should wait until timetick operator is registered")
	case <-time.After(50 * time.Millisecond):
	}

	wb := mock_wab.NewMockROWriteAheadBuffer(t)
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	operator.EXPECT().Channel().Return(pchannel)
	operator.EXPECT().WriteAheadBuffer().Return(wb)
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Maybe()
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)
	defer resource.Resource().TimeTickInspector().UnregisterSyncOperator(operator)

	select {
	case got := <-done:
		assert.Equal(t, wb, got)
	case <-time.After(time.Second):
		t.Fatal("wait write ahead buffer timeout")
	}
}

func TestPauseConsumption(t *testing.T) {
	configKey := paramtable.Get().StreamingCfg.WALScannerPauseConsumption.Key
	paramtable.Get().Save(configKey, "true")
	defer paramtable.Get().Reset(configKey)

	scanner := &scannerAdaptorImpl{
		logger: mlog.With(),
		readOption: wal.ReadOption{
			IgnorePauseConsumption: false,
		},
		filterFunc:    func(message.ImmutableMessage) bool { return true },
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewPendingQueue(),
		cleanup:       func() {},
		ScannerHelper: helper.NewScannerHelper("test"),
		metrics:       metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
	}

	done := make(chan struct{})

	go func() {
		scanner.waitUntilStartConsumption()
		close(done)
	}()

	// Wait a bit then set the param to false
	time.Sleep(50 * time.Millisecond)

	paramtable.Get().Save(configKey, "false")
	// Manually trigger event dispatch since Save() doesn't dispatch events
	paramtable.GetBaseTable().Manager().OnEvent(&config.Event{
		Key:         configKey,
		Value:       "false",
		EventType:   config.UpdateType,
		EventSource: "test",
	})

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("wait until start consumption timeout")
	}
}

func TestRecoveryBarrierConfirmsBufferedMessages(t *testing.T) {
	scanner := &scannerAdaptorImpl{
		logger: mlog.With(),
		readOption: wal.ReadOption{
			IgnorePauseConsumption: true,
		},
		filterFunc:      func(message.ImmutableMessage) bool { return true },
		reorderBuffer:   utility.NewReOrderBuffer(),
		pendingQueue:    utility.NewPendingQueue(),
		txnBuffer:       utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()),
		cleanup:         func() {},
		ScannerHelper:   helper.NewScannerHelper("test"),
		metrics:         metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
		readRateCounter: utility.NewAverageRateCounter(time.Second),
	}
	msg := newScannerTestMessage(t, 10, "v1", message.MessageTypeInsert, false)
	barrier := newScannerTestMessage(t, 20, "", message.MessageTypeRecoveryBarrier, true)

	scanner.handleUpstream(msg)
	assert.Equal(t, 1, scanner.reorderBuffer.Len())
	assert.Equal(t, 0, scanner.pendingQueue.Len())

	scanner.handleUpstream(barrier)

	assert.Equal(t, 0, scanner.reorderBuffer.Len())
	assert.Equal(t, 2, scanner.pendingQueue.Len())
	assert.Equal(t, msg, scanner.pendingQueue.Next())
	scanner.pendingQueue.UnsafeAdvance()
	assert.Equal(t, barrier, scanner.pendingQueue.Next())
}

func newScannerTestMessage(
	t *testing.T,
	timetick uint64,
	vchannel string,
	msgType message.MessageType,
	persisted bool,
) *mock_message.MockImmutableMessage {
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().MessageType().Return(msgType).Maybe()
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().VChannel().Return(vchannel).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	msg.EXPECT().IsPersisted().Return(persisted).Maybe()
	msg.EXPECT().MessageID().Return(walimplstest.NewTestMessageID(int64(timetick))).Maybe()
	msg.EXPECT().MarshalLogObject(mock.Anything).Return(nil).Maybe()
	return msg
}
