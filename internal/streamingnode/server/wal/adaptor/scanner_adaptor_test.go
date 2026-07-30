package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/mock_wab"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
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
	l.EXPECT().WALName().Return(message.WALNameTest)

	s := newScannerAdaptor("scanner", l, l,
		wal.ReadOption{
			VChannel:      "test",
			DeliverPolicy: options.DeliverPolicyAll(),
			MessageFilter: nil,
		},
		metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
		func() {}, false)
	// wait for timetick inspector first round
	<-sig1.CloseCh()
	// wait for scanner backoff 2 rounds
	<-sig2.CloseCh()
	s.Close()
	<-s.Chan()
	<-s.Done()
	assert.NoError(t, s.Error())
}

func TestScannerAdaptorPropagatesHistoricalMigrationError(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: "test-channel", AccessMode: types.AccessModeRO}

	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()

	messageCh := make(chan message.ImmutableMessage, 1)
	messageCh <- newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(innerScanner, nil).Once()

	scanner := newScannerAdaptor(
		"historical-error",
		currentWAL,
		historicalWAL,
		wal.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1))},
		metricsutil.NewScanMetrics(channel).NewScannerMetrics(),
		func() {},
		false,
	)

	select {
	case <-scanner.Done():
	case <-time.After(time.Second):
		t.Fatal("scanner did not stop after an unsupported WAL migration chain")
	}
	err := scanner.Error()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported WAL migration chain")
	assert.Error(t, scanner.Close())
}

func TestROWALReadBridgesHistoricalAndCurrentWAL(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: "cross-wal-test", AccessMode: types.AccessModeRO}

	currentMessages := make(chan message.ImmutableMessage, 2)
	currentMessages <- newTestTimeTickMessage(100, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentMessages <- newTestTimeTickMessage(101, walimplstest.NewTestMessageID(2), walimplstest.NewTestMessageID(2))
	currentScanner := mock_walimpls.NewMockScannerImpls(t)
	currentScanner.EXPECT().Chan().Return(currentMessages).Maybe()
	currentScanner.EXPECT().Close().Return(nil).Once()

	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()
	currentWAL.EXPECT().Close().Return().Once()
	currentWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
		return ok
	})).Return(currentScanner, nil).Once()

	historicalMessages := make(chan message.ImmutableMessage, 2)
	historicalMessages <- newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	historicalMessages <- newTestTimeTickMessage(100, rmq.NewRmqID(3), rmq.NewRmqID(2))
	historicalScanner := mock_walimpls.NewMockScannerImpls(t)
	historicalScanner.EXPECT().Chan().Return(historicalMessages).Maybe()
	historicalScanner.EXPECT().Close().Return(nil).Once()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartAfter)
		return ok
	})).Return(historicalScanner, nil).Once()

	roWAL := adaptImplsToROWAL(currentWAL, func() {}, func(
		_ context.Context,
		walName message.WALName,
		gotChannel types.PChannelInfo,
	) (walimpls.ROWALImpls, error) {
		assert.Equal(t, message.WALNameRocksmq, walName)
		assert.Equal(t, channel, gotChannel)
		return historicalWAL, nil
	})
	defer roWAL.Close()

	scanner, err := roWAL.Read(context.Background(), wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyStartAfter(rmq.NewRmqID(1)),
	})
	require.NoError(t, err)

	var timeTicks []uint64
	deadline := time.After(time.Second)
	for len(timeTicks) < 2 {
		select {
		case msg := <-scanner.Chan():
			if msg.MessageType() == message.MessageTypeTimeTick {
				timeTicks = append(timeTicks, msg.TimeTick())
			}
		case <-deadline:
			t.Fatalf("timed out waiting for bridged TimeTicks, got %v", timeTicks)
		}
	}
	assert.Equal(t, []uint64{100, 101}, timeTicks)
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
		scanner.waitUntilStartConsumption(context.Background())
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
