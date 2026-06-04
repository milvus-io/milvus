package adaptor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
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

func TestPauseConsumption(t *testing.T) {
	configKey := paramtable.Get().StreamingCfg.WALScannerPauseConsumption.Key
	paramtable.Get().Save(configKey, "true")
	defer paramtable.Get().Reset(configKey)

	scanner := &scannerAdaptorImpl{
		logger: log.With(),
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

func TestScannerAdaptorObservesPhysicalDedupDropFromPushResult(t *testing.T) {
	paramtable.Init()
	pchannel := types.PChannelInfo{Name: "scanner-dedup-pchannel"}
	scanMetrics := metricsutil.NewScanMetrics(pchannel)
	defer scanMetrics.Close()
	scannerMetrics := scanMetrics.NewScannerMetrics()

	scanner := &scannerAdaptorImpl{
		logger:          log.With(),
		reorderBuffer:   utility.NewReOrderBuffer(),
		pendingQueue:    utility.NewPendingQueue(),
		txnBuffer:       utility.NewTxnBuffer(log.With(), scannerMetrics),
		readRateCounter: utility.NewAverageRateCounter(time.Second),
		metrics:         scannerMetrics,
	}

	counter := metrics.WALIdempotencyReaderDedupDropTotal.WithLabelValues(
		paramtable.GetStringNodeID(),
		pchannel.Name,
		metrics.WALScannerModelCatchup,
	)
	before := testutil.ToFloat64(counter)

	scanner.handleUpstream(newScannerAdaptorTestMessage(t, 1, 10, "v1", message.MessageTypeInsert))
	scanner.handleUpstream(newScannerAdaptorTestMessage(t, 2, 10, "v1", message.MessageTypeInsert))

	require.Equal(t, before+1, testutil.ToFloat64(counter))
}

func newScannerAdaptorTestMessage(t *testing.T, id int64, timetick uint64, vchannel string, msgType message.MessageType) *mock_message.MockImmutableMessage {
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().MessageID().Return(scannerAdaptorTestMessageID(id)).Maybe()
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().MessageType().Return(msgType).Maybe()
	msg.EXPECT().VChannel().Return(vchannel).Maybe()
	msg.EXPECT().IsPersisted().Return(false).Maybe()
	return msg
}

type scannerAdaptorTestMessageID int64

func (id scannerAdaptorTestMessageID) WALName() message.WALName {
	return message.WALNameTest
}

func (id scannerAdaptorTestMessageID) LT(other message.MessageID) bool {
	return id < other.(scannerAdaptorTestMessageID)
}

func (id scannerAdaptorTestMessageID) LTE(other message.MessageID) bool {
	return id <= other.(scannerAdaptorTestMessageID)
}

func (id scannerAdaptorTestMessageID) EQ(other message.MessageID) bool {
	return id == other.(scannerAdaptorTestMessageID)
}

func (id scannerAdaptorTestMessageID) Marshal() string {
	return strconv.FormatInt(int64(id), 10)
}

func (id scannerAdaptorTestMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      id.Marshal(),
		WALName: commonpb.WALName(id.WALName()),
	}
}

func (id scannerAdaptorTestMessageID) String() string {
	return id.Marshal()
}
