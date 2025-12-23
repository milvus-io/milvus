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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
