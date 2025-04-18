package adaptor

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/mock_wab"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
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
