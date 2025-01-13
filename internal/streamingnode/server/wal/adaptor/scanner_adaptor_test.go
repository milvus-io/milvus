package adaptor

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/mock_wab"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

func TestScannerAdaptorReadError(t *testing.T) {
	resource.InitForTest(t)

	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	operator.EXPECT().Channel().Return(types.PChannelInfo{})
	operator.EXPECT().Sync(mock.Anything).Return()
	wb := mock_wab.NewMockROWriteAheadBuffer(t)
	operator.EXPECT().WriteAheadBuffer(mock.Anything).Return(wb, nil)
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)

	err := errors.New("read error")
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, err)
	l.EXPECT().Channel().Return(types.PChannelInfo{})

	s := newScannerAdaptor("scanner", l,
		wal.ReadOption{
			VChannel:      "test",
			DeliverPolicy: options.DeliverPolicyAll(),
			MessageFilter: nil,
		},
		metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
		func() {})
	time.Sleep(200 * time.Millisecond)
	s.Close()
	<-s.Chan()
	<-s.Done()
	assert.NoError(t, s.Error())
}
