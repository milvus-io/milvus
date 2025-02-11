package adaptor

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

func TestScannerAdaptorReadError(t *testing.T) {
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
	defer s.Close()
	<-s.Chan()
	<-s.Done()
	assert.ErrorIs(t, s.Error(), err)
}
