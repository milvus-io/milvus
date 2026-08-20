package recovery

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoveryMetricsObserveIdempotencyPersist(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "metrics-idempotency-pchannel", Term: 7}
	m := newRecoveryStorageMetrics(channel)
	defer m.Close()

	nodeID := paramtable.GetStringNodeID()
	term := "7"
	successCounter := pkgmetrics.WALIdempotencyPersistTotal.WithLabelValues(nodeID, channel.Name, term, pkgmetrics.SuccessLabel)
	failCounter := pkgmetrics.WALIdempotencyPersistTotal.WithLabelValues(nodeID, channel.Name, term, pkgmetrics.FailLabel)
	lagGauge := pkgmetrics.WALIdempotencyPersistWatermarkLag.WithLabelValues(nodeID, channel.Name, term)
	successBefore := testutil.ToFloat64(successCounter)
	failBefore := testutil.ToFloat64(failCounter)

	m.ObserveIdempotencyPersist(true)
	m.ObserveIdempotencyPersist(false)
	m.ObserveIdempotencyPersistWatermarkLag(3.5)

	require.Equal(t, successBefore+1, testutil.ToFloat64(successCounter))
	require.Equal(t, failBefore+1, testutil.ToFloat64(failCounter))
	require.Equal(t, 3.5, testutil.ToFloat64(lagGauge))
}
