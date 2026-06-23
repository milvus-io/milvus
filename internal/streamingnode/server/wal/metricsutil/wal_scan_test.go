package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestScannerMetricsObservePhysicalDedupDrop(t *testing.T) {
	paramtable.Init()
	pchannel := types.PChannelInfo{Name: "metrics-reader-dedup-pchannel"}
	scanMetrics := NewScanMetrics(pchannel)
	defer scanMetrics.Close()
	scannerMetrics := scanMetrics.NewScannerMetrics()

	counter := metrics.WALIdempotencyReaderDedupDropTotal.WithLabelValues(
		paramtable.GetStringNodeID(),
		pchannel.Name,
		metrics.WALScannerModelTailing,
	)
	before := testutil.ToFloat64(counter)

	scannerMetrics.ObservePhysicalDedupDrop(true)

	require.Equal(t, before+1, testutil.ToFloat64(counter))
}
