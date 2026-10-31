package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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

func TestScannerMetricsSetReaderWALName(t *testing.T) {
	paramtable.Init()
	scanMetrics := NewScanMetrics(types.PChannelInfo{Name: "reader-metrics-test"})
	defer scanMetrics.Close()

	scannerMetrics := scanMetrics.NewConsumerScannerMetrics("v1", "reader-1")
	scannerMetrics.SetReaderWALName(message.WALNamePulsar)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.readerInfo.WithLabelValues("v1", "reader-1", message.WALNamePulsar.String()),
	))
	assert.Equal(t, 1, testutil.CollectAndCount(scannerMetrics.readerInfo))

	scannerMetrics.SetReaderWALName(message.WALNameWoodpecker)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.readerInfo.WithLabelValues("v1", "reader-1", message.WALNameWoodpecker.String()),
	))
	assert.Equal(t, 1, testutil.CollectAndCount(scannerMetrics.readerInfo))

	scannerMetrics.Close()
	assert.Equal(t, 0, testutil.CollectAndCount(scannerMetrics.readerInfo))
}
