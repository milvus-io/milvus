//go:build test

package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestScannerMetricsSetReaderWALName(t *testing.T) {
	paramtable.Init()
	scanMetrics := NewScanMetrics(types.PChannelInfo{Name: "reader-metrics-test"})
	defer scanMetrics.Close()

	scannerMetrics := scanMetrics.NewConsumerScannerMetrics("v1", "reader-1")
	scannerMetrics.SetReaderWALName(message.WALNamePulsar)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.consumerReaderInfo.WithLabelValues("v1", "reader-1", message.WALNamePulsar.String()),
	))
	assert.Equal(t, 1, testutil.CollectAndCount(scannerMetrics.consumerReaderInfo))

	scannerMetrics.SetReaderWALName(message.WALNameWoodpecker)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.consumerReaderInfo.WithLabelValues("v1", "reader-1", message.WALNameWoodpecker.String()),
	))
	assert.Equal(t, 1, testutil.CollectAndCount(scannerMetrics.consumerReaderInfo))

	scannerMetrics.Close()
	assert.Equal(t, 0, testutil.CollectAndCount(scannerMetrics.consumerReaderInfo))
}
