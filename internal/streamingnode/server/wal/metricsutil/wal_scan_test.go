//go:build test

package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestScannerMetricsSwitchReaderInfo(t *testing.T) {
	paramtable.Init()
	scanMetrics := NewScanMetrics(types.PChannelInfo{Name: "reader-metrics-test"})
	defer scanMetrics.Close()

	scannerMetrics := scanMetrics.NewScannerMetrics()
	scannerMetrics.SetReaderInfo(message.WALNamePulsar, metrics.WALReaderRoleHistorical)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.activeReaders.WithLabelValues(message.WALNamePulsar.String(), metrics.WALReaderRoleHistorical),
	))

	scannerMetrics.SwitchReaderInfo(message.WALNameWoodpecker, metrics.WALReaderRoleCurrent)
	assert.Equal(t, float64(0), testutil.ToFloat64(
		scannerMetrics.activeReaders.WithLabelValues(message.WALNamePulsar.String(), metrics.WALReaderRoleHistorical),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		scannerMetrics.activeReaders.WithLabelValues(message.WALNameWoodpecker.String(), metrics.WALReaderRoleCurrent),
	))

	scannerMetrics.Close()
	assert.Equal(t, float64(0), testutil.ToFloat64(
		scannerMetrics.activeReaders.WithLabelValues(message.WALNameWoodpecker.String(), metrics.WALReaderRoleCurrent),
	))
}
