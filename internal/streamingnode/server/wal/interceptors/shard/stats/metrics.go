package stats

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newMetricsHelper creates a new metrics helper for the WAL segment.
func newMetricsHelper() *metricsHelper {
	return &metricsHelper{
		growingBytesHWM: metrics.WALGrowingSegmentHWMBytes.With(prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}),
		growingBytesLWM: metrics.WALGrowingSegmentLWMBytes.With(prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}),
		growingBytes:    metrics.WALGrowingSegmentBytes.MustCurryWith(prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}),
	}
}

// metricsHelper is a helper struct for managing metrics related to WAL segments.
type metricsHelper struct {
	growingBytesHWM prometheus.Gauge
	growingBytesLWM prometheus.Gauge
	growingBytes    *prometheus.GaugeVec
}

// ObservePChannelBytesUpdate updates the bytes of a pchannel.
func (m *metricsHelper) ObservePChannelBytesUpdate(pchannel string, bytes uint64) {
	if bytes <= 0 {
		metrics.WALGrowingSegmentBytes.DeletePartialMatch(prometheus.Labels{metrics.WALChannelLabelName: pchannel})
		return
	}
	m.growingBytes.WithLabelValues(pchannel).Set(float64(bytes))
}

// ObserveConfigUpdate is a update method for configuration changes.
func (m *metricsHelper) ObserveConfigUpdate(cfg statsConfig) {
	m.growingBytesHWM.Set(float64(cfg.growingBytesHWM))
	m.growingBytesLWM.Set(float64(cfg.growingBytesLWM))
}
