package producer

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newProducerMetrics creates a new producer metrics.
func newProducerMetrics(pchannel types.PChannelInfo) *producerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel.Name,
	}
	pm := &producerMetrics{
		produceTotal:  metrics.StreamingNodeProducerTotal.With(constLabel),
		inflightTotal: metrics.StreamingNodeProduceInflightTotal.With(constLabel),
	}
	pm.produceTotal.Inc()
	return pm
}

// producerMetrics is the metrics for producer.
type producerMetrics struct {
	produceTotal  prometheus.Gauge
	inflightTotal prometheus.Gauge
}

// StartProduce starts the produce metrics.
func (m *producerMetrics) StartProduce() produceMetricsGuard {
	m.inflightTotal.Inc()
	return produceMetricsGuard{
		metrics: m,
	}
}

// Close closes the producer metrics.
func (m *producerMetrics) Close() {
	m.produceTotal.Dec()
}

// produceMetricsGuard is the guard for produce metrics.
type produceMetricsGuard struct {
	metrics *producerMetrics
}

// Finish finishes the produce metrics.
func (g produceMetricsGuard) Finish(err error) {
	g.metrics.inflightTotal.Dec()
}
