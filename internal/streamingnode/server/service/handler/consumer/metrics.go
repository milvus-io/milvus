package consumer

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newConsumerMetrics creates a new consumer metrics.
func newConsumerMetrics(pchannel string) *consumerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	m := &consumerMetrics{
		scannerTotal:  metrics.StreamingNodeConsumerTotal.With(constLabel),
		inflightTotal: metrics.StreamingNodeConsumeInflightTotal.With(constLabel),
		bytes:         metrics.StreamingNodeConsumeBytes.With(constLabel),
	}
	m.scannerTotal.Inc()
	return m
}

// consumerMetrics is the metrics for consumer.
type consumerMetrics struct {
	scannerTotal  prometheus.Gauge
	inflightTotal prometheus.Gauge
	bytes         prometheus.Observer
}

// StartConsume starts a consume operation.
func (m *consumerMetrics) StartConsume(bytes int) consumerMetricsGuard {
	m.inflightTotal.Inc()
	return consumerMetricsGuard{
		metrics: m,
		bytes:   bytes,
	}
}

// Close closes the consumer metrics.
func (m *consumerMetrics) Close() {
	m.scannerTotal.Dec()
}

// consumerMetricsGuard is a guard for consumer metrics.
type consumerMetricsGuard struct {
	metrics *consumerMetrics
	bytes   int
}

// Finish finishes the consume operation.
func (g consumerMetricsGuard) Finish(err error) {
	g.metrics.inflightTotal.Dec()
	if err == nil {
		g.metrics.bytes.Observe(float64(g.bytes))
	}
}
