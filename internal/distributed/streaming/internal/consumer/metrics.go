package consumer

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newConsumerMetrics creates a new producer metrics.
func newConsumerMetrics(pchannel string) *consumerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	m := &consumerMetrics{
		available:     false,
		clientTotal:   metrics.StreamingServiceClientConsumerTotal.MustCurryWith(constLabel),
		inflightTotal: metrics.StreamingServiceClientConsumeInflightTotal.With(constLabel),
		bytes:         metrics.StreamingServiceClientConsumeBytes.With(constLabel),
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	return m
}

// consumerMetrics is the metrics for producer.
type consumerMetrics struct {
	available     bool
	clientTotal   *prometheus.GaugeVec
	inflightTotal prometheus.Gauge
	bytes         prometheus.Observer
}

// IntoUnavailable sets the producer metrics to unavailable.
func (m *consumerMetrics) IntoUnavailable() {
	if !m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	m.available = false
}

// IntoAvailable sets the producer metrics to available.
func (m *consumerMetrics) IntoAvailable() {
	if m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Inc()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	m.available = true
}

// StartConsume starts a consume operation.
func (m *consumerMetrics) StartConsume(bytes int) consumerMetricsGuard {
	m.inflightTotal.Inc()
	return consumerMetricsGuard{
		metrics: m,
		bytes:   bytes,
	}
}

func (m *consumerMetrics) Close() {
	if m.available {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	} else {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	}
}

type consumerMetricsGuard struct {
	metrics *consumerMetrics
	bytes   int
}

func (g consumerMetricsGuard) Finish() {
	g.metrics.inflightTotal.Dec()
	g.metrics.bytes.Observe(float64(g.bytes))
}
