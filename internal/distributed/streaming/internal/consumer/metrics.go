package consumer

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newConsumerMetrics creates a new producer metrics.
func newConsumerMetrics(pchannel string) *resumingConsumerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	m := &resumingConsumerMetrics{
		available:           false,
		resumingClientTotal: metrics.StreamingServiceClientResumingConsumerTotal.MustCurryWith(constLabel),
		bytes:               metrics.StreamingServiceClientConsumeBytes.With(constLabel),
	}
	m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	return m
}

// resumingConsumerMetrics is the metrics for producer.
type resumingConsumerMetrics struct {
	available           bool
	resumingClientTotal *prometheus.GaugeVec
	bytes               prometheus.Observer
}

// IntoUnavailable sets the producer metrics to unavailable.
func (m *resumingConsumerMetrics) IntoUnavailable() {
	if !m.available {
		return
	}
	m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	m.available = false
}

// IntoAvailable sets the producer metrics to available.
func (m *resumingConsumerMetrics) IntoAvailable() {
	if m.available {
		return
	}
	m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Inc()
	m.available = true
}

// StartConsume starts a consume operation.
func (m *resumingConsumerMetrics) StartConsume(bytes int) consumerMetricsGuard {
	return consumerMetricsGuard{
		metrics: m,
		bytes:   bytes,
	}
}

func (m *resumingConsumerMetrics) Close() {
	if m.available {
		m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	} else {
		m.resumingClientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	}
}

type consumerMetricsGuard struct {
	metrics *resumingConsumerMetrics
	bytes   int
}

func (g consumerMetricsGuard) Finish() {
	g.metrics.bytes.Observe(float64(g.bytes))
}

func newConsumerWithMetrics(pchannel string, c consumer.Consumer) consumer.Consumer {
	accessModel := metrics.WALAccessModelRemote
	if registry.IsLocal(c) {
		accessModel = metrics.WALAccessModelLocal
	}
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     pchannel,
		metrics.WALAccessModelLabelName: accessModel,
	}
	cm := &consumerWithMetrics{
		Consumer:    c,
		clientTotal: metrics.StreamingServiceClientConsumerTotal.With(constLabel),
	}
	cm.clientTotal.Inc()
	return cm
}

type consumerWithMetrics struct {
	consumer.Consumer
	clientTotal prometheus.Gauge
}

func (c *consumerWithMetrics) Close() error {
	err := c.Consumer.Close()
	c.clientTotal.Dec()
	return err
}
