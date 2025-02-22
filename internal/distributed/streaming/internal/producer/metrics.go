package producer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newProducerMetrics creates a new producer metrics.
func newProducerMetrics(pchannel string) *producerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	m := &producerMetrics{
		available:       false,
		clientTotal:     metrics.StreamingServiceClientProducerTotal.MustCurryWith(constLabel),
		inflightTotal:   metrics.StreamingServiceClientProduceInflightTotal.With(constLabel),
		bytes:           metrics.StreamingServiceClientProduceBytes.MustCurryWith(constLabel),
		durationSeconds: metrics.StreamingServiceClientProduceDurationSeconds.MustCurryWith(constLabel),
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	return m
}

// producerMetrics is the metrics for producer.
type producerMetrics struct {
	available       bool
	clientTotal     *prometheus.GaugeVec
	inflightTotal   prometheus.Gauge
	bytes           prometheus.ObserverVec
	durationSeconds prometheus.ObserverVec
}

// IntoUnavailable sets the producer metrics to unavailable.
func (m *producerMetrics) IntoUnavailable() {
	if !m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	m.available = false
}

// IntoAvailable sets the producer metrics to available.
func (m *producerMetrics) IntoAvailable() {
	if m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Inc()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	m.available = true
}

// StartProduce starts the produce metrics.
func (m *producerMetrics) StartProduce(bytes int) produceMetricsGuard {
	m.inflightTotal.Inc()
	return produceMetricsGuard{
		start:   time.Now(),
		bytes:   bytes,
		metrics: m,
	}
}

func (m *producerMetrics) Close() {
	if m.available {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	} else {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	}
}

// produceMetricsGuard is the guard for produce metrics.
type produceMetricsGuard struct {
	start   time.Time
	bytes   int
	metrics *producerMetrics
}

// Finish finishes the produce metrics.
func (g produceMetricsGuard) Finish(err error) {
	status := parseError(err)
	g.metrics.bytes.WithLabelValues(status).Observe(float64(g.bytes))
	g.metrics.durationSeconds.WithLabelValues(status).Observe(time.Since(g.start).Seconds())
	g.metrics.inflightTotal.Dec()
}

// parseError parses the error to status.
func parseError(err error) string {
	if err == nil {
		return metrics.StreamingServiceClientStatusOK
	}
	if status.IsCanceled(err) {
		return metrics.StreamingServiceClientStatusCancel
	}
	return metrics.StreamignServiceClientStatusError
}
