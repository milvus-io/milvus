package producer

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newResumingProducerMetrics creates a new producer metrics.
func newResumingProducerMetrics(pchannel string) *resumingProducerMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	m := &resumingProducerMetrics{
		available:   false,
		clientTotal: metrics.StreamingServiceClientResumingProducerTotal.MustCurryWith(constLabel),
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	return m
}

// resumingProducerMetrics is the metrics for producer.
type resumingProducerMetrics struct {
	available   bool
	clientTotal *prometheus.GaugeVec
}

// IntoUnavailable sets the producer metrics to unavailable.
func (m *resumingProducerMetrics) IntoUnavailable() {
	if !m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Inc()
	m.available = false
}

// IntoAvailable sets the producer metrics to available.
func (m *resumingProducerMetrics) IntoAvailable() {
	if m.available {
		return
	}
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Inc()
	m.available = true
}

// Close closes the producer metrics.
func (m *resumingProducerMetrics) Close() {
	if m.available {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusAvailable).Dec()
	} else {
		m.clientTotal.WithLabelValues(metrics.StreamingServiceClientStatusUnavailable).Dec()
	}
}

func newProducerMetrics(pchannel string, isLocal bool) *producerMetrics {
	accessModel := metrics.WALAccessModelRemote
	if isLocal {
		accessModel = metrics.WALAccessModelLocal
	}
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     pchannel,
		metrics.WALAccessModelLabelName: accessModel,
	}
	m := &producerMetrics{
		producerTotal:                 metrics.StreamingServiceClientProducerTotal.With(constLabel),
		produceTotal:                  metrics.StreamingServiceClientProduceTotal.MustCurryWith(constLabel),
		produceBytes:                  metrics.StreamingServiceClientProduceBytes.MustCurryWith(constLabel),
		produceSuccessBytes:           metrics.StreamingServiceClientSuccessProduceBytes.With(constLabel),
		produceSuccessDurationSeconds: metrics.StreamingServiceClientSuccessProduceDurationSeconds.With(constLabel),
	}
	m.producerTotal.Inc()
	return m
}

// producerMetrics is the metrics for producer.
type producerMetrics struct {
	producerTotal                 prometheus.Gauge
	produceTotal                  *prometheus.CounterVec
	produceBytes                  *prometheus.CounterVec
	produceSuccessBytes           prometheus.Observer
	produceSuccessDurationSeconds prometheus.Observer
}

// StartProduce starts the produce metrics.
func (m *producerMetrics) StartProduce(bytes int) produceMetricsGuard {
	return produceMetricsGuard{
		start:   time.Now(),
		bytes:   bytes,
		metrics: m,
	}
}

func (m *producerMetrics) Close() {
	m.producerTotal.Dec()
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
	g.metrics.produceTotal.WithLabelValues(status).Inc()
	g.metrics.produceBytes.WithLabelValues(status).Add(float64(g.bytes))
	g.metrics.produceSuccessBytes.Observe(float64(g.bytes))
	g.metrics.produceSuccessDurationSeconds.Observe(time.Since(g.start).Seconds())
}

// parseError parses the error to status.
func parseError(err error) string {
	if err == nil {
		return metrics.WALStatusOK
	}
	if status.IsCanceled(err) {
		return metrics.WALStatusCancel
	}
	return metrics.WALStatusError
}

func newProducerWithMetrics(channel string, p handler.Producer) handler.Producer {
	if p == nil {
		return nil
	}
	return producerWithMetrics{
		Producer: p,
		metrics:  newProducerMetrics(channel, registry.IsLocal(p)),
	}
}

type producerWithMetrics struct {
	handler.Producer
	metrics *producerMetrics
}

func (pm producerWithMetrics) Append(ctx context.Context, msg message.MutableMessage) (result *types.AppendResult, err error) {
	g := pm.metrics.StartProduce(msg.EstimateSize())
	defer func() {
		g.Finish(err)
	}()

	return pm.Producer.Append(ctx, msg)
}

func (pm producerWithMetrics) Close() {
	pm.Producer.Close()
	pm.metrics.Close()
}
