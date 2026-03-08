package metricsutil

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const labelExpired = "expired"

func NewTxnMetrics(pchannel string) *TxnMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &TxnMetrics{
		mu:               syncutil.ClosableLock{},
		constLabel:       constLabel,
		inflightTxnGauge: metrics.WALInflightTxn.With(constLabel),
		duration:         metrics.WALTxnDurationSeconds.MustCurryWith(constLabel),
	}
}

type TxnMetrics struct {
	mu               syncutil.ClosableLock
	constLabel       prometheus.Labels
	inflightTxnGauge prometheus.Gauge
	duration         prometheus.ObserverVec
}

func (m *TxnMetrics) BeginTxn() *TxnMetricsGuard {
	if !m.mu.LockIfNotClosed() {
		return nil
	}
	m.inflightTxnGauge.Inc()
	m.mu.Unlock()

	return &TxnMetricsGuard{
		inner: m,
		start: time.Now(),
	}
}

type TxnMetricsGuard struct {
	inner *TxnMetrics
	start time.Time
}

func (g *TxnMetricsGuard) Done(state message.TxnState) {
	if g == nil {
		return
	}
	if !g.inner.mu.LockIfNotClosed() {
		return
	}
	g.inner.inflightTxnGauge.Dec()

	s := labelExpired
	if state == message.TxnStateRollbacked || state == message.TxnStateCommitted {
		s = state.String()
	}
	g.inner.duration.WithLabelValues(s).Observe(time.Since(g.start).Seconds())
	g.inner.mu.Unlock()
}

func (m *TxnMetrics) Close() {
	m.mu.Close()
	metrics.WALInflightTxn.Delete(m.constLabel)
	metrics.WALTxnDurationSeconds.DeletePartialMatch(m.constLabel)
}
