package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func NewTxnMetrics(pchannel string) *TxnMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &TxnMetrics{
		mu:               syncutil.ClosableLock{},
		constLabel:       constLabel,
		inflightTxnGauge: metrics.WALInflightTxn.With(constLabel),
		txnCounter:       metrics.WALFinishTxn.MustCurryWith(constLabel),
	}
}

type TxnMetrics struct {
	mu               syncutil.ClosableLock
	constLabel       prometheus.Labels
	inflightTxnGauge prometheus.Gauge
	txnCounter       *prometheus.CounterVec
}

func (m *TxnMetrics) BeginTxn() {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.inflightTxnGauge.Inc()
	m.mu.Unlock()
}

func (m *TxnMetrics) Finish(state message.TxnState) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.inflightTxnGauge.Dec()
	m.txnCounter.WithLabelValues(state.String()).Inc()
	m.mu.Unlock()
}

func (m *TxnMetrics) Close() {
	m.mu.Close()
	metrics.WALInflightTxn.Delete(m.constLabel)
	metrics.WALFinishTxn.DeletePartialMatch(m.constLabel)
}
