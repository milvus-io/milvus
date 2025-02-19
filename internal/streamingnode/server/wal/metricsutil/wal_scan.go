package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func NewScanMetrics(pchannel types.PChannelInfo) *ScanMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel.Name,
	}
	return &ScanMetrics{
		constLabel:             constLabel,
		messageBytes:           metrics.WALScanMessageBytes.With(constLabel),
		passMessageBytes:       metrics.WALScanPassMessageBytes.With(constLabel),
		messageTotal:           metrics.WALScanMessageTotal.MustCurryWith(constLabel),
		passMessageTotal:       metrics.WALScanPassMessageTotal.MustCurryWith(constLabel),
		timeTickViolationTotal: metrics.WALScanTimeTickViolationMessageTotal.MustCurryWith(constLabel),
		txnTotal:               metrics.WALScanTxnTotal.MustCurryWith(constLabel),
		pendingQueueSize:       metrics.WALScannerPendingQueueBytes.With(constLabel),
		timeTickBufSize:        metrics.WALScannerTimeTickBufBytes.With(constLabel),
		txnBufSize:             metrics.WALScannerTxnBufBytes.With(constLabel),
	}
}

type ScanMetrics struct {
	constLabel             prometheus.Labels
	messageBytes           prometheus.Observer
	passMessageBytes       prometheus.Observer
	messageTotal           *prometheus.CounterVec
	passMessageTotal       *prometheus.CounterVec
	timeTickViolationTotal *prometheus.CounterVec
	txnTotal               *prometheus.CounterVec
	timeTickBufSize        prometheus.Gauge
	txnBufSize             prometheus.Gauge
	pendingQueueSize       prometheus.Gauge
}

// ObserveMessage observes the message.
func (m *ScanMetrics) ObserveMessage(msgType message.MessageType, bytes int) {
	m.messageBytes.Observe(float64(bytes))
	m.messageTotal.WithLabelValues(msgType.String()).Inc()
}

// ObserveFilteredMessage observes the filtered message.
func (m *ScanMetrics) ObserveFilteredMessage(msgType message.MessageType, bytes int) {
	m.passMessageBytes.Observe(float64(bytes))
	m.passMessageTotal.WithLabelValues(msgType.String()).Inc()
}

// ObserveTimeTickViolation observes the time tick violation.
func (m *ScanMetrics) ObserveTimeTickViolation(msgType message.MessageType) {
	m.timeTickViolationTotal.WithLabelValues(msgType.String()).Inc()
}

// ObserveAutoCommitTxn observes the auto commit txn.
func (m *ScanMetrics) ObserveAutoCommitTxn() {
	m.txnTotal.WithLabelValues("autocommit").Inc()
}

// ObserveTxn observes the txn.
func (m *ScanMetrics) ObserveTxn(state message.TxnState) {
	m.txnTotal.WithLabelValues(state.String()).Inc()
}

// ObserveErrorTxn observes the error txn.
func (m *ScanMetrics) ObserveErrorTxn() {
	m.txnTotal.WithLabelValues("error").Inc()
}

// ObserveExpiredTxn observes the expired txn.
func (m *ScanMetrics) ObserveExpiredTxn() {
	m.txnTotal.WithLabelValues("expired").Inc()
}

// NewScannerMetrics creates a new scanner metrics.
func (m *ScanMetrics) NewScannerMetrics() *ScannerMetrics {
	return &ScannerMetrics{
		ScanMetrics:              m,
		previousTxnBufSize:       0,
		previousTimeTickBufSize:  0,
		previousPendingQueueSize: 0,
	}
}

// Close closes the metrics.
func (m *ScanMetrics) Close() {
	metrics.WALScanMessageBytes.Delete(m.constLabel)
	metrics.WALScanPassMessageBytes.DeletePartialMatch(m.constLabel)
	metrics.WALScanMessageTotal.DeletePartialMatch(m.constLabel)
	metrics.WALScanPassMessageTotal.DeletePartialMatch(m.constLabel)
	metrics.WALScanTimeTickViolationMessageTotal.DeletePartialMatch(m.constLabel)
	metrics.WALScanTxnTotal.DeletePartialMatch(m.constLabel)
	metrics.WALScannerTimeTickBufBytes.Delete(m.constLabel)
	metrics.WALScannerTxnBufBytes.Delete(m.constLabel)
	metrics.WALScannerPendingQueueBytes.Delete(m.constLabel)
}

type ScannerMetrics struct {
	*ScanMetrics
	previousTxnBufSize       int
	previousTimeTickBufSize  int
	previousPendingQueueSize int
}

func (m *ScannerMetrics) UpdatePendingQueueSize(size int) {
	diff := size - m.previousPendingQueueSize
	m.pendingQueueSize.Add(float64(diff))
	m.previousPendingQueueSize = size
}

func (m *ScannerMetrics) UpdateTxnBufSize(size int) {
	diff := size - m.previousTimeTickBufSize
	m.timeTickBufSize.Add(float64(diff))
	m.previousTimeTickBufSize = size
}

func (m *ScannerMetrics) UpdateTimeTickBufSize(size int) {
	diff := size - m.previousTxnBufSize
	m.txnBufSize.Add(float64(diff))
	m.previousTxnBufSize = size
}

func (m *ScannerMetrics) Close() {
	m.UpdatePendingQueueSize(0)
	m.UpdateTimeTickBufSize(0)
	m.UpdateTimeTickBufSize(0)
}
