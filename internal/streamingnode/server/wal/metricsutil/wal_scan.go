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
	catchupLabel, tailingLabel := make(prometheus.Labels), make(prometheus.Labels)
	for k, v := range constLabel {
		catchupLabel[k] = v
		tailingLabel[k] = v
	}
	catchupLabel[metrics.WALScannerModelLabelName] = metrics.WALScannerModelCatchup
	tailingLabel[metrics.WALScannerModelLabelName] = metrics.WALScannerModelTailing
	return &ScanMetrics{
		constLabel:   constLabel,
		scannerTotal: metrics.WALScannerTotal.MustCurryWith(constLabel),
		tailing: underlyingScannerMetrics{
			messageBytes:           metrics.WALScanMessageBytes.With(tailingLabel),
			passMessageBytes:       metrics.WALScanPassMessageBytes.With(tailingLabel),
			messageTotal:           metrics.WALScanMessageTotal.MustCurryWith(tailingLabel),
			passMessageTotal:       metrics.WALScanPassMessageTotal.MustCurryWith(tailingLabel),
			timeTickViolationTotal: metrics.WALScanTimeTickViolationMessageTotal.MustCurryWith(tailingLabel),
		},
		catchup: underlyingScannerMetrics{
			messageBytes:           metrics.WALScanMessageBytes.With(catchupLabel),
			passMessageBytes:       metrics.WALScanPassMessageBytes.With(catchupLabel),
			messageTotal:           metrics.WALScanMessageTotal.MustCurryWith(catchupLabel),
			passMessageTotal:       metrics.WALScanPassMessageTotal.MustCurryWith(catchupLabel),
			timeTickViolationTotal: metrics.WALScanTimeTickViolationMessageTotal.MustCurryWith(catchupLabel),
		},
		txnTotal:         metrics.WALScanTxnTotal.MustCurryWith(constLabel),
		pendingQueueSize: metrics.WALScannerPendingQueueBytes.With(constLabel),
		timeTickBufSize:  metrics.WALScannerTimeTickBufBytes.With(constLabel),
		txnBufSize:       metrics.WALScannerTxnBufBytes.With(constLabel),
	}
}

type ScanMetrics struct {
	constLabel       prometheus.Labels
	scannerTotal     *prometheus.GaugeVec
	catchup          underlyingScannerMetrics
	tailing          underlyingScannerMetrics
	txnTotal         *prometheus.CounterVec
	timeTickBufSize  prometheus.Gauge
	txnBufSize       prometheus.Gauge
	pendingQueueSize prometheus.Gauge
}

type underlyingScannerMetrics struct {
	messageBytes           prometheus.Observer
	passMessageBytes       prometheus.Observer
	messageTotal           *prometheus.CounterVec
	passMessageTotal       *prometheus.CounterVec
	timeTickViolationTotal *prometheus.CounterVec
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
	m.scannerTotal.WithLabelValues(metrics.WALScannerModelCatchup).Inc()
	return &ScannerMetrics{
		ScanMetrics:              m,
		scannerModel:             metrics.WALScannerModelCatchup,
		previousTxnBufSize:       0,
		previousTimeTickBufSize:  0,
		previousPendingQueueSize: 0,
	}
}

// Close closes the metrics.
func (m *ScanMetrics) Close() {
	metrics.WALScannerTotal.DeletePartialMatch(m.constLabel)
	metrics.WALScanMessageBytes.DeletePartialMatch(m.constLabel)
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
	scannerModel             string
	previousTxnBufSize       int
	previousTimeTickBufSize  int
	previousPendingQueueSize int
}

// SwitchModel switches the scanner model.
func (m *ScannerMetrics) SwitchModel(s string) {
	m.scannerTotal.WithLabelValues(m.scannerModel).Dec()
	m.scannerModel = s
	m.scannerTotal.WithLabelValues(m.scannerModel).Inc()
}

// ObserveMessage observes the message.
func (m *ScannerMetrics) ObserveMessage(tailing bool, msgType message.MessageType, bytes int) {
	underlying := m.catchup
	if tailing {
		underlying = m.tailing
	}
	underlying.messageBytes.Observe(float64(bytes))
	underlying.messageTotal.WithLabelValues(msgType.String()).Inc()
}

// ObservePassedMessage observes the filtered message.
func (m *ScannerMetrics) ObservePassedMessage(tailing bool, msgType message.MessageType, bytes int) {
	underlying := m.catchup
	if tailing {
		underlying = m.tailing
	}
	underlying.passMessageBytes.Observe(float64(bytes))
	underlying.passMessageTotal.WithLabelValues(msgType.String()).Inc()
}

// ObserveTimeTickViolation observes the time tick violation.
func (m *ScannerMetrics) ObserveTimeTickViolation(tailing bool, msgType message.MessageType) {
	underlying := m.catchup
	if tailing {
		underlying = m.tailing
	}
	underlying.timeTickViolationTotal.WithLabelValues(msgType.String()).Inc()
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
	m.scannerTotal.WithLabelValues(m.scannerModel).Dec()
}
