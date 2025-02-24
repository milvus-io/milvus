package metricsutil

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// NewWriteMetrics creates a new WriteMetrics.
func NewWriteMetrics(pchannel types.PChannelInfo, walName string) *WriteMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel.Name,
	}
	metrics.WALInfo.WithLabelValues(
		paramtable.GetStringNodeID(),
		pchannel.Name,
		strconv.FormatInt(pchannel.Term, 10),
		walName).Set(1)

	return &WriteMetrics{
		walName:          walName,
		pchannel:         pchannel,
		constLabel:       constLabel,
		bytes:            metrics.WALAppendMessageBytes.MustCurryWith(constLabel),
		total:            metrics.WALAppendMessageTotal.MustCurryWith(constLabel),
		walDuration:      metrics.WALAppendMessageDurationSeconds.MustCurryWith(constLabel),
		walimplsDuration: metrics.WALImplsAppendMessageDurationSeconds.MustCurryWith(constLabel),
	}
}

type WriteMetrics struct {
	walName          string
	pchannel         types.PChannelInfo
	constLabel       prometheus.Labels
	bytes            prometheus.ObserverVec
	total            *prometheus.CounterVec
	walDuration      prometheus.ObserverVec
	walimplsDuration prometheus.ObserverVec
}

func (m *WriteMetrics) StartAppend(msgType message.MessageType, bytes int) *WriteGuard {
	return &WriteGuard{
		startAppend: time.Now(),
		metrics:     m,
		msgType:     msgType,
		bytes:       bytes,
	}
}

func (m *WriteMetrics) Close() {
	metrics.WALAppendMessageBytes.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageTotal.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALImplsAppendMessageDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALInfo.DeleteLabelValues(
		paramtable.GetStringNodeID(),
		m.pchannel.Name,
		strconv.FormatInt(m.pchannel.Term, 10),
		m.walName,
	)
}

type WriteGuard struct {
	startAppend     time.Time
	startImplAppend time.Time
	implCost        time.Duration
	metrics         *WriteMetrics
	msgType         message.MessageType
	bytes           int
}

func (g *WriteGuard) StartWALImplAppend() {
	g.startImplAppend = time.Now()
}

func (g *WriteGuard) FinishWALImplAppend() {
	g.implCost = time.Since(g.startImplAppend)
}

func (g *WriteGuard) Finish(err error) {
	status := parseError(err)
	if g.implCost != 0 {
		g.metrics.walimplsDuration.WithLabelValues(status).Observe(g.implCost.Seconds())
	}
	g.metrics.bytes.WithLabelValues(status).Observe(float64(g.bytes))
	g.metrics.total.WithLabelValues(g.msgType.String(), status).Inc()
	g.metrics.walDuration.WithLabelValues(status).Observe(time.Since(g.startAppend).Seconds())
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
