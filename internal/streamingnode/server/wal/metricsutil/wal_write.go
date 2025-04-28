package metricsutil

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/wp"
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

	slowLogThreshold := paramtable.Get().StreamingCfg.LoggingAppendSlowThreshold.GetAsDurationByParse()
	if slowLogThreshold <= 0 {
		slowLogThreshold = time.Second
	}
	if walName == wp.WALName && slowLogThreshold < 3*time.Second {
		// slow log threshold is not set in woodpecker, so we set it to 0.
		slowLogThreshold = 3 * time.Second
	}
	return &WriteMetrics{
		walName:                      walName,
		pchannel:                     pchannel,
		constLabel:                   constLabel,
		bytes:                        metrics.WALAppendMessageBytes.MustCurryWith(constLabel),
		total:                        metrics.WALAppendMessageTotal.MustCurryWith(constLabel),
		walDuration:                  metrics.WALAppendMessageDurationSeconds.MustCurryWith(constLabel),
		walimplsDuration:             metrics.WALImplsAppendMessageDurationSeconds.MustCurryWith(constLabel),
		walBeforeInterceptorDuration: metrics.WALAppendMessageBeforeInterceptorDurationSeconds.MustCurryWith(constLabel),
		walAfterInterceptorDuration:  metrics.WALAppendMessageAfterInterceptorDurationSeconds.MustCurryWith(constLabel),
		slowLogThreshold:             time.Second,
	}
}

type WriteMetrics struct {
	log.Binder

	walName                      string
	pchannel                     types.PChannelInfo
	constLabel                   prometheus.Labels
	bytes                        prometheus.ObserverVec
	total                        *prometheus.CounterVec
	walDuration                  prometheus.ObserverVec
	walimplsDuration             prometheus.ObserverVec
	walBeforeInterceptorDuration prometheus.ObserverVec
	walAfterInterceptorDuration  prometheus.ObserverVec
	slowLogThreshold             time.Duration
}

func (m *WriteMetrics) StartAppend(msg message.MutableMessage) *AppendMetrics {
	return &AppendMetrics{
		wm:           m,
		msg:          msg,
		interceptors: make(map[string][]*InterceptorMetrics),
	}
}

func (m *WriteMetrics) done(appendMetrics *AppendMetrics) {
	if !appendMetrics.msg.IsPersisted() {
		return
	}
	status := parseError(appendMetrics.err)
	if appendMetrics.implAppendDuration != 0 {
		m.walimplsDuration.WithLabelValues(status).Observe(appendMetrics.implAppendDuration.Seconds())
	}
	m.bytes.WithLabelValues(status).Observe(float64(appendMetrics.bytes))
	m.total.WithLabelValues(appendMetrics.msg.MessageType().String(), status).Inc()
	m.walDuration.WithLabelValues(status).Observe(appendMetrics.appendDuration.Seconds())
	for name, ims := range appendMetrics.interceptors {
		for _, im := range ims {
			if im.Before != 0 {
				m.walBeforeInterceptorDuration.WithLabelValues(name).Observe(im.Before.Seconds())
			}
			if im.After != 0 {
				m.walAfterInterceptorDuration.WithLabelValues(name).Observe(im.After.Seconds())
			}
		}
	}
	if appendMetrics.err != nil {
		m.Logger().Warn("append message into wal failed", appendMetrics.IntoLogFields()...)
		return
	}
	if appendMetrics.appendDuration >= m.slowLogThreshold {
		// log slow append catch
		m.Logger().Warn("append message into wal too slow", appendMetrics.IntoLogFields()...)
		return
	}
	if m.Logger().Level().Enabled(zapcore.DebugLevel) {
		m.Logger().Debug("append message into wal", appendMetrics.IntoLogFields()...)
	}
}

func (m *WriteMetrics) Close() {
	metrics.WALAppendMessageBeforeInterceptorDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageAfterInterceptorDurationSeconds.DeletePartialMatch(m.constLabel)
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
