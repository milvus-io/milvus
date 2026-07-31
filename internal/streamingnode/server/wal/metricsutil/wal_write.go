package metricsutil

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// NewWriteMetrics creates a new WriteMetrics.
func NewWriteMetrics(pchannel types.PChannelInfo, walName message.WALName) *WriteMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel.Name,
	}
	metrics.WALInfo.WithLabelValues(
		paramtable.GetStringNodeID(),
		pchannel.Name,
		strconv.FormatInt(pchannel.Term, 10),
		walName.String()).Set(1)

	slowLogThreshold := paramtable.Get().StreamingCfg.LoggingAppendSlowThreshold.GetAsDurationByParse()
	if slowLogThreshold <= 0 {
		slowLogThreshold = time.Second
	}
	if walName == message.WALNameWoodpecker && slowLogThreshold < 3*time.Second {
		// woodpecker wal is always slow, so we need to set a higher threshold by default.
		slowLogThreshold = 3 * time.Second
	}
	writeMetrics := &WriteMetrics{
		walName:                      walName.String(),
		pchannel:                     pchannel,
		constLabel:                   constLabel,
		bytes:                        metrics.WALAppendMessageBytes.MustCurryWith(constLabel),
		total:                        metrics.WALAppendMessageTotal.MustCurryWith(constLabel),
		walDuration:                  metrics.WALAppendMessageDurationSeconds.MustCurryWith(constLabel),
		walimplsRetryTotal:           metrics.WALImplsAppendRetryTotal.With(constLabel),
		walimplsDuration:             metrics.WALImplsAppendMessageDurationSeconds.MustCurryWith(constLabel),
		walBeforeInterceptorDuration: metrics.WALAppendMessageBeforeInterceptorDurationSeconds.MustCurryWith(constLabel),
		walAfterInterceptorDuration:  metrics.WALAppendMessageAfterInterceptorDurationSeconds.MustCurryWith(constLabel),
		slowLogThreshold:             slowLogThreshold,
	}
	for index, status := range []string{metrics.WALStatusOK, metrics.WALStatusCancel, metrics.WALStatusError} {
		writeMetrics.status[index] = writeStatusMetrics{
			bytes:        writeMetrics.bytes.WithLabelValues(status),
			walDuration:  writeMetrics.walDuration.WithLabelValues(status),
			implDuration: writeMetrics.walimplsDuration.WithLabelValues(status),
		}
	}
	return writeMetrics
}

type writeStatusMetrics struct {
	bytes        prometheus.Observer
	walDuration  prometheus.Observer
	implDuration prometheus.Observer
}

type messageStatusKey struct {
	messageType string
	status      int
}

type interceptorObservers struct {
	before prometheus.Observer
	after  prometheus.Observer
}

type WriteMetrics struct {
	mlog.Binder

	walName                      string
	pchannel                     types.PChannelInfo
	constLabel                   prometheus.Labels
	bytes                        prometheus.ObserverVec
	total                        *prometheus.CounterVec
	walDuration                  prometheus.ObserverVec
	walimplsRetryTotal           prometheus.Counter
	walimplsDuration             prometheus.ObserverVec
	walBeforeInterceptorDuration prometheus.ObserverVec
	walAfterInterceptorDuration  prometheus.ObserverVec
	slowLogThreshold             time.Duration
	status                       [3]writeStatusMetrics
	totalByMessageStatus         sync.Map
	interceptorByName            sync.Map
}

func (m *WriteMetrics) StartAppend(msg message.MutableMessage) *AppendMetrics {
	return &AppendMetrics{
		wm:  m,
		msg: msg,
	}
}

func (m *WriteMetrics) done(ctx context.Context, appendMetrics *AppendMetrics) {
	if !appendMetrics.msg.IsPersisted() {
		return
	}
	statusIndex, status := errorStatus(appendMetrics.err)
	statusMetrics := m.status[statusIndex]
	if appendMetrics.implAppendDuration != 0 {
		statusMetrics.implDuration.Observe(appendMetrics.implAppendDuration.Seconds())
	}
	statusMetrics.bytes.Observe(float64(appendMetrics.msg.EstimateSize()))
	m.totalCounter(appendMetrics.msg.MessageType().String(), statusIndex, status).Inc()
	statusMetrics.walDuration.Observe(appendMetrics.appendDuration.Seconds())
	appendMetrics.rangeInterceptorMetrics(func(name string, _ int, im *InterceptorMetrics) bool {
		observers := m.interceptorObservers(name)
		if im.Before != 0 {
			observers.before.Observe(im.Before.Seconds())
		}
		if im.After != 0 {
			observers.after.Observe(im.After.Seconds())
		}
		return true
	})
	if appendMetrics.err != nil {
		m.Logger().Warn(ctx, "append message into wal failed", appendMetrics.IntoLogFields()...)
		return
	}
	if appendMetrics.appendDuration >= m.slowLogThreshold {
		// log slow append catch
		m.Logger().Warn(ctx, "append message into wal too slow", appendMetrics.IntoLogFields()...)
		return
	}
	logLV := appendMetrics.msg.MessageType().LogLevel()
	if m.Logger().LevelEnabled(logLV) {
		m.Logger().Log(ctx, logLV, "append message into wal", appendMetrics.IntoLogFields()...)
	}
}

func (m *WriteMetrics) totalCounter(messageType string, statusIndex int, status string) prometheus.Counter {
	key := messageStatusKey{messageType: messageType, status: statusIndex}
	if cached, ok := m.totalByMessageStatus.Load(key); ok {
		return cached.(prometheus.Counter)
	}
	counter := m.total.WithLabelValues(messageType, status)
	actual, _ := m.totalByMessageStatus.LoadOrStore(key, counter)
	return actual.(prometheus.Counter)
}

func (m *WriteMetrics) interceptorObservers(name string) interceptorObservers {
	if cached, ok := m.interceptorByName.Load(name); ok {
		return cached.(interceptorObservers)
	}
	observers := interceptorObservers{
		before: m.walBeforeInterceptorDuration.WithLabelValues(name),
		after:  m.walAfterInterceptorDuration.WithLabelValues(name),
	}
	actual, _ := m.interceptorByName.LoadOrStore(name, observers)
	return actual.(interceptorObservers)
}

// ObserveRetry observes the retry of the walimpls.
func (m *WriteMetrics) ObserveRetry() {
	m.walimplsRetryTotal.Inc()
}

func (m *WriteMetrics) Close() {
	metrics.WALAppendMessageBeforeInterceptorDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageAfterInterceptorDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageBytes.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageTotal.DeletePartialMatch(m.constLabel)
	metrics.WALAppendMessageDurationSeconds.DeletePartialMatch(m.constLabel)
	metrics.WALImplsAppendRetryTotal.DeletePartialMatch(m.constLabel)
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
	_, status := errorStatus(err)
	return status
}

func errorStatus(err error) (int, string) {
	if err == nil {
		return 0, metrics.WALStatusOK
	}
	if status.IsCanceled(err) {
		return 1, metrics.WALStatusCancel
	}
	return 2, metrics.WALStatusError
}
