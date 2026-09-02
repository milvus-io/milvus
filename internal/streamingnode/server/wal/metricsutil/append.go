package metricsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

const (
	maxLogged                    = 3
	logThreshold                 = 10 * time.Millisecond
	inlineInterceptorGroups      = 16
	inlineInterceptorOccurrences = 2
)

type InterceptorMetrics struct {
	Before    time.Duration
	BeforeErr error
	After     time.Duration
}

func (im *InterceptorMetrics) ShouldBeLogged() bool {
	return im.Before > logThreshold || im.After > logThreshold || im.BeforeErr != nil
}

func (im *InterceptorMetrics) String() string {
	return fmt.Sprintf("b:%s,a:%s,err:%s", im.Before, im.After, im.BeforeErr)
}

// AppendMetrics is the metrics for append operation.
type AppendMetrics struct {
	wm  *WriteMetrics
	msg message.MutableMessage

	result             *types.AppendResult
	err                error
	appendDuration     time.Duration
	implAppendDuration time.Duration
	interceptorGroups  [inlineInterceptorGroups]interceptorMetricsGroup
	interceptorCount   int
	extraInterceptors  map[string][]*InterceptorMetrics
}

type interceptorMetricsGroup struct {
	name    string
	metrics [inlineInterceptorOccurrences]InterceptorMetrics
	count   int
	extra   []*InterceptorMetrics
}

type AppendMetricsGuard struct {
	inner           *AppendMetrics
	startAppend     time.Time
	startImplAppend time.Time
}

// StartInterceptorCollector start the interceptor to collect the duration.
func (m *AppendMetrics) StartInterceptorCollector(name string) InterceptorCollectGuard {
	for i := 0; i < m.interceptorCount; i++ {
		if m.interceptorGroups[i].name == name {
			return newInterceptorCollectGuard(m.interceptorGroups[i].next())
		}
	}
	if m.interceptorCount < len(m.interceptorGroups) {
		group := &m.interceptorGroups[m.interceptorCount]
		m.interceptorCount++
		group.name = name
		return newInterceptorCollectGuard(group.next())
	}
	if m.extraInterceptors == nil {
		m.extraInterceptors = make(map[string][]*InterceptorMetrics)
	}
	metric := &InterceptorMetrics{}
	m.extraInterceptors[name] = append(m.extraInterceptors[name], metric)
	return newInterceptorCollectGuard(metric)
}

func newInterceptorCollectGuard(metric *InterceptorMetrics) InterceptorCollectGuard {
	return InterceptorCollectGuard{
		start:        time.Now(),
		afterStarted: false,
		interceptor:  metric,
	}
}

func (g *interceptorMetricsGroup) next() *InterceptorMetrics {
	if g.count < len(g.metrics) {
		metric := &g.metrics[g.count]
		g.count++
		return metric
	}
	metric := &InterceptorMetrics{}
	g.extra = append(g.extra, metric)
	return metric
}

// StartAppendGuard start the append operation.
func (m *AppendMetrics) StartAppendGuard() AppendMetricsGuard {
	return AppendMetricsGuard{
		inner:       m,
		startAppend: time.Now(),
	}
}

// IntoLogFields convert the metrics to log fields.
func (m *AppendMetrics) IntoLogFields() []mlog.Field {
	fields := []mlog.Field{
		mlog.FieldMessage(m.msg),
		mlog.Duration("duration", m.appendDuration),
		mlog.Duration("implDuration", m.implAppendDuration),
	}

	if m.err != nil {
		fields = append(fields, mlog.Err(m.err))
	} else {
		fields = append(fields, mlog.String("messageID", m.result.MessageID.String()))
		fields = append(fields, mlog.String("lcMessageID", m.result.LastConfirmedMessageID.String()))
		fields = append(fields, mlog.Uint64("timetick", m.result.TimeTick))
		if m.result.TxnCtx != nil {
			fields = append(fields, mlog.Int64("txnID", int64(m.result.TxnCtx.TxnID)))
		}
	}
	loggedInterceptorCount := 0
	m.rangeInterceptorMetrics(func(name string, occurrence int, im *InterceptorMetrics) bool {
		if !im.ShouldBeLogged() {
			return true
		}
		if loggedInterceptorCount <= maxLogged {
			fields = append(fields, mlog.Stringer(fmt.Sprintf("%s_%d", name, occurrence), im))
			loggedInterceptorCount++
		}
		return loggedInterceptorCount < maxLogged
	})
	return fields
}

// StartWALImplAppend start the implementation append operation.
func (m *AppendMetricsGuard) StartWALImplAppend() {
	m.startImplAppend = time.Now()
}

// FinishImplAppend finish the implementation append operation.
func (m *AppendMetricsGuard) FinishWALImplAppend() {
	m.inner.implAppendDuration = time.Since(m.startImplAppend)
}

// FinishAppend finish the append operation.
func (m *AppendMetricsGuard) FinishAppend() {
	m.inner.appendDuration = time.Since(m.startAppend)
}

// RangeOverInterceptors range over the interceptors.
func (m *AppendMetrics) RangeOverInterceptors(f func(name string, ims []*InterceptorMetrics)) {
	for i := 0; i < m.interceptorCount; i++ {
		group := &m.interceptorGroups[i]
		metrics := make([]*InterceptorMetrics, 0, group.count+len(group.extra))
		for j := 0; j < group.count; j++ {
			metrics = append(metrics, &group.metrics[j])
		}
		metrics = append(metrics, group.extra...)
		f(group.name, metrics)
	}
	for name, metrics := range m.extraInterceptors {
		f(name, metrics)
	}
}

func (m *AppendMetrics) rangeInterceptorMetrics(f func(name string, occurrence int, metric *InterceptorMetrics) bool) {
	for i := 0; i < m.interceptorCount; i++ {
		group := &m.interceptorGroups[i]
		for j := 0; j < group.count; j++ {
			if !f(group.name, j, &group.metrics[j]) {
				return
			}
		}
		for j, metric := range group.extra {
			if !f(group.name, group.count+j, metric) {
				return
			}
		}
	}
	for name, metrics := range m.extraInterceptors {
		for occurrence, metric := range metrics {
			if !f(name, occurrence, metric) {
				return
			}
		}
	}
}

// Done push the metrics.
func (m *AppendMetrics) Done(ctx context.Context, result *types.AppendResult, err error) {
	m.err = err
	m.result = result
	m.wm.done(ctx, m)
}

// InterceptorCollectGuard is used to collect the metrics of interceptor.
type InterceptorCollectGuard struct {
	start        time.Time
	afterStarted bool
	interceptor  *InterceptorMetrics
}

// BeforeDone mark the before append operation is done.
func (g *InterceptorCollectGuard) BeforeDone() {
	g.interceptor.Before = time.Since(g.start)
}

// BeforeFailure mark the operation before append is failed.
func (g *InterceptorCollectGuard) BeforeFailure(err error) {
	if g.interceptor.Before == 0 {
		// if before duration is not set, means the operation is failed before the interceptor.
		g.interceptor.Before = time.Since(g.start)
		g.interceptor.BeforeErr = err
	}
}

// AfterStart mark the after append operation is started.
func (g *InterceptorCollectGuard) AfterStart() {
	g.start = time.Now()
	g.afterStarted = true
}

// AfterDone mark the after append operation is done.
func (g *InterceptorCollectGuard) AfterDone() {
	if g.afterStarted {
		g.interceptor.After += time.Since(g.start)
	}
}
