package metricsutil

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

const maxRedoLogged = 3

type InterceptorMetrics struct {
	Before    time.Duration
	BeforeErr error
	After     time.Duration
}

func (im *InterceptorMetrics) String() string {
	return fmt.Sprintf("before: %s, after: %s, before_err: %v", im.Before, im.After, im.BeforeErr)
}

// AppendMetrics is the metrics for append operation.
type AppendMetrics struct {
	wm  *WriteMetrics
	msg message.MutableMessage

	result             *types.AppendResult
	err                error
	appendDuration     time.Duration
	implAppendDuration time.Duration
	interceptors       map[string][]*InterceptorMetrics
}

type AppendMetricsGuard struct {
	inner           *AppendMetrics
	startAppend     time.Time
	startImplAppend time.Time
}

// StartInterceptorCollector start the interceptor to collect the duration.
func (m *AppendMetrics) StartInterceptorCollector(name string) *InterceptorCollectGuard {
	if _, ok := m.interceptors[name]; !ok {
		m.interceptors[name] = make([]*InterceptorMetrics, 0, 2)
	}
	im := &InterceptorMetrics{}
	m.interceptors[name] = append(m.interceptors[name], im)
	return &InterceptorCollectGuard{
		start:        time.Now(),
		afterStarted: false,
		interceptor:  im,
	}
}

// StartAppendGuard start the append operation.
func (m *AppendMetrics) StartAppendGuard() *AppendMetricsGuard {
	return &AppendMetricsGuard{
		inner:       m,
		startAppend: time.Now(),
	}
}

// IntoLogFields convert the metrics to log fields.
func (m *AppendMetrics) IntoLogFields() []zap.Field {
	fields := []zap.Field{
		log.FieldMessage(m.msg),
		zap.Duration("append_duration", m.appendDuration),
		zap.Duration("impl_append_duration", m.implAppendDuration),
	}
	for name, ims := range m.interceptors {
		for i, im := range ims {
			if i <= maxRedoLogged {
				fields = append(fields, zap.Any(fmt.Sprintf("%s_%d", name, i), im))
			}
		}
	}
	if m.err != nil {
		fields = append(fields, zap.Error(m.err))
	} else {
		fields = append(fields, zap.String("message_id", m.result.MessageID.String()))
		fields = append(fields, zap.Uint64("time_tick", m.result.TimeTick))
		if m.result.TxnCtx != nil {
			fields = append(fields, zap.Int64("txn_id", int64(m.result.TxnCtx.TxnID)))
		}
	}
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
	for name, ims := range m.interceptors {
		f(name, ims)
	}
}

// Done push the metrics.
func (m *AppendMetrics) Done(result *types.AppendResult, err error) {
	m.err = err
	m.result = result
	m.wm.done(m)
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
