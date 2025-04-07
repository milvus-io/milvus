package flowcontrol

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsHelper struct {
	state     *prometheus.GaugeVec
	total     *prometheus.CounterVec
	durations prometheus.Observer
}

// newMetricsHelper creates a new metrics helper.
func newMetricsHelper() *metricsHelper {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
	}
	return &metricsHelper{
		state:     metrics.StreamingNodeFlowcontrolState.MustCurryWith(constLabel),
		total:     metrics.StreamingNodeFlowcontrolTotal.MustCurryWith(constLabel),
		durations: metrics.StreamingNodeFlowcontrolDurationSeconds.With(constLabel),
	}
}

func (m *metricsHelper) ObserveStateChange(state flowcontrolState) {
	metrics.StreamingNodeFlowcontrolState.DeletePartialMatch(prometheus.Labels{})
	m.state.With(prometheus.Labels{metrics.StreamingNodeFlowcontrolStateLabelName: state.String()}).Set(1)
}

func (m *metricsHelper) ObserveResult(err error) {
}

func (m *metricsHelper) StartFlowControl() *metricsGuard {
	return &metricsGuard{
		metricsHelper: m,
		startTime:     time.Now(),
	}
}

type metricsGuard struct {
	*metricsHelper
	startTime time.Time
}

func (g *metricsGuard) Done(err error) {
	if err == nil {
		g.durations.Observe(time.Since(g.startTime).Seconds())
	}
	g.total.With(prometheus.Labels{metrics.StatusLabelName: parseError(err)}).Inc()
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
