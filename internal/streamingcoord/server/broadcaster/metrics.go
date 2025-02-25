package broadcaster

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newBroadcasterMetrics creates a new broadcaster metrics.
func newBroadcasterMetrics() *broadcasterMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
	}
	return &broadcasterMetrics{
		taskTotal:         metrics.StreamingCoordBroadcasterTaskTotal.MustCurryWith(constLabel),
		resourceKeyTotal:  metrics.StreamingCoordResourceKeyTotal.MustCurryWith(constLabel),
		broadcastDuration: metrics.StreamingCoordBroadcastDurationSeconds.With(constLabel),
		ackAnyOneDuration: metrics.StreamingCoordBroadcasterAckAnyOneDurationSeconds.With(constLabel),
		ackAllDuration:    metrics.StreamingCoordBroadcasterAckAllDurationSeconds.With(constLabel),
	}
}

// broadcasterMetrics is the metrics of the broadcaster.
type broadcasterMetrics struct {
	taskTotal         *prometheus.GaugeVec
	resourceKeyTotal  *prometheus.GaugeVec
	broadcastDuration prometheus.Observer
	ackAnyOneDuration prometheus.Observer
	ackAllDuration    prometheus.Observer
}

// fromStateToState updates the metrics when the state of the broadcast task changes.
func (m *broadcasterMetrics) fromStateToState(from streamingpb.BroadcastTaskState, to streamingpb.BroadcastTaskState) {
	if from != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_UNKNOWN {
		m.taskTotal.WithLabelValues(from.String()).Dec()
	}
	if to != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		m.taskTotal.WithLabelValues(to.String()).Inc()
	}
}

// NewBroadcastTask creates a new broadcast task.
func (m *broadcasterMetrics) NewBroadcastTask(state streamingpb.BroadcastTaskState) *taskMetricsGuard {
	g := &taskMetricsGuard{
		start:              time.Now(),
		state:              state,
		broadcasterMetrics: m,
	}
	g.broadcasterMetrics.fromStateToState(streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_UNKNOWN, state)
	return g
}

func (m *broadcasterMetrics) IncomingResourceKey(domain messagespb.ResourceDomain) {
	m.resourceKeyTotal.WithLabelValues(domain.String()).Inc()
}

func (m *broadcasterMetrics) GoneResourceKey(domain messagespb.ResourceDomain) {
	m.resourceKeyTotal.WithLabelValues(domain.String()).Dec()
}

type taskMetricsGuard struct {
	start time.Time
	state streamingpb.BroadcastTaskState
	*broadcasterMetrics
}

// ToState updates the state of the broadcast task.
func (g *taskMetricsGuard) ToState(state streamingpb.BroadcastTaskState) {
	g.broadcasterMetrics.fromStateToState(g.state, state)
	g.state = state
}

// ObserveBroadcastDone observes the broadcast done.
func (g *taskMetricsGuard) ObserveBroadcastDone() {
	g.broadcastDuration.Observe(time.Since(g.start).Seconds())
}

// ObserverAckOne observes the ack any one.
func (g *taskMetricsGuard) ObserveAckAnyOne() {
	g.ackAnyOneDuration.Observe(time.Since(g.start).Seconds())
}

// ObserverAckOne observes the ack all.
func (g *taskMetricsGuard) ObserveAckAll() {
	g.ackAllDuration.Observe(time.Since(g.start).Seconds())
}
