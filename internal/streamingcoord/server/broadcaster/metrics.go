package broadcaster

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// newBroadcasterMetrics creates a new broadcaster metrics.
func newBroadcasterMetrics() *broadcasterMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName: paramtable.GetStringNodeID(),
	}
	return &broadcasterMetrics{
		taskTotal:           metrics.StreamingCoordBroadcasterTaskTotal.MustCurryWith(constLabel),
		executionDuration:   metrics.StreamingCoordBroadcasterTaskExecutionDurationSeconds.MustCurryWith(constLabel),
		broadcastDuration:   metrics.StreamingCoordBroadcasterTaskBroadcastDurationSeconds.MustCurryWith(constLabel),
		ackCallbackDuration: metrics.StreamingCoordBroadcasterTaskAckCallbackDurationSeconds.MustCurryWith(constLabel),
		acquireLockDuration: metrics.StreamingCoordBroadcasterTaskAcquireLockDurationSeconds.MustCurryWith(constLabel),
	}
}

// broadcasterMetrics is the metrics of the broadcaster.
type broadcasterMetrics struct {
	taskTotal           *prometheus.GaugeVec
	executionDuration   prometheus.ObserverVec
	broadcastDuration   prometheus.ObserverVec
	ackCallbackDuration prometheus.ObserverVec
	acquireLockDuration prometheus.ObserverVec
}

// ObserveAcquireLockDuration observes the acquire lock duration.
func (m *broadcasterMetrics) ObserveAcquireLockDuration(from time.Time, rks []message.ResourceKey) {
	m.acquireLockDuration.WithLabelValues(formatResourceKeys(rks)).Observe(time.Since(from).Seconds())
}

// fromStateToState updates the metrics when the state of the broadcast task changes.
func (m *broadcasterMetrics) fromStateToState(msgType message.MessageType, from streamingpb.BroadcastTaskState, to streamingpb.BroadcastTaskState) {
	if from != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_UNKNOWN {
		m.taskTotal.WithLabelValues(msgType.String(), from.String()).Dec()
	}
	if to != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		m.taskTotal.WithLabelValues(msgType.String(), to.String()).Inc()
	}
}

// NewBroadcastTask creates a new broadcast task.
func (m *broadcasterMetrics) NewBroadcastTask(msgType message.MessageType, state streamingpb.BroadcastTaskState, rks []message.ResourceKey) *taskMetricsGuard {
	rks = uniqueSortResourceKeys(rks)
	g := &taskMetricsGuard{
		start:              time.Now(),
		ackCallbackBegin:   time.Now(),
		state:              state,
		resourceKeys:       formatResourceKeys(rks),
		broadcasterMetrics: m,
		messageType:        msgType,
	}
	g.broadcasterMetrics.fromStateToState(msgType, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_UNKNOWN, state)
	return g
}

type taskMetricsGuard struct {
	start            time.Time
	ackCallbackBegin time.Time
	state            streamingpb.BroadcastTaskState
	resourceKeys     string
	messageType      message.MessageType
	*broadcasterMetrics
}

// ObserveStateChanged updates the state of the broadcast task.
func (g *taskMetricsGuard) ObserveStateChanged(state streamingpb.BroadcastTaskState) {
	g.broadcasterMetrics.fromStateToState(g.messageType, g.state, state)
	if state == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
		g.executionDuration.WithLabelValues(g.messageType.String()).Observe(time.Since(g.start).Seconds())
	}
	g.state = state
}

// ObserveBroadcastDone observes the broadcast done.
func (g *taskMetricsGuard) ObserveBroadcastDone() {
	g.broadcastDuration.WithLabelValues(g.messageType.String()).Observe(time.Since(g.start).Seconds())
}

// ObserveAckCallbackBegin observes the ack callback begin.
func (g *taskMetricsGuard) ObserveAckCallbackBegin() {
	g.ackCallbackBegin = time.Now()
}

// ObserveAckCallbackDone observes the ack callback done.
func (g *taskMetricsGuard) ObserveAckCallbackDone() {
	g.ackCallbackDuration.WithLabelValues(g.messageType.String()).Observe(time.Since(g.ackCallbackBegin).Seconds())
}

// formatResourceKeys formats the resource keys.
func formatResourceKeys(rks []message.ResourceKey) string {
	keys := make([]string, 0, len(rks))
	for _, rk := range rks {
		keys = append(keys, rk.ShortString())
	}
	return strings.Join(keys, "|")
}
