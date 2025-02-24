package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func NewSegmentAssignMetrics(pchannel string) *SegmentAssignMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &SegmentAssignMetrics{
		constLabel:      constLabel,
		allocTotal:      metrics.WALSegmentAllocTotal.MustCurryWith(constLabel),
		segmentBytes:    metrics.WALSegmentBytes.With(constLabel),
		flushedTotal:    metrics.WALSegmentFlushedTotal.MustCurryWith(constLabel),
		partitionTotal:  metrics.WALPartitionTotal.With(constLabel),
		collectionTotal: metrics.WALCollectionTotal.With(constLabel),
	}
}

// SegmentAssignMetrics is the metrics of the segment assignment.
type SegmentAssignMetrics struct {
	constLabel prometheus.Labels

	allocTotal      *prometheus.GaugeVec
	segmentBytes    prometheus.Observer
	flushedTotal    *prometheus.CounterVec
	partitionTotal  prometheus.Gauge
	collectionTotal prometheus.Gauge
}

// UpdateGrowingSegmentState updates the metrics of the segment assignment state.
func (m *SegmentAssignMetrics) UpdateGrowingSegmentState(from streamingpb.SegmentAssignmentState, to streamingpb.SegmentAssignmentState) {
	if from != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_UNKNOWN {
		m.allocTotal.WithLabelValues(from.String()).Dec()
	}
	if to != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		m.allocTotal.WithLabelValues(to.String()).Inc()
	}
}

func (m *SegmentAssignMetrics) ObserveSegmentFlushed(policy string, bytes int64) {
	m.segmentBytes.Observe(float64(bytes))
	m.flushedTotal.WithLabelValues(policy).Inc()
}

func (m *SegmentAssignMetrics) UpdatePartitionCount(cnt int) {
	m.partitionTotal.Set(float64(cnt))
}

func (m *SegmentAssignMetrics) UpdateCollectionCount(cnt int) {
	m.collectionTotal.Set(float64(cnt))
}

func (m *SegmentAssignMetrics) Close() {
	metrics.WALSegmentAllocTotal.DeletePartialMatch(m.constLabel)
	metrics.WALSegmentFlushedTotal.DeletePartialMatch(m.constLabel)
	metrics.WALSegmentBytes.Delete(m.constLabel)
	metrics.WALPartitionTotal.Delete(m.constLabel)
	metrics.WALCollectionTotal.Delete(m.constLabel)
}
