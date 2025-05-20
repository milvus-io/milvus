package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type GrowingSegmentState string

func NewSegmentAssignMetrics(pchannel string) *SegmentAssignMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &SegmentAssignMetrics{
		constLabel:       constLabel,
		allocTotal:       metrics.WALSegmentAllocTotal.With(constLabel),
		segmentRowsTotal: metrics.WALSegmentRowsTotal.With(constLabel),
		segmentBytes:     metrics.WALSegmentBytes.With(constLabel),
		flushedTotal:     metrics.WALSegmentFlushedTotal.MustCurryWith(constLabel),
		partitionTotal:   metrics.WALPartitionTotal.With(constLabel),
		collectionTotal:  metrics.WALCollectionTotal.With(constLabel),
	}
}

// SegmentAssignMetrics is the metrics of the segment assignment.
type SegmentAssignMetrics struct {
	constLabel prometheus.Labels

	onAllocTotal     prometheus.Gauge
	onFlushTotal     prometheus.Gauge
	allocTotal       prometheus.Gauge
	segmentRowsTotal prometheus.Observer
	segmentBytes     prometheus.Observer
	flushedTotal     *prometheus.CounterVec
	partitionTotal   prometheus.Gauge
	collectionTotal  prometheus.Gauge
}

// ObserveOnAllocating observe a allocating operation and return a guard function.
func (m *SegmentAssignMetrics) ObserveOnAllocating() func() {
	m.onAllocTotal.Inc()
	return func() {
		m.onAllocTotal.Dec()
	}
}

// ObserveOnFlushing observe a flush operation and return a guard function.
func (m *SegmentAssignMetrics) ObseveOnFlushing() func() {
	m.onFlushTotal.Inc()
	return func() {
		m.onFlushTotal.Dec()
	}
}

// ObserveCreateSegment increments the total number of growing segment.
func (m *SegmentAssignMetrics) ObserveCreateSegment() {
	m.allocTotal.Inc()
}

// ObserveSegmentFlushed records the number of bytes flushed and increments the total number of flushed segments.
func (m *SegmentAssignMetrics) ObserveSegmentFlushed(policy string, rows int64, bytes int64) {
	m.allocTotal.Dec()
	m.flushedTotal.WithLabelValues(policy).Inc()
	m.segmentRowsTotal.Observe(float64(rows))
	m.segmentBytes.Observe(float64(bytes))
}

func (m *SegmentAssignMetrics) UpdateSegmentCount(cnt int) {
	m.allocTotal.Set(float64(cnt))
}

func (m *SegmentAssignMetrics) UpdatePartitionCount(cnt int) {
	m.partitionTotal.Set(float64(cnt))
}

func (m *SegmentAssignMetrics) UpdateCollectionCount(cnt int) {
	m.collectionTotal.Set(float64(cnt))
}

func (m *SegmentAssignMetrics) Close() {
	metrics.WALSegmentAllocTotal.Delete(m.constLabel)
	metrics.WALSegmentFlushedTotal.DeletePartialMatch(m.constLabel)
	metrics.WALSegmentRowsTotal.Delete(m.constLabel)
	metrics.WALSegmentBytes.Delete(m.constLabel)
	metrics.WALPartitionTotal.Delete(m.constLabel)
	metrics.WALCollectionTotal.Delete(m.constLabel)
}
