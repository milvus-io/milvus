package metricsutil

import (
	"github.com/milvus-io/milvus/pkg/metrics"
)

var _ metricManager = &loadedSegmentMetricManager{}

// ResourceUsageMetrics records the estimate resource usage of the segment.
type ResourceUsageMetrics struct {
	MemUsage  uint64
	DiskUsage uint64
	RowNum    uint64
}

func (m *ResourceUsageMetrics) IsZero() bool {
	return m.MemUsage == 0 && m.DiskUsage == 0
}

// newLoadedSegmentMetricManager creates a new active segment metric manager.
func newLoadedSegmentMetricManager(nodeID string) *loadedSegmentMetricManager {
	return &loadedSegmentMetricManager{
		nodeID:    nodeID,
		lastUsage: make(map[databaseLabel]*ResourceUsageMetrics),
	}
}

// loadedSegmentMetricManager is the active segment metric manager.
type loadedSegmentMetricManager struct {
	nodeID    string
	lastUsage map[databaseLabel]*ResourceUsageMetrics
}

// Apply update the active segment metric.
func (m *loadedSegmentMetricManager) Apply(s *sampler) {
	usage := m.aggregateSegments(s)
	m.overwriteMetrics(usage)
}

// aggregateSegments aggregates the segments.
func (m *loadedSegmentMetricManager) aggregateSegments(s *sampler) map[databaseLabel]*ResourceUsageMetrics {
	// aggregate the hot segments.
	history := s.GetHistory()

	usages := make(map[databaseLabel]*ResourceUsageMetrics, len(history))
	for label, h := range history {
		// Skip no resource usage segment.
		if h.latestResourceUsage.IsZero() {
			continue
		}

		l := databaseLabel{
			database:      label.DatabaseName,
			resourceGroup: label.ResourceGroup,
		}
		if usage, ok := usages[l]; !ok {
			usages[l] = &ResourceUsageMetrics{
				MemUsage:  h.latestResourceUsage.MemUsage,
				DiskUsage: h.latestResourceUsage.DiskUsage,
			}
		} else {
			usage.MemUsage += h.latestResourceUsage.MemUsage
			usage.DiskUsage += h.latestResourceUsage.DiskUsage
		}
	}
	return usages
}

// overwriteMetrics overwrites the hot segment metrics.
func (m *loadedSegmentMetricManager) overwriteMetrics(usages map[databaseLabel]*ResourceUsageMetrics) {
	for label, usage := range usages {
		metrics.QueryNodeLoadedSegmentMemoryBytes.WithLabelValues(m.nodeID, label.database, label.resourceGroup).Set(float64(usage.MemUsage))
		metrics.QueryNodeLoadedSegmentDiskBytes.WithLabelValues(m.nodeID, label.database, label.resourceGroup).Set(float64(usage.DiskUsage))
	}
	// Remove the metrics of the hot segments that are not hot now.
	for label := range m.lastUsage {
		if _, ok := usages[label]; !ok {
			metrics.QueryNodeLoadedSegmentMemoryBytes.DeleteLabelValues(m.nodeID, label.database, label.resourceGroup)
			metrics.QueryNodeLoadedSegmentDiskBytes.DeleteLabelValues(m.nodeID, label.database, label.resourceGroup)
		}
	}
	m.lastUsage = usages
}
