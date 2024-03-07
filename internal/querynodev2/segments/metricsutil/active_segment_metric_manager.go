package metricsutil

import (
	"github.com/cockroachdb/errors"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

var _ metricManager = &activeSegmentMetricManager{}

// databaseLabel is the label of the active segment metric.
type databaseLabel struct {
	database      string
	resourceGroup string
}

// newActiveSegmentMetricManager creates a new active segment metric manager.
func newActiveSegmentMetricManager(nodeID string) *activeSegmentMetricManager {
	return &activeSegmentMetricManager{
		nodeID:    nodeID,
		lastUsage: make(map[databaseLabel]*ResourceUsageMetrics),
	}
}

// activeSegmentMetricManager is the active segment metric manager.
type activeSegmentMetricManager struct {
	nodeID      string
	lastExpr    string
	enabledExpr string
	exprProgram *vm.Program
	lastUsage   map[databaseLabel]*ResourceUsageMetrics
}

// Apply update the active segment metric.
func (m *activeSegmentMetricManager) Apply(s *sampler) {
	// update the active segment predicate expression if needed.
	m.updateActiveSegmentPredicate()
	usage, err := m.aggregateSegments(s)
	if err != nil {
		log.Warn("failed to aggregate active segments", zap.Error(err))
		return
	}
	m.overwriteMetrics(usage)
}

// overwriteMetrics overwrites the active segment metrics.
func (m *activeSegmentMetricManager) overwriteMetrics(usages map[databaseLabel]*ResourceUsageMetrics) {
	for label, usage := range usages {
		metrics.QueryNodeActiveSegmentMemoryBytes.WithLabelValues(m.nodeID, label.database, label.resourceGroup).Set(float64(usage.MemUsage))
		metrics.QueryNodeActiveSegmentDiskBytes.WithLabelValues(m.nodeID, label.database, label.resourceGroup).Set(float64(usage.DiskUsage))
	}
	// Remove the metrics of the active segments that are not active now.
	for label := range m.lastUsage {
		if _, ok := usages[label]; !ok {
			metrics.QueryNodeActiveSegmentMemoryBytes.DeleteLabelValues(m.nodeID, label.database, label.resourceGroup)
			metrics.QueryNodeActiveSegmentDiskBytes.DeleteLabelValues(m.nodeID, label.database, label.resourceGroup)
		}
	}
	m.lastUsage = usages
}

// updateActiveSegmentPredicate updates the active segment predicate expression.
func (m *activeSegmentMetricManager) updateActiveSegmentPredicate() {
	newExpr := paramtable.Get().QueryNodeCfg.ActiveSegmentPredicateExpr.GetValue()
	if newExpr == m.lastExpr {
		return
	}

	m.lastExpr = newExpr
	program, err := m.compileExpr(newExpr)
	if err != nil {
		log.Warn("failed to update active segment predicate, use old one", zap.Error(err), zap.String("oldExpr", m.enabledExpr), zap.String("newExpr", newExpr))
		return
	}
	log.Info("update active segment predicate expression", zap.String("oldExpr", m.enabledExpr), zap.String("newExpr", newExpr))
	m.enabledExpr = newExpr
	m.exprProgram = program
}

// aggregateSegments aggregates the active segments.
func (m *activeSegmentMetricManager) aggregateSegments(s *sampler) (map[databaseLabel]*ResourceUsageMetrics, error) {
	if m.exprProgram == nil {
		return nil, errors.New("active segment predicate expression is nil")
	}

	// aggregate the active segments.
	history := s.GetHistory()
	usages := make(map[databaseLabel]*ResourceUsageMetrics, len(history))
	for label, h := range history {
		l := databaseLabel{
			database:      label.DatabaseName,
			resourceGroup: label.ResourceGroup,
		}
		if h.Len() <= 1 {
			// skip the segment with only one snapsactive or no snapsactive.
			continue
		}
		// Skip no resource usage segment.
		if h.latestResourceUsage.IsZero() {
			continue
		}

		ok, err := m.runPredicate(h)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to run active segment predicate expression on label %v, history count: %d", label, h.Len())
		}
		// update the active metric if predicate success.
		if ok {
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
	}
	return usages, nil
}

// compileExpr compiles the expression.
func (m *activeSegmentMetricManager) compileExpr(newExpr string) (*vm.Program, error) {
	program, err := expr.Compile(newExpr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile active segment predicate expression")
	}

	output, err := expr.Run(program, getTestSegmentHistory())
	if err != nil {
		return nil, errors.Wrap(err, "failed to test active segment predicate on test case")
	}
	_, ok := output.(bool)
	if !ok {
		return nil, errors.New("failed to convert output to bool on test case")
	}
	return program, nil
}

// runPredicate runs the predicate expression.
func (m *activeSegmentMetricManager) runPredicate(h *segmentHistory) (bool, error) {
	output, err := expr.Run(m.exprProgram, h)
	if err != nil {
		return false, err
	}
	b, ok := output.(bool)
	if !ok {
		return false, errors.New("failed to convert output to bool")
	}
	return b, nil
}
