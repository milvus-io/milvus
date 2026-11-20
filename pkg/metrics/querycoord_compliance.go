package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// The callback is installed only by an active coordinator. Holding the read lock
// through evaluation makes clearing it a barrier before coordinator shutdown.
type queryCoordComplianceCollector struct {
	mu       sync.RWMutex
	evaluate func() (map[string]bool, error)
	ready    *prometheus.Desc
	success  *prometheus.Desc
}

func newQueryCoordComplianceCollector() *queryCoordComplianceCollector {
	return &queryCoordComplianceCollector{
		ready: prometheus.NewDesc("milvus_querycoord_resource_group_compliance",
			"Whether the resource group satisfies replica load and WAL placement obligations.", []string{ResourceGroupLabelName}, nil),
		success: prometheus.NewDesc("milvus_querycoord_resource_group_compliance_check_success",
			"Whether this scrape successfully evaluated resource group compliance.", nil, nil),
	}
}

var queryCoordCompliance = newQueryCoordComplianceCollector()

// SetQueryCoordComplianceEvaluator installs the active coordinator's evaluator.
// A nil evaluator disables collection on standby and after shutdown.
func SetQueryCoordComplianceEvaluator(evaluate func() (map[string]bool, error)) {
	queryCoordCompliance.mu.Lock()
	defer queryCoordCompliance.mu.Unlock()
	queryCoordCompliance.evaluate = evaluate
}

func (c *queryCoordComplianceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ready
	ch <- c.success
}

func (c *queryCoordComplianceCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.evaluate == nil {
		return
	}
	groups, err := c.evaluate()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1)
	for rg, ready := range groups {
		value := 0.0
		if ready {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(c.ready, prometheus.GaugeValue, value, rg)
	}
}
