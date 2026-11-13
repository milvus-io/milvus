package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestQueryCoordComplianceScrapeLifecycle(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(queryCoordCompliance)
	SetQueryCoordComplianceEvaluator(nil)
	t.Cleanup(func() { SetQueryCoordComplianceEvaluator(nil) })
	read := func() map[string]map[string]float64 {
		families, err := registry.Gather()
		require.NoError(t, err)
		result := make(map[string]map[string]float64)
		for _, family := range families {
			values := make(map[string]float64)
			for _, metric := range family.Metric {
				rg := ""
				if len(metric.Label) > 0 {
					rg = metric.Label[0].GetValue()
				}
				values[rg] = metric.Gauge.GetValue()
			}
			result[family.GetName()] = values
		}
		return result
	}
	const success = "milvus_querycoord_resource_group_compliance_check_success"
	const compliance = "milvus_querycoord_resource_group_compliance"
	require.Empty(t, read(), "standby must not publish a verdict")
	groups := map[string]bool{"A": true, "B": false}
	var evaluationError error
	calls := 0
	SetQueryCoordComplianceEvaluator(func() (map[string]bool, error) {
		calls++
		return groups, evaluationError
	})
	require.Equal(t, map[string]map[string]float64{
		success: {"": 1}, compliance: {"A": 1, "B": 0},
	}, read(), "one unhealthy RG must not poison a healthy RG")
	delete(groups, "A")
	groups["B"] = true
	require.Equal(t, map[string]float64{"B": 1}, read()[compliance], "deleted RGs disappear on the next scrape")
	evaluationError = errors.New("metadata unavailable")
	require.Equal(t, map[string]map[string]float64{success: {"": 0}}, read(), "errors must not retain Ready")
	evaluationError = nil
	require.Equal(t, map[string]float64{"B": 1}, read()[compliance], "recovery re-evaluates")
	require.Equal(t, 4, calls)
	SetQueryCoordComplianceEvaluator(nil)
	require.Empty(t, read(), "stopped coordinator must not publish")
	require.Equal(t, 4, calls)
}

func TestQueryCoordComplianceShutdownWaitsForEvaluation(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(queryCoordCompliance)
	entered, release := make(chan struct{}), make(chan struct{})
	SetQueryCoordComplianceEvaluator(func() (map[string]bool, error) {
		close(entered)
		<-release
		return map[string]bool{"A": true}, nil
	})
	t.Cleanup(func() { SetQueryCoordComplianceEvaluator(nil) })
	gathered := make(chan error, 1)
	go func() {
		_, err := registry.Gather()
		gathered <- err
	}()
	<-entered
	stopping, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		close(stopping)
		SetQueryCoordComplianceEvaluator(nil)
		close(stopped)
	}()
	<-stopping
	select {
	case <-stopped:
		t.Error("shutdown returned before the evaluator released coordinator dependencies")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-gathered)
	<-stopped
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}
