package testutils

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// PromMetricsSuite is a util suite wrapper providing prometheus metrics assertion functions.
type PromMetricsSuite struct {
	suite.Suite
}

func (suite *PromMetricsSuite) MetricsEqual(c prometheus.Collector, expect float64, msgAndArgs ...any) bool {
	value := testutil.ToFloat64(c)
	return suite.Equal(expect, value, msgAndArgs...)
}

func (suite *PromMetricsSuite) CollectCntEqual(c prometheus.Collector, expect int, msgAndArgs ...any) bool {
	cnt := testutil.CollectAndCount(c)
	return suite.EqualValues(expect, cnt, msgAndArgs...)
}

// GaugeSeries returns every series of c keyed by its label values, in
// labelNames order and joined with "|", without creating any series the way
// WithLabelValues would.
func GaugeSeries(t *testing.T, c prometheus.Collector, labelNames ...string) map[string]float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(c))
	families, err := registry.Gather()
	require.NoError(t, err)
	series := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.Len(t, metric.GetLabel(), len(labelNames))
			labels := make(map[string]string, len(labelNames))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			values := make([]string, 0, len(labelNames))
			for _, name := range labelNames {
				value, ok := labels[name]
				require.True(t, ok, "series has no label %q", name)
				values = append(values, value)
			}
			series[strings.Join(values, "|")] = metric.GetGauge().GetValue()
		}
	}
	return series
}
