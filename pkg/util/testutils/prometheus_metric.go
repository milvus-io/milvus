package testutils

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

// PromMetricsSuite is a util suite wrapper providing prometheus metrics assertion functions.
type PromMetricsSuite struct {
	suite.Suite
}

func (suite *PromMetricsSuite) MetricsEqual(c prometheus.Collector, expect float64, msgAndArgs ...any) bool {
	value := testutil.ToFloat64(c)
	return suite.Suite.Equal(expect, value, msgAndArgs...)
}

func (suite *PromMetricsSuite) CollectCntEqual(c prometheus.Collector, expect int, msgAndArgs ...any) bool {
	cnt := testutil.CollectAndCount(c)
	return suite.Suite.EqualValues(expect, cnt, msgAndArgs...)
}
