package search_agg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type terminateSentinelAggregate struct{}

func (terminateSentinelAggregate) Name() string { return "sentinel" }

func (terminateSentinelAggregate) Update(target *agg.FieldValue, new *agg.FieldValue) error {
	return nil
}

func (terminateSentinelAggregate) NewState() []*agg.FieldValue {
	return []*agg.FieldValue{agg.NewNullFieldValue()}
}

func (terminateSentinelAggregate) UpdateState(slots []*agg.FieldValue, new *agg.FieldValue) error {
	return nil
}

func (terminateSentinelAggregate) Terminate(slots []*agg.FieldValue) (any, error) {
	return "terminated", nil
}

func (terminateSentinelAggregate) ToPB() *planpb.Aggregate { return nil }

func (terminateSentinelAggregate) FieldID() int64 { return 0 }

func (terminateSentinelAggregate) OriginalName() string { return "sentinel" }

func TestFinalizeMetricsUsesAggregateTerminate(t *testing.T) {
	metrics, err := finalizeMetrics([]metricPlan{{
		alias:     "sentinel_metric",
		spec:      MetricSpec{Op: "sum"},
		aggregate: terminateSentinelAggregate{},
	}}, map[string][]*agg.FieldValue{
		"sentinel_metric": {agg.NewFieldValue("raw-state")},
	})

	require.NoError(t, err)
	require.Equal(t, "terminated", metrics["sentinel_metric"])
}

func TestFinalizeMetricsRejectsMissingAggregate(t *testing.T) {
	_, err := finalizeMetrics([]metricPlan{{
		alias: "missing_aggregate",
		spec:  MetricSpec{Op: "sum"},
	}}, map[string][]*agg.FieldValue{
		"missing_aggregate": {agg.NewFieldValue("raw-state")},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "semantic aggregate is nil")
}

func TestCompileMetricPlansUsesAvgAggregateWrapper(t *testing.T) {
	plans, err := compileMetricPlans(map[string]MetricSpec{
		"avg_value": {Op: "avg", FieldID: 100, FieldType: schemapb.DataType_Int64},
	})

	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.IsType(t, &agg.AvgAggregate{}, plans[0].aggregate)
}

func TestFinalizeMetricsUsesAggregateOwnedState(t *testing.T) {
	plans, err := compileMetricPlans(map[string]MetricSpec{
		"avg_value": {Op: "avg", FieldID: 100, FieldType: schemapb.DataType_Int64},
	})
	require.NoError(t, err)

	state := plans[0].aggregate.NewState()
	require.NoError(t, plans[0].aggregate.UpdateState(state, agg.NewFieldValue(int64(10))))
	require.NoError(t, plans[0].aggregate.UpdateState(state, agg.NewFieldValue(int64(20))))

	metrics, err := finalizeMetrics(plans, map[string][]*agg.FieldValue{"avg_value": state})
	require.NoError(t, err)
	require.Equal(t, float64(15), metrics["avg_value"])
}
