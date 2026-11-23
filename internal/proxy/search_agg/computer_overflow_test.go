package search_agg

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchAggregationIntegerOverflow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		op       string
		values   []int64
		want     any
		overflow bool
	}{
		{"avg", "avg", []int64{math.MaxInt64, 1}, math.Exp2(62), false},
		{"sum cancellation", "sum", []int64{math.MaxInt64, 1, -1}, int64(math.MaxInt64), false},
		{"sum overflow", "sum", []int64{math.MaxInt64, 1}, nil, true},
		{"sum underflow", "sum", []int64{math.MinInt64, -1}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestAggregationContext(t, 1, []LevelContext{{
				OwnFieldIDs: []int64{101}, Size: 10,
				Metrics: map[string]MetricSpec{
					"metric_value": {Op: tc.op, FieldID: 102, FieldType: schemapb.DataType_Int64},
					"avg_value":    {Op: "avg", FieldID: 102, FieldType: schemapb.DataType_Int64},
					"count_value":  {Op: "count", FieldID: 102, FieldType: schemapb.DataType_Int64},
				},
				Order: []OrderCriterion{{Key: "metric_value", Dir: "desc"}},
			}}, nil, []int64{102})
			// Interleave two buckets, each with a NULL. The first bucket needs
			// promotion; the second must remain independent and sort correctly.
			groups := []string{"B", "A", "B", "A"}
			valid := []bool{true, false, false, true}
			values := []int64{0, tc.values[0]}
			for _, value := range tc.values[1:] {
				groups = append(groups, "A")
				valid = append(valid, true)
				values = append(values, value)
			}
			field := testLongFieldData(102, values)
			field.ValidData = valid
			data := &schemapb.SearchResultData{
				NumQueries: 1, Topks: []int64{int64(len(groups))},
				FieldsData:         []*schemapb.FieldData{field},
				GroupByFieldValues: []*schemapb.FieldData{testStringFieldData(101, groups)},
			}
			result, err := NewSearchAggregationComputer(data, ctx, true).Compute(context.Background())
			if tc.overflow {
				require.Nil(t, result)
				require.ErrorIs(t, err, merr.ErrAggregateOverflow)
				require.Contains(t, err.Error(), `metric "metric_value"`)
				status := merr.Status(err)
				require.Equal(t, merr.Code(merr.ErrAggregateOverflow), status.GetCode())
				require.False(t, status.GetRetriable())
				return
			}
			require.NoError(t, err)
			require.Len(t, result[0], 2)
			require.Equal(t, "A", result[0][0].Key[101])
			require.Equal(t, tc.want, result[0][0].Metrics["metric_value"])
			require.Equal(t, int64(len(tc.values)), result[0][0].Metrics["count_value"])
			require.Equal(t, "B", result[0][1].Key[101])
			require.Equal(t, int64(1), result[0][1].Metrics["count_value"])
		})
	}
}
