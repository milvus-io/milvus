package queryclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSearchPlanReadUsage(t *testing.T) {
	plan := ShardPlan{Usage: &viewpb.PlannedQueryUsage{
		Reads: []*viewpb.PlannedColumnRead{
			{FieldIds: []int64{100, 100, 101}, Nq: 2, Columns: &viewpb.FieldLogicalBytes{
				PayloadBytes: map[int64]uint64{100: 5, 101: 37}, ValidityBits: map[int64]uint64{100: 3},
			}},
			{FieldIds: []int64{100, 102}, Nq: 3, Columns: &viewpb.FieldLogicalBytes{
				PayloadBytes: map[int64]uint64{100: 11, 102: 0},
			}},
		},
		FilterOnly: &viewpb.PlannedColumnRead{FieldIds: []int64{100}, Nq: 2, Columns: &viewpb.FieldLogicalBytes{
			PayloadBytes: map[int64]uint64{100: 5},
		}},
	}}
	usage, err := searchPlanReadUsage(plan)
	require.NoError(t, err)
	require.EqualValues(t, 127, usage.GetDataBytes())
	require.EqualValues(t, 12, usage.GetValidityBits()[100])
	plan.FilterOnlyRan = true
	usage, err = searchPlanReadUsage(plan)
	require.NoError(t, err)
	require.EqualValues(t, 137, usage.GetDataBytes())

	delete(plan.Usage.Reads[1].Columns.PayloadBytes, 102)
	_, err = searchPlanReadUsage(plan)
	require.ErrorContains(t, err, "missing logical column size for field 102")
}

func TestQueryPlanReadUsageRequeryPartitions(t *testing.T) {
	plan := ShardPlan{Usage: &viewpb.PlannedQueryUsage{
		RequeryPartitions: map[int64]*viewpb.PlannedColumnRead{
			1: {FieldIds: []int64{100}, Nq: 1, Columns: &viewpb.FieldLogicalBytes{PayloadBytes: map[int64]uint64{100: 32}}},
			2: {FieldIds: []int64{100}, Nq: 1, Columns: &viewpb.FieldLogicalBytes{}},
		},
	}}
	usage, err := queryPlanReadUsage(plan, []int64{1}, true)
	require.NoError(t, err)
	require.EqualValues(t, 32, usage.GetDataBytes())
	_, err = queryPlanReadUsage(plan, nil, true)
	require.ErrorContains(t, err, "missing logical column size for field 100")
}
