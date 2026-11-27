package adaptor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestSearchMeterFieldsIncludesOuterAggregationAndOrder(t *testing.T) {
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{
		VectorAnns: &planpb.VectorANNS{FieldId: 100},
	}}
	fields, err := searchMeterFields(plan, []int64{101, 102, 101}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{100, 101, 102, 101}, fields)
}
