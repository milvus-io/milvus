package segments

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAggregateValidityAfterOrdering(t *testing.T) {
	paramtable.Init()
	schema := testQueryPipelineSchema()
	schema.Fields[1].Nullable = true
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "g", DataType: schemapb.DataType_Int64})
	for _, op := range []planpb.AggregateOp{planpb.AggregateOp_min, planpb.AggregateOp_max, planpb.AggregateOp_sum} {
		for _, ordered := range []bool{false, true} {
			for _, limit := range []int64{1, 10} {
				t.Run(fmt.Sprintf("%s/ordered=%v/limit=%d", op, ordered, limit), func(t *testing.T) {
					aggs := []*planpb.Aggregate{{Op: op, FieldId: 101}}
					var order []*planpb.OrderByField
					if ordered {
						order = []*planpb.OrderByField{{FieldId: 102, Ascending: true}}
					}
					req := buildQueryReqForDelegator(limit, nil, order, []int64{102}, aggs)
					values := makeInt64Field(0, "", []int64{9, 0})
					typeutil.SetFieldDataValidData(values, []bool{true, false})
					input := &segcorepb.RetrieveResults{FieldsData: []*schemapb.FieldData{makeInt64Field(102, "g", []int64{2, 1}), values}}
					before := proto.Clone(input)
					pipeline, msg, err := buildStandardQNPipeline(req, schema, nil, []*segcorepb.RetrieveResults{input}, nil, nil)
					require.NoError(t, err)
					output, err := pipeline.Run(t.Context(), nil, msg)
					require.NoError(t, err)
					qn, err := extractSegcoreResult(output, []*segcorepb.RetrieveResults{input}, req, schema)
					require.NoError(t, err)
					shard, err := RunDelegatorQueryPipeline(t.Context(), req, schema, []*internalpb.RetrieveResults{{FieldsData: qn.FieldsData}})
					require.NoError(t, err)
					require.True(t, proto.Equal(before, input), "projection cannot mutate source fields")
					for _, fields := range [][]*schemapb.FieldData{qn.FieldsData, shard.FieldsData} {
						require.Equal(t, typeutil.GetFieldDataValidData(fields[1]), fields[1].GetValidData())
						// Decode the actual response as a released reader: only top-level validity.
						legacy := proto.Clone(&internalpb.RetrieveResults{FieldsData: fields}).(*internalpb.RetrieveResults)
						for _, field := range legacy.FieldsData {
							field.GetScalars().ValidData = nil
						}
						want := int64(5)
						if op == planpb.AggregateOp_max {
							want = -5
						}
						other := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{makeInt64Field(102, "g", []int64{1}), makeInt64Field(0, "", []int64{want})}}
						reducer := agg.NewGroupAggReducer([]int64{102}, aggs, -1, schema)
						partials, err := reducer.DecodeInternalResults([]*internalpb.RetrieveResults{legacy, other})
						require.NoError(t, err)
						merged, err := reducer.Merge(t.Context(), partials)
						require.NoError(t, err)
						for i, key := range merged.GetFieldDatas()[0].GetScalars().GetLongData().GetData() {
							if key == 1 {
								require.Equal(t, want, merged.GetFieldDatas()[1].GetScalars().GetLongData().GetData()[i])
							}
						}
					}
				})
			}
		}
	}
}

func TestEmptyQNGlobalIdentityProjectsValidity(t *testing.T) {
	schema := testQueryPipelineSchema()
	req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 101}, {Op: planpb.AggregateOp_count, FieldId: 101}}}}
	result, _, err := RunQNQueryPipeline(t.Context(), req, schema, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []bool{false}, result.FieldsData[0].GetValidData())
	require.Equal(t, []bool{true}, result.FieldsData[1].GetValidData())
}
