package dql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryPipelineNullableAvg(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		for _, mode := range []string{"global-empty", "grouped", "grouped-ordered"} {
			t.Run(fmt.Sprintf("%s/%s", typ, mode), func(t *testing.T) {
				schema := testSchema()
				schema.Fields[1].DataType = typ
				schema.Fields[1].Nullable = true
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "g", DataType: schemapb.DataType_Int64})
				operators, err := agg.NewAggregate("avg", 101, "avg(val)", typ)
				require.NoError(t, err)
				var groupIDs []int64
				var groupNames []string
				names := []string{"avg(val)"}
				var inputs []*internalpb.RetrieveResults
				var original *internalpb.RetrieveResults
				if mode != "global-empty" {
					groupIDs = []int64{102}
					groupNames = []string{"g"}
					names = []string{"g", "avg(val)"}
					sum := makeTestInt64Field(0, "", []int64{0, 31})
					if typ == schemapb.DataType_Double {
						sum = &schemapb.FieldData{Type: typ, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{0, 31}}}}}}
					}
					typeutil.SetFieldDataValidData(sum, []bool{false, true})
					input := &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{makeTestInt64Field(102, "g", []int64{1, 2}), sum, makeTestInt64Field(0, "", []int64{0, 2})}}
					original = proto.Clone(input).(*internalpb.RetrieveResults)
					inputs = []*internalpb.RetrieveResults{input, {}}
				}
				outputMap, err := agg.NewAggregationFieldMap(names, groupNames, operators)
				require.NoError(t, err)
				var order []*orderby.OrderByField
				if mode == "grouped-ordered" {
					order = []*orderby.OrderByField{orderby.NewOrderByField(102, "g", schemapb.DataType_Int64)}
				}
				pipeline, err := NewQueryPipeline(schema, 10, 0, reduce.IReduceNoOrder, order, groupIDs, agg.AggregatesToPB(operators), outputMap, nil)
				require.NoError(t, err)
				result, err := pipeline.Execute(t.Context(), inputs)
				require.NoError(t, err)
				avg := result.FieldsData[len(result.FieldsData)-1]
				if mode == "global-empty" {
					require.Equal(t, []float64{0}, avg.GetScalars().GetDoubleData().GetData())
					require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(avg))
				} else {
					require.Equal(t, []float64{0, 15.5}, avg.GetScalars().GetDoubleData().GetData())
					require.Equal(t, []bool{false, true}, typeutil.GetFieldDataValidData(avg))
					require.True(t, proto.Equal(original, inputs[0]), "final remapping must not mutate input columns")
				}
			})
		}
	}
}
