package viewquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReferencedSchemaFieldsIncludesHiddenDependencies(t *testing.T) {
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{
		Predicates: &planpb.Expr{Expr: &planpb.Expr_CompareExpr{CompareExpr: &planpb.CompareExpr{
			LeftColumnInfo: &planpb.ColumnInfo{FieldId: 102}, RightColumnInfo: &planpb.ColumnInfo{FieldId: 103},
		}}},
		GroupByFieldIds: []int64{104}, Aggregates: []*planpb.Aggregate{{FieldId: 105}}, OrderByFields: []*planpb.OrderByField{{FieldId: 106}},
	}}, OutputFieldIds: []int64{107}}
	serialized, err := proto.Marshal(plan)
	require.NoError(t, err)
	request := &internalpb.RetrieveRequest{CollectionID: 999, SerializedExprPlan: serialized, OutputFieldsId: []int64{108}}
	schema := &schemapb.CollectionSchema{
		Fields:     []*schemapb.FieldSchema{{FieldID: 109, Name: "ttl"}},
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "ttl"}},
		Functions:  []*schemapb.FunctionSchema{{OutputFieldIds: []int64{108}, InputFieldIds: []int64{110}}, {OutputFieldIds: []int64{110}, InputFieldIds: []int64{111}}},
	}
	fields, err := ReferencedSchemaFields(request, schema)
	require.NoError(t, err)
	require.Equal(t, []int64{102, 103, 104, 105, 106, 107, 108, 109, 110, 111}, fields)
}

func TestSchemaBindingRejectsMVCCSchemaAndFieldChanges(t *testing.T) {
	request := &internalpb.RetrieveRequest{SchemaVersion: 0, OutputFieldsId: []int64{100}}
	original := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 99, SchemaBinding: &viewpb.QuerySchemaBinding{SchemaVersion: 0, GrowingTimetick: 100, TransformingTimetick: 99, FieldIds: []int64{100}}}
	require.NoError(t, ValidateSchemaBinding(request, original, nil))
	for _, change := range []string{"schema", "growing mvcc", "transform mvcc", "field"} {
		t.Run(change, func(t *testing.T) {
			req := proto.Clone(request).(*internalpb.RetrieveRequest)
			mvcc := proto.Clone(original).(*viewpb.QueryPlanMVCC)
			switch change {
			case "schema":
				req.SchemaVersion = 1
			case "growing mvcc":
				mvcc.GrowingTimetick++
			case "transform mvcc":
				mvcc.TransformingTimetick++
			case "field":
				req.OutputFieldsId = append(req.OutputFieldsId, 101)
			}
			require.True(t, viewerror.AsViewError(ValidateSchemaBinding(req, mvcc, nil)).IsViewInvalidated())
		})
	}
}
