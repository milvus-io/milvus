package proxynode

import (
	"errors"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"strconv"
	"strings"
)

func parseQueryExpr(schema *schemapb.CollectionSchema, exprStr string) (*planpb.Expr, error){
	// TODO: handle more cases
	// TODO: currently just A > 3
	tmps := strings.Split(exprStr, ">")
	if len(tmps) != 2 {
		return nil, errors.New("unsupported yet")
	}
	for i, str := range tmps {
		tmps[i] = strings.TrimSpace(str)
	}
	fieldName :=  tmps[0]
	tmpValue, err := strconv.ParseInt(tmps[1], 10, 64)
	if err != nil {
		return nil, err
	}
	fieldValue := &planpb.GenericValue{
		Val:  &planpb.GenericValue_Int64Val{Int64Val: tmpValue},
	}
	// TODO: use schema helper
	var field *schemapb.FieldSchema
	for _, fieldIter := range schema.Fields {
		if fieldIter.GetName() == fieldName {
			field = fieldIter
			break
		}
	}
	if field == nil {
		return nil, errors.New("field not found")
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_RangeExpr{
			RangeExpr: &planpb.RangeExpr{
				FieldId: field.FieldID,
				DataType: field.DataType,
				Ops: []planpb.RangeExpr_OpType{planpb.RangeExpr_GreaterThan},
				Values: []*planpb.GenericValue{fieldValue},
			},
		},
	}
	return expr, nil
}

func CreateQueryPlan(schema *schemapb.CollectionSchema, exprStr string, queryInfo *planpb.QueryInfo) (*planpb.PlanNode, error){
	expr, err := parseQueryExpr(schema, exprStr)
	if err != nil {
		return nil, err
	}
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				IsBinary: false,
				Predicates: expr,
				QueryInfo: queryInfo,
				PlaceholderTag: "$0",
			},
		},
	}
	return planNode, nil
}
