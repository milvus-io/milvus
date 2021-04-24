package proxynode

import (
	"errors"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"strconv"
	"strings"
)

func getField(schema *schemapb.CollectionSchema, fieldName string) (field *schemapb.FieldSchema, err error) {
	// TODO: use schema helper
	for _, fieldIter := range schema.Fields {
		if fieldIter.GetName() == fieldName {
			field = fieldIter
			break
		}
	}
	if field == nil {
		return nil, errors.New("field not found")
	}
	return field, nil
}


func parseQueryExpr(schema *schemapb.CollectionSchema, exprStrNullable *string) (*planpb.Expr, error){
	// TODO: handle more cases
	// TODO: currently just A > 3
	if exprStrNullable == nil {
		return nil, nil
	}
	exprStr := *exprStrNullable

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

	field, err := getField(schema, fieldName)
	if err != nil {
		return nil, err
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

func CreateQueryPlan(schema *schemapb.CollectionSchema, exprStrNullable *string, vectorFieldName string, queryInfo *planpb.QueryInfo) (*planpb.PlanNode, error){
	expr, err := parseQueryExpr(schema, exprStrNullable)
	if err != nil {
		return nil, err
	}
	vectorField, err := getField(schema, vectorFieldName)
	if err != nil {
		return nil, err
	}
	fieldID := vectorField.FieldID

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				IsBinary: false,
				Predicates: expr,
				QueryInfo: queryInfo,
				PlaceholderTag: "$0",
				FieldId: fieldID,
			},
		},
	}
	return planNode, nil
}
