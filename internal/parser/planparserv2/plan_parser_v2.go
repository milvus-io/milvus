package planparserv2

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func handleExpr(schema *typeutil.SchemaHelper, exprStr string) interface{} {
	exprStr = convertHanToASCII(exprStr)
	return handleExprWithErrorListener(schema, exprStr, &errorListenerImpl{})
}

func handleExprWithErrorListener(schema *typeutil.SchemaHelper, exprStr string, errorListener errorListener) interface{} {
	if isEmptyExpression(exprStr) {
		return &ExprWithType{
			dataType: schemapb.DataType_Bool,
			expr:     alwaysTrueExpr(),
		}
	}

	inputStream := antlr.NewInputStream(exprStr)
	lexer := getLexer(inputStream, errorListener)
	if errorListener.Error() != nil {
		return errorListener.Error()
	}

	parser := getParser(lexer, errorListener)
	if errorListener.Error() != nil {
		return errorListener.Error()
	}

	ast := parser.Expr()
	if errorListener.Error() != nil {
		return errorListener.Error()
	}

	if parser.GetCurrentToken().GetTokenType() != antlr.TokenEOF {
		log.Info("invalid expression", zap.String("expr", exprStr))
		return fmt.Errorf("invalid expression: %s", exprStr)
	}

	// lexer & parser won't be used by this thread, can be put into pool.
	putLexer(lexer)
	putParser(parser)

	visitor := NewParserVisitor(schema)
	return ast.Accept(visitor)
}

func ParseExpr(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.Expr, error) {
	ret := handleExpr(schema, exprStr)

	if err := getError(ret); err != nil {
		return nil, fmt.Errorf("cannot parse expression: %s, error: %s", exprStr, err)
	}

	predicate := getExpr(ret)
	if predicate == nil {
		return nil, fmt.Errorf("cannot parse expression: %s", exprStr)
	}
	if !canBeExecuted(predicate) {
		return nil, fmt.Errorf("predicate is not a boolean expression: %s, data type: %s", exprStr, predicate.dataType)
	}

	valueMap, err := UnmarshalExpressionValues(exprTemplateValues)
	if err != nil {
		return nil, err
	}

	if err := FillExpressionValue(predicate.expr, valueMap); err != nil {
		return nil, err
	}

	return predicate.expr, nil
}

func ParseIdentifier(schema *typeutil.SchemaHelper, identifier string, checkFunc func(*planpb.Expr) error) error {
	ret := handleExpr(schema, identifier)

	if err := getError(ret); err != nil {
		return fmt.Errorf("cannot parse identifier: %s, error: %s", identifier, err)
	}

	predicate := getExpr(ret)
	if predicate == nil {
		return fmt.Errorf("cannot parse identifier: %s", identifier)
	}
	if predicate.expr.GetColumnExpr() == nil {
		return fmt.Errorf("cannot parse identifier: %s", identifier)
	}

	return checkFunc(predicate.expr)
}

func CreateRetrievePlan(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, error) {
	expr, err := ParseExpr(schema, exprStr, exprTemplateValues)
	if err != nil {
		return nil, err
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: expr,
			},
		},
	}
	return planNode, nil
}

func CreateSearchPlan(schema *typeutil.SchemaHelper, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, error) {
	parse := func() (*planpb.Expr, error) {
		if len(exprStr) <= 0 {
			return nil, nil
		}
		return ParseExpr(schema, exprStr, exprTemplateValues)
	}

	expr, err := parse()
	if err != nil {
		log.Info("CreateSearchPlan failed", zap.Error(err))
		return nil, err
	}
	vectorField, err := schema.GetFieldFromName(vectorFieldName)
	if err != nil {
		log.Info("CreateSearchPlan failed", zap.Error(err))
		return nil, err
	}
	// plan ok with schema, check ann field
	if !schema.IsFieldLoaded(vectorField.GetFieldID()) {
		return nil, merr.WrapErrParameterInvalidMsg("ann field \"%s\" not loaded", vectorFieldName)
	}
	fieldID := vectorField.FieldID
	dataType := vectorField.DataType

	var vectorType planpb.VectorType
	if !typeutil.IsVectorType(dataType) {
		return nil, fmt.Errorf("field (%s) to search is not of vector data type", vectorFieldName)
	}
	switch dataType {
	case schemapb.DataType_BinaryVector:
		vectorType = planpb.VectorType_BinaryVector
	case schemapb.DataType_FloatVector:
		vectorType = planpb.VectorType_FloatVector
	case schemapb.DataType_Float16Vector:
		vectorType = planpb.VectorType_Float16Vector
	case schemapb.DataType_BFloat16Vector:
		vectorType = planpb.VectorType_BFloat16Vector
	case schemapb.DataType_SparseFloatVector:
		vectorType = planpb.VectorType_SparseFloatVector
	default:
		log.Error("Invalid dataType", zap.Any("dataType", dataType))
		return nil, err
	}
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				VectorType:     vectorType,
				Predicates:     expr,
				QueryInfo:      queryInfo,
				PlaceholderTag: "$0",
				FieldId:        fieldID,
			},
		},
	}
	return planNode, nil
}

func CreateRequeryPlan(pkField *schemapb.FieldSchema, ids *schemapb.IDs) *planpb.PlanNode {
	var values []*planpb.GenericValue
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		values = lo.Map(ids.GetIntId().GetData(), func(id int64, _ int) *planpb.GenericValue {
			return &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: id,
				},
			}
		})
	case *schemapb.IDs_StrId:
		values = lo.Map(ids.GetStrId().GetData(), func(id string, _ int) *planpb.GenericValue {
			return &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: id,
				},
			}
		})
	}

	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:        pkField.GetFieldID(),
								DataType:       pkField.GetDataType(),
								IsPrimaryKey:   true,
								IsAutoID:       pkField.GetAutoID(),
								IsPartitionKey: pkField.GetIsPartitionKey(),
							},
							Values: values,
						},
					},
				},
				IsCount: false,
				Limit:   int64(len(values)),
			},
		},
	}
}
