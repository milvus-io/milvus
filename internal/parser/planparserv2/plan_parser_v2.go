package planparserv2

import (
	"fmt"
	"time"

	"github.com/antlr4-go/antlr/v4"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	planparserv2 "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	exprCache   = expirable.NewLRU[string, any](1024, nil, time.Minute*10)
	trueLiteral = &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr:     alwaysTrueExpr(),
	}
)

func handleInternal(exprStr string) (ast planparserv2.IExprContext, err error) {
	val, ok := exprCache.Get(exprStr)
	if ok {
		switch v := val.(type) {
		case planparserv2.IExprContext:
			return v, nil
		case error:
			return nil, v
		default:
			return nil, fmt.Errorf("unknown cache error: %v", v)
		}
	}

	// Note that the errors will be cached, too.
	defer func() {
		if err != nil {
			exprCache.Add(exprStr, err)
		}
	}()
	exprNormal := convertHanToASCII(exprStr)
	listener := &errorListenerImpl{}

	inputStream := antlr.NewInputStream(exprNormal)
	lexer := getLexer(inputStream, listener)
	if err = listener.Error(); err != nil {
		return
	}

	parser := getParser(lexer, listener)
	if err = listener.Error(); err != nil {
		return
	}

	ast = parser.Expr()
	if err = listener.Error(); err != nil {
		return
	}

	if parser.GetCurrentToken().GetTokenType() != antlr.TokenEOF {
		log.Info("invalid expression", zap.String("expr", exprStr))
		err = fmt.Errorf("invalid expression: %s", exprStr)
		return
	}

	// lexer & parser won't be used by this thread, can be put into pool.
	putLexer(lexer)
	putParser(parser)

	exprCache.Add(exprStr, ast)
	return
}

func handleExpr(schema *typeutil.SchemaHelper, exprStr string) interface{} {
	if isEmptyExpression(exprStr) {
		return trueLiteral
	}
	ast, err := handleInternal(exprStr)
	if err != nil {
		return err
	}

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
