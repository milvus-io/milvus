package planparserv2

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/antlr4-go/antlr/v4"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	planparserv2 "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	exprCache   = expirable.NewLRU[string, any](1024, nil, time.Minute*10)
	trueLiteral = &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr:     alwaysTrueExpr(),
	}
)

type ExprParams struct {
	UseJSONStats bool
}

func ParseExprParams(vals map[string]*schemapb.TemplateValue) *ExprParams {
	ep := &ExprParams{
		UseJSONStats: paramtable.Get().CommonCfg.UsingJSONStatsForQuery.GetAsBool(),
	}

	if vals != nil {
		if v, ok := vals[common.ExprUseJSONStatsKey]; ok && v != nil {
			ep.UseJSONStats = v.GetBoolVal()
		}
	}
	return ep
}

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

func handleExprInternal(schema *typeutil.SchemaHelper, exprStr string, visitorArgs *ParserVisitorArgs) (result interface{}) {
	defer func() {
		if r := recover(); r != nil {
			result = fmt.Errorf("unsupported expression: %s", exprStr)
		}
	}()

	if isEmptyExpression(exprStr) {
		return trueLiteral
	}
	ast, err := handleInternal(exprStr)
	if err != nil {
		return err
	}

	visitor := NewParserVisitor(schema, visitorArgs)
	return ast.Accept(visitor)
}

func handleExpr(schema *typeutil.SchemaHelper, exprStr string) (result interface{}) {
	return handleExprInternal(schema, exprStr, &ParserVisitorArgs{})
}

func parseExprInner(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue, visitorArgs *ParserVisitorArgs) (*planpb.Expr, error) {
	ret := handleExprInternal(schema, exprStr, visitorArgs)

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

func ParseExpr(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.Expr, error) {
	return parseExprInner(schema, exprStr, exprTemplateValues, &ParserVisitorArgs{})
}

func parseIdentifierInner(schema *typeutil.SchemaHelper, identifier string, checkFunc func(*planpb.Expr) error, visitorArgs *ParserVisitorArgs) error {
	ret := handleExprInternal(schema, identifier, visitorArgs)

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

func ParseIdentifier(schema *typeutil.SchemaHelper, identifier string, checkFunc func(*planpb.Expr) error) error {
	visitorArgs := &ParserVisitorArgs{}
	return parseIdentifierInner(schema, identifier, checkFunc, visitorArgs)
}

func CreateRetrievePlanArgs(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue, visitorArgs *ParserVisitorArgs) (*planpb.PlanNode, error) {
	expr, err := parseExprInner(schema, exprStr, exprTemplateValues, visitorArgs)
	if err != nil {
		return nil, err
	}

	exprParams := ParseExprParams(exprTemplateValues)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: expr,
			},
		},
		PlanOptions: &planpb.PlanOption{
			ExprUseJsonStats: exprParams.UseJSONStats,
		},
	}
	return planNode, nil
}

func CreateRetrievePlan(schema *typeutil.SchemaHelper, exprStr string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, error) {
	visitorArgs := &ParserVisitorArgs{}
	return CreateRetrievePlanArgs(schema, exprStr, exprTemplateValues, visitorArgs)
}

func CreateSearchPlanArgs(schema *typeutil.SchemaHelper, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo, exprTemplateValues map[string]*schemapb.TemplateValue, functionScorer *schemapb.FunctionScore, visitorArgs *ParserVisitorArgs) (*planpb.PlanNode, error) {
	parse := func() (*planpb.Expr, error) {
		if len(exprStr) <= 0 {
			return nil, nil
		}
		return parseExprInner(schema, exprStr, exprTemplateValues, visitorArgs)
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
	fieldID := vectorField.FieldID
	dataType := vectorField.DataType
	elementType := vectorField.ElementType

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
	case schemapb.DataType_Int8Vector:
		vectorType = planpb.VectorType_Int8Vector
	case schemapb.DataType_ArrayOfVector:
		switch elementType {
		case schemapb.DataType_FloatVector:
			vectorType = planpb.VectorType_EmbListFloatVector
		default:
			log.Error("Invalid elementType", zap.Any("elementType", elementType))
			return nil, err
		}

	default:
		log.Error("Invalid dataType", zap.Any("dataType", dataType))
		return nil, err
	}

	scorers, options, err := CreateSearchScorers(schema, functionScorer, exprTemplateValues)
	if err != nil {
		return nil, err
	}

	if len(scorers) != 0 && (queryInfo.GroupByFieldId != -1 || queryInfo.SearchIteratorV2Info != nil) {
		return nil, fmt.Errorf("don't support use segment scorer with group_by or search_iterator")
	}

	exprParams := ParseExprParams(exprTemplateValues)

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
		Scorers:     scorers,
		ScoreOption: options,
		PlanOptions: &planpb.PlanOption{
			ExprUseJsonStats: exprParams.UseJSONStats,
		},
	}
	return planNode, nil
}

func prepareBoostRandomParams(schema *typeutil.SchemaHelper, bytes string) ([]*commonpb.KeyValuePair, error) {
	paramsMap := make(map[string]any)

	dec := json.NewDecoder(strings.NewReader(bytes))
	dec.UseNumber()

	err := dec.Decode(&paramsMap)
	if err != nil {
		return nil, err
	}

	result := make([]*commonpb.KeyValuePair, 0)
	for key, value := range paramsMap {
		switch key {
		// parse field name to field ID
		case RandomScoreFileNameKey:
			name, ok := value.(string)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("random seed field name must be string")
			}

			field, err := schema.GetFieldFromName(name)
			if err != nil {
				return nil, merr.WrapErrFieldNotFound(value, "random seed field not found")
			}

			if field.DataType != schemapb.DataType_Int64 {
				return nil, merr.WrapErrParameterInvalidMsg("only support int64 field as random seed, but got %s", field.DataType.String())
			}
			result = append(result, &commonpb.KeyValuePair{Key: RandomScoreFileIdKey, Value: fmt.Sprint(field.FieldID)})
		case RandomScoreSeedKey:
			number, ok := value.(json.Number)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("random seed must be number")
			}

			result = append(result, &commonpb.KeyValuePair{Key: key, Value: number.String()})
		}
	}
	return result, nil
}

func setBoostType(schema *typeutil.SchemaHelper, scorer *planpb.ScoreFunction, params []*commonpb.KeyValuePair) error {
	scorer.Type = planpb.FunctionType_FunctionTypeWeight
	for _, param := range params {
		switch param.GetKey() {
		case BoostRandomScoreKey:
			{
				scorer.Type = planpb.FunctionType_FunctionTypeRandom
				params, err := prepareBoostRandomParams(schema, param.GetValue())
				if err != nil {
					return err
				}
				scorer.Params = params
			}
		default:
		}
	}
	return nil
}

func CreateSearchScorer(schema *typeutil.SchemaHelper, function *schemapb.FunctionSchema, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.ScoreFunction, error) {
	rerankerName := rerank.GetRerankName(function)
	switch rerankerName {
	case rerank.BoostName:
		scorer := &planpb.ScoreFunction{}
		filter, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(rerank.FilterKey, function.GetParams())
		if ok {
			expr, err := ParseExpr(schema, filter, exprTemplateValues)
			if err != nil {
				return nil, fmt.Errorf("parse expr failed with error: {%v}", err)
			}
			scorer.Filter = expr
		}

		weightStr, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(rerank.WeightKey, function.GetParams())
		if !ok {
			return nil, fmt.Errorf("must set weight params for weight scorer")
		}

		weight, err := strconv.ParseFloat(weightStr, 32)
		if err != nil {
			return nil, fmt.Errorf("parse function scorer weight params failed with error: {%v}", err)
		}
		scorer.Weight = float32(weight)

		err = setBoostType(schema, scorer, function.GetParams())
		if err != nil {
			return nil, err
		}

		return scorer, nil
	default:
		// if not boost scorer, regard as normal function scorer
		// will be checked at ranker
		// return nil here
		return nil, nil
	}
}

func ParseBoostMode(s string) (planpb.BoostMode, error) {
	s = strings.ToLower(s)
	switch s {
	case "multiply":
		return planpb.BoostMode_BoostModeMultiply, nil
	case "sum":
		return planpb.BoostMode_BoostModeSum, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unknown boost mode: %s", s)
	}
}

func ParseFunctionMode(s string) (planpb.FunctionMode, error) {
	s = strings.ToLower(s)
	switch s {
	case "multiply":
		return planpb.FunctionMode_FunctionModeMultiply, nil
	case "sum":
		return planpb.FunctionMode_FunctionModeSum, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unknown function mode: %s", s)
	}
}

func CreateSearchScorers(schema *typeutil.SchemaHelper, functionScore *schemapb.FunctionScore, exprTemplateValues map[string]*schemapb.TemplateValue) ([]*planpb.ScoreFunction, *planpb.ScoreOption, error) {
	scorers := []*planpb.ScoreFunction{}
	for _, function := range functionScore.GetFunctions() {
		// create scorer for search plan
		scorer, err := CreateSearchScorer(schema, function, exprTemplateValues)
		if err != nil {
			return nil, nil, err
		}
		if scorer != nil {
			scorers = append(scorers, scorer)
		}
	}
	if len(scorers) == 0 {
		return nil, nil, nil
	}

	option := &planpb.ScoreOption{}

	s, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(BoostModeKey, functionScore.GetParams())
	if ok {
		boostMode, err := ParseBoostMode(s)
		if err != nil {
			return nil, nil, err
		}
		option.BoostMode = boostMode
	}

	s, ok = funcutil.TryGetAttrByKeyFromRepeatedKV(BoostFunctionModeKey, functionScore.GetParams())
	if ok {
		functionMode, err := ParseFunctionMode(s)
		if err != nil {
			return nil, nil, err
		}
		option.FunctionMode = functionMode
	}

	return scorers, option, nil
}

func CreateSearchPlan(schema *typeutil.SchemaHelper, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo, exprTemplateValues map[string]*schemapb.TemplateValue, functionScorer *schemapb.FunctionScore) (*planpb.PlanNode, error) {
	visitorArgs := &ParserVisitorArgs{}
	return CreateSearchPlanArgs(schema, exprStr, vectorFieldName, queryInfo, exprTemplateValues, functionScorer, visitorArgs)
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
