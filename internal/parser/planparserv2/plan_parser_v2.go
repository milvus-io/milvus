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

func handleExpr(schema *typeutil.SchemaHelper, exprStr string) (result interface{}) {
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

func CreateSearchPlan(schema *typeutil.SchemaHelper, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo, exprTemplateValues map[string]*schemapb.TemplateValue, functionScorer *schemapb.FunctionScore) (*planpb.PlanNode, error) {
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

	scorers, err := CreateSearchScorers(schema, functionScorer, exprTemplateValues)
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
		Scorers: scorers,
		PlanOptions: &planpb.PlanOption{
			ExprUseJsonStats: exprParams.UseJSONStats,
		},
	}
	return planNode, nil
}

func prepareBoostRandomParams(schema *typeutil.SchemaHelper, bytes string) ([]*commonpb.KeyValuePair, error) {
	paramsMap := make(map[string]any)

	log.Info("test--", zap.String("json", string(bytes)))

	dec := json.NewDecoder(strings.NewReader(bytes))
	dec.UseNumber()

	err := dec.Decode(&paramsMap)
	if err != nil {
		return nil, err
	}

	result := make([]*commonpb.KeyValuePair, 0)
	for key, value := range paramsMap {
		log.Info("test-- kv", zap.String("key", key), zap.Any("value", value))
		switch key {
		// parse field name to field ID
		case "field":
			name, ok := value.(string)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("random seed field name must be string")
			}

			field, err := schema.GetFieldFromName(name)
			if err != nil {
				return nil, merr.WrapErrFieldNotFound(value, "random seed field not found")
			}
			result = append(result, &commonpb.KeyValuePair{Key: "field_id", Value: fmt.Sprint(field.FieldID)})
		case "seed":
			number, ok := value.(json.Number)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("random seed must be int")
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
		case "random_score":
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

func CreateSearchScorers(schema *typeutil.SchemaHelper, functionScore *schemapb.FunctionScore, exprTemplateValues map[string]*schemapb.TemplateValue) ([]*planpb.ScoreFunction, error) {
	scorers := []*planpb.ScoreFunction{}
	for _, function := range functionScore.GetFunctions() {
		// create scorer for search plan
		scorer, err := CreateSearchScorer(schema, function, exprTemplateValues)
		if err != nil {
			return nil, err
		}
		if scorer != nil {
			scorers = append(scorers, scorer)
		}
	}
	if len(scorers) == 0 {
		return nil, nil
	}
	return scorers, nil
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
