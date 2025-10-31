// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rerank

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ExprRerank allows users to define reranking logic using golang expression language https://github.com/expr-lang/expr
type ExprRerank[T PKType] struct {
	RerankBase

	program       *vm.Program
	exprString    string
	needNormalize bool

	// AST Field Pruning: only load fields that are actually used in the expression
	requiredFieldNames   []string       // Subset of inputFieldNames that are referenced in expression
	requiredFieldIndices map[string]int // Map from field name to index in inputFieldNames
	// collectionName captured from collection schema at creation time (best-effort)
	collectionName string
}

const (
	ExprName         = "expr"
	ExprCodeKey      = "expr_code"
	ExprNormalizeKey = "normalize"
)

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0.0 // fallback for unsupported types
	}
}

func extractFieldValue(data interface{}, idx int) (interface{}, bool, error) {
	if data == nil {
		return nil, true, nil
	}

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return nil, false, fmt.Errorf("expected slice, got %T", data)
	}

	if idx >= v.Len() {
		return nil, true, nil
	}

	elem := v.Index(idx)

	if !elem.IsValid() || (elem.Kind() == reflect.Ptr && elem.IsNil()) {
		return nil, true, nil
	}

	// Handle pointer types
	if elem.Kind() == reflect.Ptr {
		if elem.IsNil() {
			return nil, true, nil
		}
		elem = elem.Elem()
	}

	return elem.Interface(), false, nil
}

// mathFunctions contains all mathematical functions for expr-lang https://github.com/expr-lang/expr/blob/main/docs/language.md#math-functions
// Created once at package level to avoid recreating functions on every call
var mathFunctions = map[string]interface{}{
	// Basic math functions with type conversion
	"abs":   func(x interface{}) float64 { return math.Abs(toFloat64(x)) },
	"ceil":  func(x interface{}) float64 { return math.Ceil(toFloat64(x)) },
	"floor": func(x interface{}) float64 { return math.Floor(toFloat64(x)) },
	"round": func(x interface{}) float64 { return math.Round(toFloat64(x)) },
	"sqrt":  func(x interface{}) float64 { return math.Sqrt(toFloat64(x)) },
	"pow":   func(x, y interface{}) float64 { return math.Pow(toFloat64(x), toFloat64(y)) },

	// Exponential and logarithmic functions with type conversion
	"exp":   func(x interface{}) float64 { return math.Exp(toFloat64(x)) },
	"exp2":  func(x interface{}) float64 { return math.Exp2(toFloat64(x)) },
	"log":   func(x interface{}) float64 { return math.Log(toFloat64(x)) },
	"log10": func(x interface{}) float64 { return math.Log10(toFloat64(x)) },
	"log2":  func(x interface{}) float64 { return math.Log2(toFloat64(x)) },

	// Min/Max functions with type conversion
	"min": func(x, y interface{}) float64 { return math.Min(toFloat64(x), toFloat64(y)) },
	"max": func(x, y interface{}) float64 { return math.Max(toFloat64(x), toFloat64(y)) },

	// Utility functions with type conversion
	"mod":       func(x, y interface{}) float64 { return math.Mod(toFloat64(x), toFloat64(y)) },
	"remainder": func(x, y interface{}) float64 { return math.Remainder(toFloat64(x), toFloat64(y)) },
	"trunc":     func(x interface{}) float64 { return math.Trunc(toFloat64(x)) },

	// Clamping function with type conversion
	"clamp": func(x, min_val, max_val interface{}) float64 {
		xf := toFloat64(x)
		minf := toFloat64(min_val)
		maxf := toFloat64(max_val)
		if xf < minf {
			return minf
		}
		if xf > maxf {
			return maxf
		}
		return xf
	},

	// Mathematical constants
	"PI": math.Pi,
	"E":  math.E,
}

func newExprFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, ExprName, false)
	if err != nil {
		return nil, err
	}

	var exprCode string
	needNormalize := false

	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case ExprCodeKey:
			exprCode = param.Value
		case ExprNormalizeKey:
			if norm, err := parseBool(param.Value); err != nil {
				return nil, fmt.Errorf("invalid normalize value: %w", err)
			} else {
				needNormalize = norm
			}
		}
	}

	if exprCode == "" {
		return nil, fmt.Errorf("expr rerank requires %s parameter", ExprCodeKey)
	}

	// Create environment with mathematical functions
	env := make(map[string]interface{}, 3+len(mathFunctions))

	env["score"] = float32(0)
	env["rank"] = int(0)
	env["fields"] = map[string]interface{}{}

	for name, fn := range mathFunctions {
		env[name] = fn
	}

	program, err := expr.Compile(exprCode, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression: %w", err)
	}

	// AST Field Pruning: Analyze expression to determine which fields are actually used
	requiredFieldNames, requiredFieldIndices, err := analyzeRequiredFields(exprCode, base.inputFieldNames)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze required fields: %w", err)
	}

	// Log optimization impact
	totalFields := len(base.inputFieldNames)
	requiredFields := len(requiredFieldNames)
	if requiredFields < totalFields {
		reductionPct := float64(totalFields-requiredFields) / float64(totalFields) * 100
		log.Info("Expression reranker AST field pruning enabled",
			zap.String("expression", exprCode),
			zap.Int("total_available_fields", totalFields),
			zap.Int("required_fields", requiredFields),
			zap.Strings("pruned_fields", requiredFieldNames),
			zap.Float64("reduction_percentage", reductionPct))
	} else if requiredFields == 0 {
		log.Info("Expression reranker uses no fields (score/rank only)",
			zap.String("expression", exprCode))
	} else {
		log.Info("Expression reranker uses all available fields",
			zap.String("expression", exprCode),
			zap.Int("field_count", totalFields))
	}

	if base.pkType == schemapb.DataType_Int64 {
		er := &ExprRerank[int64]{
			RerankBase:           *base,
			program:              program,
			exprString:           exprCode,
			needNormalize:        needNormalize,
			requiredFieldNames:   requiredFieldNames,
			requiredFieldIndices: requiredFieldIndices,
			collectionName:       collSchema.GetName(),
		}
		registerRerankerForCollection(collSchema.GetName(), er)
		return er, nil
	}
	er := &ExprRerank[string]{
		RerankBase:           *base,
		program:              program,
		exprString:           exprCode,
		needNormalize:        needNormalize,
		requiredFieldNames:   requiredFieldNames,
		requiredFieldIndices: requiredFieldIndices,
		collectionName:       collSchema.GetName(),
	}
	registerRerankerForCollection(collSchema.GetName(), er)
	return er, nil
}

func (e *ExprRerank[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	newScores := map[T]float32{}
	idLocations := make(map[T]IDLoc)

	// Labels for error/field-load counters
	nodeIDStr := strconv.FormatInt(paramtable.GetNodeID(), 10)
	roleName := typeutil.QueryNodeRole
	collectionNameStr := e.collectionName
	rerankerName := e.GetRankName()

	// Determine sort order based on metric type (L2 = ascending, IP/COSINE = descending)
	_, descendingOrder := classifyMetricsOrder(searchParams.searchMetrics)

	for colIdx, col := range cols {
		if col.size == 0 {
			continue
		}

		ids := col.ids.([]T)
		scores := col.scores

		for idx, id := range ids {
			if _, exists := newScores[id]; exists {
				continue // Already processed (use first occurrence)
			}

			env := make(map[string]interface{}, 3+len(mathFunctions))

			env["score"] = scores[idx]
			env["rank"] = idx

			for name, fn := range mathFunctions {
				env[name] = fn
			}

			// AST Field Pruning: Only load fields that are actually used in the expression
			fields := make(map[string]interface{}, len(e.requiredFieldNames))
			fieldsLoaded := 0

			for _, fieldName := range e.requiredFieldNames {
				// Get the index in inputFieldNames array
				fieldIdx, ok := e.requiredFieldIndices[fieldName]
				if !ok || fieldIdx >= len(col.data) {
					continue
				}
				value, isNull, err := extractFieldValue(col.data[fieldIdx], idx)
				if err != nil {
					log.Warn("failed to extract field value",
						zap.String("field", fieldName),
						zap.Int("idx", idx),
						zap.Error(err))
					metrics.RerankErrors.WithLabelValues(roleName, nodeIDStr, collectionNameStr, rerankerName, "field_extraction_error").Inc()
					continue
				}

				if !isNull {
					fields[fieldName] = value
					fieldsLoaded++
				}
				// NULL fields are omitted from the fields map, expression must handle missing keys
			}

			env["fields"] = fields

			output, err := expr.Run(e.program, env)
			if err != nil {
				metrics.RerankErrors.WithLabelValues(roleName, nodeIDStr, collectionNameStr, rerankerName, "evaluation_error").Inc()
				return nil, fmt.Errorf(
					"failed to execute rerank expression\n"+
						"  ID: %v\n"+
						"  Expression: %s\n"+
						"  Score: %v (rank: %v)\n"+
						"  Fields: %+v\n"+
						"  Error: %w",
					id, e.exprString, env["score"], env["rank"], env["fields"], err,
				)
			}

			// Convert output to float32 score with type validation
			var newScore float32
			switch v := output.(type) {
			case float64:
				newScore = float32(v)
			case float32:
				newScore = v
			case int:
				newScore = float32(v)
			case int64:
				newScore = float32(v)
			case int32:
				newScore = float32(v)
			default:
				metrics.RerankErrors.WithLabelValues(roleName, nodeIDStr, collectionNameStr, rerankerName, "evaluation_error").Inc()
				return nil, fmt.Errorf(
					"expression returned unsupported type\n"+
						"  ID: %v\n"+
						"  Expression: %s\n"+
						"  Expected: numeric type (float/int)\n"+
						"  Got: %T with value: %v\n"+
						"  Hint: Ensure expression returns a number, not %T",
					id, e.exprString, output, output, output,
				)
			}

			newScores[id] = newScore
			idLocations[id] = IDLoc{batchIdx: colIdx, offset: idx}
		}
	}

	if searchParams.isGrouping() {
		return newGroupingIDScores(newScores, idLocations, searchParams, idGroup)
	}
	return newIDScores(newScores, idLocations, searchParams, descendingOrder), nil
}

func (e *ExprRerank[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(inputs, searchParams)

	// Metrics tracking aggregated at Process level
	nodeIDStr := strconv.FormatInt(paramtable.GetNodeID(), 10)
	roleName := typeutil.QueryNodeRole
	collectionNameStr := e.collectionName
	rerankerName := e.GetRankName()
	startTime := time.Now()
	totalResults := 0

	defer func() {
		elapsed := time.Since(startTime).Milliseconds()
		metrics.RerankLatency.WithLabelValues(roleName, nodeIDStr, collectionNameStr, rerankerName).Observe(float64(elapsed))
		metrics.RerankResultCount.WithLabelValues(roleName, nodeIDStr, collectionNameStr, rerankerName).Add(float64(totalResults))
	}()

	for _, cols := range inputs.data {
		idScore, err := e.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		totalResults += int(idScore.size)
		appendResult(inputs, outputs, idScore)
	}

	return outputs, nil
}

// GetInputFieldNames returns only the required fields (AST-pruned subset)
// This overrides the base implementation to enable field pruning optimization
func (e *ExprRerank[T]) GetInputFieldNames() []string {
	if len(e.requiredFieldNames) > 0 {
		// Return only fields that are actually used in the expression
		return e.requiredFieldNames
	}
	// Fallback to all fields if analysis failed or no fields are used
	return e.RerankBase.GetInputFieldNames()
}

// GetInputFieldIDs returns only the required field IDs (AST-pruned subset)
// This overrides the base implementation to enable field pruning optimization
func (e *ExprRerank[T]) GetInputFieldIDs() []int64 {
	if len(e.requiredFieldNames) == 0 {
		// No fields required, return empty
		return []int64{}
	}

	// Build pruned field ID list based on required fields
	requiredFieldIDs := make([]int64, 0, len(e.requiredFieldNames))
	for _, fieldName := range e.requiredFieldNames {
		if fieldIdx, ok := e.requiredFieldIndices[fieldName]; ok {
			if fieldIdx < len(e.inputFieldIDs) {
				requiredFieldIDs = append(requiredFieldIDs, e.inputFieldIDs[fieldIdx])
			}
		}
	}
	return requiredFieldIDs
}

// Close releases any long-lived resources held by ExprRerank
func (e *ExprRerank[T]) Close() {
	e.program = nil
	e.requiredFieldNames = nil
	e.requiredFieldIndices = nil
	e.exprString = ""
	e.collectionName = ""
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "1", "yes", "y":
		return true, nil
	case "false", "0", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", s)
	}
}
