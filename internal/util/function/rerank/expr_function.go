package rerank

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// ExprRerank allows users to define reranking logic using golang expression language https://github.com/expr-lang/expr
type ExprRerank[T PKType] struct {
	RerankBase

	program       *vm.Program
	exprString    string
	needNormalize bool
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

	if base.pkType == schemapb.DataType_Int64 {
		return &ExprRerank[int64]{
			RerankBase:    *base,
			program:       program,
			exprString:    exprCode,
			needNormalize: needNormalize,
		}, nil
	}
	return &ExprRerank[string]{
		RerankBase:    *base,
		program:       program,
		exprString:    exprCode,
		needNormalize: needNormalize,
	}, nil
}

func (e *ExprRerank[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	newScores := map[T]float32{}
	idLocations := make(map[T]IDLoc)

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

			fields := make(map[string]interface{}, len(e.inputFieldNames))
			for fieldIdx, fieldName := range e.inputFieldNames {
				if fieldIdx < len(col.data) {
					switch data := col.data[fieldIdx].(type) {
					case []int32:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					case []int64:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					case []float32:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					case []float64:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					case []string:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					case []bool:
						if idx < len(data) {
							fields[fieldName] = data[idx]
						}
					}
				}
			}
			env["fields"] = fields

			// Execute the expression
			output, err := expr.Run(e.program, env)
			if err != nil {
				return nil, fmt.Errorf("failed to execute expression for id %v: %w", id, err)
			}

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
			default:
				return nil, fmt.Errorf("expression returned unsupported type %T for id %v", output, id)
			}

			newScores[id] = newScore
			idLocations[id] = IDLoc{batchIdx: colIdx, offset: idx}
		}
	}

	if searchParams.isGrouping() {
		return newGroupingIDScores(newScores, idLocations, searchParams, idGroup)
	}
	return newIDScores(newScores, idLocations, searchParams, true), nil
}

func (e *ExprRerank[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(inputs, searchParams)

	for _, cols := range inputs.data {
		idScore, err := e.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(inputs, outputs, idScore)
	}

	return outputs, nil
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", s)
	}
}
