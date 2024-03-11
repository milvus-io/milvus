package planparserv2

import (
	"fmt"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var arithExprMap = map[int]planpb.ArithOpType{
	parser.PlanParserADD: planpb.ArithOpType_Add,
	parser.PlanParserSUB: planpb.ArithOpType_Sub,
	parser.PlanParserMUL: planpb.ArithOpType_Mul,
	parser.PlanParserDIV: planpb.ArithOpType_Div,
	parser.PlanParserMOD: planpb.ArithOpType_Mod,
}

var arithNameMap = map[int]string{
	parser.PlanParserADD: "add",
	parser.PlanParserSUB: "subtract",
	parser.PlanParserMUL: "multiply",
	parser.PlanParserDIV: "divide",
	parser.PlanParserMOD: "modulo",
}

var cmpOpMap = map[int]planpb.OpType{
	parser.PlanParserLT: planpb.OpType_LessThan,
	parser.PlanParserLE: planpb.OpType_LessEqual,
	parser.PlanParserGT: planpb.OpType_GreaterThan,
	parser.PlanParserGE: planpb.OpType_GreaterEqual,
	parser.PlanParserEQ: planpb.OpType_Equal,
	parser.PlanParserNE: planpb.OpType_NotEqual,
}

var cmpNameMap = map[int]string{
	parser.PlanParserLT: "less",
	parser.PlanParserLE: "lessequal",
	parser.PlanParserGT: "greater",
	parser.PlanParserGE: "greaterequal",
	parser.PlanParserEQ: "equal",
	parser.PlanParserNE: "notequal",
}

var unaryLogicalOpMap = map[int]planpb.UnaryExpr_UnaryOp{
	parser.PlanParserNOT: planpb.UnaryExpr_Not,
}

var unaryLogicalNameMap = map[int]string{
	parser.PlanParserNOT: "not",
}

var binaryLogicalOpMap = map[int]planpb.BinaryExpr_BinaryOp{
	parser.PlanParserAND: planpb.BinaryExpr_LogicalAnd,
	parser.PlanParserOR:  planpb.BinaryExpr_LogicalOr,
}

var binaryLogicalNameMap = map[int]string{
	parser.PlanParserAND: "and",
	parser.PlanParserOR:  "or",
}

func Add(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "+")
	}

	if IsString(a) || IsString(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "+")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if aFloat && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() + b.GetFloatVal())
	} else if aFloat && bInt {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() + float64(b.GetInt64Val()))
	} else if aInt && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(float64(a.GetInt64Val()) + b.GetFloatVal())
	} else {
		// aInt && bInt
		ret.dataType = schemapb.DataType_Int64
		ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() + b.GetInt64Val())
	}

	return ret, nil
}

func Subtract(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "-")
	}

	if IsString(a) || IsString(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "-")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if aFloat && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() - b.GetFloatVal())
	} else if aFloat && bInt {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() - float64(b.GetInt64Val()))
	} else if aInt && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(float64(a.GetInt64Val()) - b.GetFloatVal())
	} else {
		// aInt && bInt
		ret.dataType = schemapb.DataType_Int64
		ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() - b.GetInt64Val())
	}

	return ret, nil
}

func Multiply(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "*")
	}

	if IsString(a) || IsString(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "*")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if aFloat && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() * b.GetFloatVal())
	} else if aFloat && bInt {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() * float64(b.GetInt64Val()))
	} else if aInt && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(float64(a.GetInt64Val()) * b.GetFloatVal())
	} else {
		// aInt && bInt
		ret.dataType = schemapb.DataType_Int64
		ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() * b.GetInt64Val())
	}

	return ret, nil
}

func Divide(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "/")
	}

	if IsString(a) || IsString(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "/")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if bFloat && b.GetFloatVal() == 0 {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", "Float", "Zero", "/")
	}

	if bInt && b.GetInt64Val() == 0 {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", "Int", "Zero", "/")
	}

	if aFloat && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() / b.GetFloatVal())
	} else if aFloat && bInt {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(a.GetFloatVal() / float64(b.GetInt64Val()))
	} else if aInt && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(float64(a.GetInt64Val()) / b.GetFloatVal())
	} else {
		// aInt && bInt
		ret.dataType = schemapb.DataType_Int64
		ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() / b.GetInt64Val())
	}

	return ret, nil
}

func Modulo(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	aInt, bInt := IsInteger(a), IsInteger(b)
	if !aInt || !bInt {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "%")
	}

	// aInt && bInt
	if b.GetInt64Val() == 0 {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", "Int", "Zero", "%")
	}

	ret.dataType = schemapb.DataType_Int64
	ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() % b.GetInt64Val())

	return ret, nil
}

func Power(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "**")
	}

	if IsString(a) || IsString(b) {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "**")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if aFloat && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(math.Pow(a.GetFloatVal(), b.GetFloatVal()))
	} else if aFloat && bInt {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(math.Pow(a.GetFloatVal(), float64(b.GetInt64Val())))
	} else if aInt && bFloat {
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(math.Pow(float64(a.GetInt64Val()), b.GetFloatVal()))
	} else {
		// aInt && bInt
		// 2 ** (-1) = 0.5
		ret.dataType = schemapb.DataType_Double
		ret.expr.GetValueExpr().Value = NewFloat(math.Pow(float64(a.GetInt64Val()), float64(b.GetInt64Val())))
	}

	return ret, nil
}

func BitAnd(a, b *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func BitOr(a, b *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func BitXor(a, b *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func ShiftLeft(a, b *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func ShiftRight(a, b *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func And(a, b *planpb.GenericValue) (*ExprWithType, error) {
	aBool, bBool := IsBool(a), IsBool(b)
	if !aBool || !bBool {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "and")
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewBool(a.GetBoolVal() && b.GetBoolVal()),
				},
			},
		},
	}, nil
}

func Or(a, b *planpb.GenericValue) (*ExprWithType, error) {
	aBool, bBool := IsBool(a), IsBool(b)
	if !aBool || !bBool {
		return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "or")
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewBool(a.GetBoolVal() || b.GetBoolVal()),
				},
			},
		},
	}, nil
}

func BitNot(a *planpb.GenericValue) (*ExprWithType, error) {
	return nil, fmt.Errorf("todo: unsupported")
}

func Negative(a *planpb.GenericValue) (*ExprWithType, error) {
	if IsFloating(a) {
		return &ExprWithType{
			dataType: schemapb.DataType_Double,
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: NewFloat(-a.GetFloatVal()),
					},
				},
			},
		}, nil
	}
	if IsInteger(a) {
		return &ExprWithType{
			dataType: schemapb.DataType_Int64,
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: NewInt(-a.GetInt64Val()),
					},
				},
			},
		}, nil
	}
	return nil, merr.WrapErrParseExprFailed(nil, "", "negative are only supported on numeric")
}

func Not(a *planpb.GenericValue) (*ExprWithType, error) {
	if !IsBool(a) {
		return nil, merr.WrapErrParseExprFailed(nil, "", "not are only supported on bool")
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewBool(!a.GetBoolVal()),
				},
			},
		},
	}, nil
}

/*
type relationalFn func(a, b *planpb.GenericValue) (bool, error)

func applyRelational(a, b *planpb.GenericValue, relational relationalFn) *ExprWithType {
	ret, err := relational(a, b)
	if err != nil {
		return nil
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewBool(ret),
				},
			},
		},
	}
}

func less() relationalFn {
	return func(a, b *planpb.GenericValue) (bool, error) {
		if IsString(a) && IsString(b) {
			return a.GetStringVal() < b.GetStringVal(), nil
		}

		aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
		if aFloat && bFloat {
			return a.GetFloatVal() < b.GetFloatVal(), nil
		} else if aFloat && bInt {
			return a.GetFloatVal() < float64(b.GetInt64Val()), nil
		} else if aInt && bFloat {
			return float64(a.GetInt64Val()) < b.GetFloatVal(), nil
		} else if aInt && bInt {
			return a.GetInt64Val() < b.GetInt64Val(), nil
		}

		return false, fmt.Errorf("incompatible data type")
	}
}

func Less(a, b *planpb.GenericValue) *ExprWithType {
	return applyRelational(a, b, less())
}

// TODO: Can we abstract these relational function?
*/

func Less(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() < b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() < b.GetFloatVal())
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() < float64(b.GetInt64Val()))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) < b.GetFloatVal())
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() < b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "<")
}

func LessEqual(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() <= b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() <= b.GetFloatVal())
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() <= float64(b.GetInt64Val()))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) <= b.GetFloatVal())
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() <= b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "<=")
}

func Greater(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() > b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() > b.GetFloatVal())
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() > float64(b.GetInt64Val()))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) > b.GetFloatVal())
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() > b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), ">")
}

func GreaterEqual(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() >= b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() >= b.GetFloatVal())
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() >= float64(b.GetInt64Val()))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) >= b.GetFloatVal())
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() >= b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), ">=")
}

func Equal(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsBool(a) && IsBool(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetBoolVal() == b.GetBoolVal())
		return ret, nil
	}

	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() == b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(a.GetFloatVal(), b.GetFloatVal()))
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(a.GetFloatVal(), float64(b.GetInt64Val())))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(float64(a.GetInt64Val()), b.GetFloatVal()))
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() == b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "==")
}

func NotEqual(a, b *planpb.GenericValue) (*ExprWithType, error) {
	ret := &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}
	if IsBool(a) && IsBool(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetBoolVal() != b.GetBoolVal())
		return ret, nil
	}

	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() != b.GetStringVal())
		return ret, nil
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(a.GetFloatVal(), b.GetFloatVal()))
		return ret, nil
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(a.GetFloatVal(), float64(b.GetInt64Val())))
		return ret, nil
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(float64(a.GetInt64Val()), b.GetFloatVal()))
		return ret, nil
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() != b.GetInt64Val())
		return ret, nil
	}
	return nil, merr.WrapErrParseExprUnsupported(nil, "", GetGenericValueType(a), GetGenericValueType(b), "!=")
}
