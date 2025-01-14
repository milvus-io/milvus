package planparserv2

import (
	"fmt"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
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

func Add(a, b *planpb.GenericValue) *ExprWithType {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil
	}

	if IsString(a) || IsString(b) {
		return nil
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

	return ret
}

func Subtract(a, b *planpb.GenericValue) *ExprWithType {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil
	}

	if IsString(a) || IsString(b) {
		return nil
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

	return ret
}

func Multiply(a, b *planpb.GenericValue) *ExprWithType {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil
	}

	if IsString(a) || IsString(b) {
		return nil
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

	return ret
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
		return nil, fmt.Errorf("divide cannot apply on bool field")
	}

	if IsString(a) || IsString(b) {
		return nil, fmt.Errorf("divide cannot apply on string field")
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)

	if bFloat && b.GetFloatVal() == 0 {
		return nil, fmt.Errorf("cannot divide by zero")
	}

	if bInt && b.GetInt64Val() == 0 {
		return nil, fmt.Errorf("cannot divide by zero")
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
		return nil, fmt.Errorf("modulo can only apply on integer")
	}

	// aInt && bInt
	if b.GetInt64Val() == 0 {
		return nil, fmt.Errorf("cannot modulo by zero")
	}

	ret.dataType = schemapb.DataType_Int64
	ret.expr.GetValueExpr().Value = NewInt(a.GetInt64Val() % b.GetInt64Val())

	return ret, nil
}

func Power(a, b *planpb.GenericValue) *ExprWithType {
	ret := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{},
			},
		},
	}

	if IsBool(a) || IsBool(b) {
		return nil
	}

	if IsString(a) || IsString(b) {
		return nil
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

	return ret
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
		return nil, fmt.Errorf("and can only apply on boolean")
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
		return nil, fmt.Errorf("or can only apply on boolean")
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

func Negative(a *planpb.GenericValue) *ExprWithType {
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
		}
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
		}
	}
	return nil
}

func Not(a *planpb.GenericValue) *ExprWithType {
	if !IsBool(a) {
		return nil
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
	}
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

func Less(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() < b.GetFloatVal())
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() < float64(b.GetInt64Val()))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) < b.GetFloatVal())
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() < b.GetInt64Val())
		return ret
	}
	return nil
}

func LessEqual(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() <= b.GetFloatVal())
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() <= float64(b.GetInt64Val()))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) <= b.GetFloatVal())
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() <= b.GetInt64Val())
		return ret
	}
	return nil
}

func Greater(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() > b.GetFloatVal())
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() > float64(b.GetInt64Val()))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) > b.GetFloatVal())
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() > b.GetInt64Val())
		return ret
	}
	return nil
}

func GreaterEqual(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() >= b.GetFloatVal())
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(a.GetFloatVal() >= float64(b.GetInt64Val()))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(float64(a.GetInt64Val()) >= b.GetFloatVal())
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() >= b.GetInt64Val())
		return ret
	}
	return nil
}

func Equal(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() == b.GetStringVal())
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(a.GetFloatVal(), b.GetFloatVal()))
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(a.GetFloatVal(), float64(b.GetInt64Val())))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(floatingEqual(float64(a.GetInt64Val()), b.GetFloatVal()))
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() == b.GetInt64Val())
		return ret
	}
	return nil
}

func NotEqual(a, b *planpb.GenericValue) *ExprWithType {
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
		return ret
	}

	if IsString(a) && IsString(b) {
		ret.expr.GetValueExpr().Value = NewBool(a.GetStringVal() != b.GetStringVal())
		return ret
	}

	aFloat, bFloat, aInt, bInt := IsFloating(a), IsFloating(b), IsInteger(a), IsInteger(b)
	if aFloat && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(a.GetFloatVal(), b.GetFloatVal()))
		return ret
	} else if aFloat && bInt {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(a.GetFloatVal(), float64(b.GetInt64Val())))
		return ret
	} else if aInt && bFloat {
		ret.expr.GetValueExpr().Value = NewBool(!floatingEqual(float64(a.GetInt64Val()), b.GetFloatVal()))
		return ret
	} else if aInt && bInt {
		// aInt && bInt
		ret.expr.GetValueExpr().Value = NewBool(a.GetInt64Val() != b.GetInt64Val())
		return ret
	}
	return nil
}
