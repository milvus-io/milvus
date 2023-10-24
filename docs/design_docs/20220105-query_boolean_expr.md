```haskell
Expr :=
    LogicalExpr | NIL

LogicalExpr :=
    LogicalExpr BinaryLogicalOp LogicalExpr
  | UnaryLogicalOp LogicalExpr
  | "(" LogicalExpr ")"
  | SingleExpr

BinaryLogicalOp :=
    "&&" | "and"
  | "||" | "or"

UnaryLogicalOp :=
    "not"

SingleExpr :=
    TermExpr
  | CompareExpr

TermExpr :=
    IDENTIFIER "in" ConstantArray

ConstantArray :=
    "[" ConstantExpr { "," ConstantExpr } "]"

ConstantExpr :=
    Constant
  | ConstantExpr BinaryArithOp ConstantExpr
  | UnaryArithOp ConstantExpr

Constant :=
    INTEGER
  | FLOAT_NUMBER

UnaryArithOp :=
    "+"
  | "-"

BinaryArithOp :=
    "+"
  | "-"
  | "*"
  | "/"
  | "%"
  | "**"

CompareExpr :=
    IDENTIFIER CmpOp IDENTIFIER
  | IDENTIFIER CmpOp ConstantExpr
  | ConstantExpr CmpOp IDENTIFIER
  | ConstantExpr CmpOpRestricted IDENTIFIER CmpOpRestricted ConstantExpr

CmpOpRestricted :=
    "<"
  | "<="

CmpOp :=
    ">"
  | ">="
  | "<"
  | "<="
  | "=="
  | "!="

INTEGER := 整数
FLOAT_NUM := 浮点数
IDENTIFIER := 列名
```

Tips:

1. NIL represents an empty string, which means there is no Predicate for Expr.
2. Gramma is described by EBNF syntax, expressions that may be omitted or repeated are represented through curly braces `{...}`.

After syntax analysis, the following rules will be applied:

1. Non-vector column must exist in Schema.
2. CompareExpr/TermExpr requires operand type matching.
3. CompareExpr between non-vector columns of different types is available.
4. The modulo operation requires all operands to be integers.
5. Integer columns can only match integer operands. While float columns can match both integer and float operands.
6. In BinaryOp, the `and`/`&&` operator has a higher priority than the `or`/`||` operator.

Example：

```python
A > 3 && A < 4 && (C > 5 || D < 6)
1 < A <= 2.0 + 3 - 4 * 5 / 6 % 7 ** 8
A == B
FloatCol in [1.0, 2, 3.0]
Int64Col in [1, 2, 3] or C != 6
```
