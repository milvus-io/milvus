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
    "[" Constant+, "]"

Constant := 
    INTERGER
  | FLOAT_NUMBER 

CompareExpr := 
    IDENTIFIER CmpOp Constant
  | Constant CmpOp IDENTIFIER

CmpOp := 
    ">"
  | ">="
  | "<"
  | "<="
  | "=="
  | "!="

INTERGER := 整数
FLOAT_NUM := 浮点数
IDENTIFIER := 列名
```

注意，

1. NIL 态为空字符串, 代表无 Predicate 的情形
2. ”+,” 代表逗号分隔的，且至少有一个元素的重复元素

语法分析后，执行以下静态检查规则

1. 列名必须存在于 Schema 中，且不是向量类型
2. CompareExpr/TermExpr 要求左右类型匹配
3. 整型列必须对应整型数据
    1. 浮点列可以对应浮点数据或整型数据
    2. BinaryOp 中，“与操作符” 的优先级高于 “或操作符“

简单举例说明：

```python
A > 3 && A < 4 && (C > 5 || D < 6)
FloatCol in [1.0, 2, 3.0]
Int64Col in [1, 2, 3] or C != 6
```
