# Visitor Pattern
Visitor Pattern is used in segcore for parse and execute Execution Plan.

1. Inside `${core}/src/query/PlanNode.h`, contains physical plan for vector search
    1. `FloatVectorANNS` FloatVector search execution node
    2. `BinaryVectorANNS` BinaryVector search execution node
2. `${core}/src/query/Expr.h` contains physical plan for scalar expression
    1. `TermExpr` support operation like `col in [1, 2, 3]`
    2. `RangeExpr` support constant compare with data column like `a >= 5` `1 < b < 2`
    3. `CompareExpr` support compare with different columns, like `a < b`
    4. `LogicalBinaryExpr` support and/or
    5. `LogicalUnaryExpr` support not

Currently , under `${core/query/visitors}` directory, there are following visitors
1. `ShowPlanNodeVisitor` print PlanNode in json
2. `ShowExprVisitor` Expr -> json
3. `Verify...Visitor` validate ...
4. `ExtractInfo...Visitor` extract info from... ，including involved_fields and else
5. `ExecExprVisitor` Generate bitmask according to expression
6. `ExecPlanNodeVistor` physical plan executor, for now only support ANNS node.
