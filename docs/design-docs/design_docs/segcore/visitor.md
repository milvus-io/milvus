# Visitor Pattern
Visitor Pattern is used in segcore for parse and execute Execution Plan.

1. Inside `${internal/core}/src/query/PlanNode.h`, contains physical plan for vector search:
    1. `VectorPlanNode` vector search execution node
2. `${internal/core}/src/query/Expr.h` contains physical plan for scalar expression:
    1. `TermExpr` support operation like `col in [1, 2, 3]`
    2. `RangeExpr` support constant compare with data column like `a >= 5` `1 < b < 2`
    3. `CompareExpr` support compare with different columns, like `a < b`
    4. `LogicalBinaryExpr` support and/or
    5. `LogicalUnaryExpr` support not

Currently, under `${internal/core/src/query}` directory, there are the following visitors:
1. `ExecPlanNodeVistor` physical plan executor only supports ANNS node for now
