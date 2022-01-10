# Segcore Search Design

init: 7.23.2021, by [FluorineDog](https://github.com/FluorineDog)

update: 9.16.2021, by [xiaofan-luan](https://github.com/xiaofan-luan)

## Search

Search now supports two modes: json DSL mode and Boolean Expr mode. We will talk about the latter one in detail because the former has been deprecated and is only used in tests.

The execution mode of Boolean Expr works as follows:

1. client packs search expr, topk, and query vector into proto and sends to Proxy node.
2. Proxy Node unmarshals the proto, parses it to logical plan, makes a static check, and generates protobuf IR.
3. Query Node unmarshals the plan, generates an executable plan AST, and queries in the segcore.

See details of expression usage at [expr_grammar.md](https://milvus.io/docs/v2.0.0/expression.md)

## Segcore Search Process

After obtaining the AST, the execution engine uses the visitor mode to explain and executes the whole AST tree:

1. Each node includes two steps, a mandatory vector search and an optional predicate.

   1. If Predicate exists, execute predicate expression stage to generate bitset as the vector search bitmask.
   2. If Predicate does not exist, the vector search bitmask will be empty.
   3. Bitmask will be used to mark filtered out / deleted entities in the vector execution engine.

2. Currently, Milvus supports the following node on the AST, visitor mode is used to interpret and execute from top to bottom and generate the final bitmask.

   1. LogicalUnaryExpr: not expression
   2. LogicalBinaryExpr: and or expression
   3. TermExpr: in expression `A in [1, 2, 3]`
   4. CompareExpr: compare expression `A > 1` `B <= 1`

3. TermExpr and CompareExpr are leaf nodes of execution.
