# Segcore search Deisgn
init: 7.23.2021, by [FluorineDog](https://github.com/FluorineDog)

update: 9.16.2021, by [xiaofan-luan](https://github.com/xiaofan-luan)

## Search
Search now support two mode, json DSL mode, and Boolean Expr mode. We will talk about the later one in detail because the former has been deprecated and only use in test.

The execution mode of Boolean Expr works as follow:
1. client packs search expr, topk and query vector into proto and sends to proxy node.
2. proxynode unmarshalls the proto, parses it to logical plan, makes static check, and generates protobuf IR.
3. querynode unmarshalls the plan, generates an executable plan AST, and query in the segcore.

See deatailed expression usage at [expr_grammar.md](https://milvus.io/docs/v2.0.0/expression.md)

## Segcore Search process
After obtaining the AST, excetion engine use the visitor mode to explain and execute the whole AST tree:

1. Each Node includes two steps, a must vector search and an optional predicate.
    1. If Predicate exist, execute predicate expression stage to generate bitset as the vector search bitmask.
    2. If Predicate not exist, vector search bitmask will be empty . 
    3. Bitmask will be used as mark filtered out/deleted entities in the vector execution engine.
       
2. Currently, Milvus support following node on the AST, visitor mode is used to interpret and execute from top to bottom and generate the final bitmask.
    1. LogicalUnaryExpr: not expression 
    2. LogicalBinaryExpr: and or expression
    3. TermExpr: in expression `A in [1, 2, 3]`
    4. CompareExpr: compare expression `A > 1` `B <= 1`
    
3. TermExpr and CompareExpr is the leaf node of execution