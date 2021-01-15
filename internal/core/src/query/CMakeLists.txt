# TODO
set(MILVUS_QUERY_SRCS
        deprecated/BinaryQuery.cpp
        generated/PlanNode.cpp
        generated/Expr.cpp
        visitors/ShowPlanNodeVisitor.cpp
        visitors/ShowExprVisitor.cpp
        visitors/ExecPlanNodeVisitor.cpp
        visitors/ExecExprVisitor.cpp
        visitors/VerifyPlanNodeVisitor.cpp
        visitors/VerifyExprVisitor.cpp
        Plan.cpp
        Search.cpp
        SearchOnSealed.cpp
        SearchOnIndex.cpp
        SearchBruteForce.cpp
        SubQueryResult.cpp
        )
add_library(milvus_query ${MILVUS_QUERY_SRCS})
target_link_libraries(milvus_query milvus_proto milvus_utils knowhere)
