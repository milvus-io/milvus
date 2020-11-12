# TODO
set(MILVUS_QUERY_SRCS
        deprecated/BinaryQuery.cpp
        generated/PlanNode.cpp
        generated/Expr.cpp
        visitors/ShowPlanNodeVisitor.cpp
        visitors/ExecPlanNodeVisitor.cpp
        Parser.cpp
        Plan.cpp
        )
add_library(milvus_query ${MILVUS_QUERY_SRCS})
target_link_libraries(milvus_query libprotobuf)
