# TODO
set(MILVUS_QUERY_SRCS
        BinaryQuery.cpp
        Parser.cpp
        )
add_library(milvus_query ${MILVUS_QUERY_SRCS})
target_link_libraries(milvus_query libprotobuf)
