
set(INDEXBUILDER_FILES
        IndexWrapper.cpp
        index_c.cpp)
add_library(milvus_indexbuilder SHARED
        ${INDEXBUILDER_FILES}
        )

target_link_libraries(milvus_indexbuilder
        tbb milvus_utils pthread knowhere log milvus_proto
        dl backtrace
        milvus_common
        milvus_query
        )

