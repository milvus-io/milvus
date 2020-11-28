set(COMMON_SRC 
    Schema.cpp
)

add_library(milvus_common 
    ${COMMON_SRC}
)
target_link_libraries(milvus_common milvus_proto)
