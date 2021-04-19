set(exceptions_files 
    EasyAssert.cpp
)

add_library(milvus_exceptions ${exceptions_files})
target_link_libraries(milvus_exceptions milvus_proto)
