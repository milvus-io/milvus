aux_source_directory(${MILVUS_ENGINE_SRC}/query query_files)
add_library(query STATIC ${query_files})
target_include_directories(query PUBLIC ${MILVUS_ENGINE_SRC}/query)
