aux_source_directory(${MILVUS_ENGINE_SRC}/query query_files)
include_directories(${MILVUS_ENGINE_SRC}/query)

add_library(query STATIC ${query_files})
