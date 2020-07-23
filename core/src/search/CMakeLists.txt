aux_source_directory(${MILVUS_ENGINE_SRC}/search search_files)

include_directories(${MILVUS_ENGINE_SRC}/search)
add_library(search STATIC ${search_files})
