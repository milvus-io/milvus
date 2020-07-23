aux_source_directory(${MILVUS_ENGINE_SRC}/search search_files)
add_library(search STATIC ${search_files})
target_include_directories(search PUBLIC ${MILVUS_ENGINE_SRC}/search)
