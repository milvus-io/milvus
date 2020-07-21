aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config/handler config_handler_files)

add_library(config 
    ${config_files}
    ${config_handler_files})
