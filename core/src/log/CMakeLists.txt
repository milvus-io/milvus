set(log_files
    ${MILVUS_ENGINE_SRC}/log/Log.cpp
    ${MILVUS_ENGINE_SRC}/log/Log.h
    ${MILVUS_ENGINE_SRC}/log/LogMgr.cpp
    ${MILVUS_ENGINE_SRC}/log/LogMgr.h
    ${thirdparty_easyloggingpp_files}
    )

add_library(log STATIC ${log_files})
add_dependencies(log fiu)
