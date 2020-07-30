set(LOG_FILES   ${MILVUS_ENGINE_SRC}/log/Log.cpp
                ${MILVUS_ENGINE_SRC}/log/Log.h
                ${MILVUS_ENGINE_SRC}/log/LogMgr.cpp
                ${MILVUS_ENGINE_SRC}/log/LogMgr.h
                ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.cc
                ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.h
                )

add_library(log STATIC ${LOG_FILES})
add_dependencies(log fiu)
