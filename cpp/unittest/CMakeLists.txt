#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
link_directories(
        "${CMAKE_BINARY_DIR}/lib"
)

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)

set(unittest_srcs
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
        #${EASYLOGGINGPP_INCLUDE_DIR}/easylogging++.cc)

set(require_files
        ${MILVUS_ENGINE_SRC}/server/ServerConfig.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        )

set(unittest_libs
        yaml-cpp
        gtest
        gmock
        gtest_main
        gmock_main
        easyloggingpp
        pthread
        metrics
        openblas
        gfortran
        prometheus-cpp-pull
        prometheus-cpp-push
        prometheus-cpp-core
        dl
        z
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        )

add_subdirectory(server)
add_subdirectory(db)
#add_subdirectory(faiss_wrapper)
add_subdirectory(index_wrapper)
add_subdirectory(license)
add_subdirectory(faiss_wrapper)
#add_subdirectory(license)
add_subdirectory(metrics)
add_subdirectory(storage)