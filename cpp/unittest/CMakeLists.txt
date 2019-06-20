#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
link_directories(
        "${CMAKE_BINARY_DIR}/lib"
        #"${VECWISE_THIRD_PARTY_BUILD}/lib"
#        "${GTEST_PREFIX}/lib/"

)
#message(STATUS "GTEST LIB: ${GTEST_PREFIX}/lib")

set(unittest_srcs
        ${CMAKE_CURRENT_SOURCE_DIR}/vecwise_test.cpp)
        #${EASYLOGGINGPP_INCLUDE_DIR}/easylogging++.cc)

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
add_subdirectory(faiss_wrapper)
add_subdirectory(license)
add_subdirectory(metrics)