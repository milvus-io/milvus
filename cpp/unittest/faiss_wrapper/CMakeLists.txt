#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_src)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

set(wrapper_test_src
        ${unittest_srcs}
        ${wrapper_src}
        ${config_files}
        ${require_files}
        wrapper_test.cpp
        )

add_executable(wrapper_test ${wrapper_test_src})

set(wrapper_libs
        stdc++
        boost_system_static
        boost_filesystem_static
        faiss
        cudart
        cublas
        sqlite
        snappy
        bz2
        z
        zstd
        lz4
        )
if(${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
    set(wrapper_libs ${wrapper_libs} ${MKL_LIBS} ${MKL_LIBS})
else()
    set(wrapper_libs ${wrapper_libs}
            lapack
            openblas)
endif()

target_link_libraries(wrapper_test ${wrapper_libs} ${unittest_libs})

set(topk_test_src
        topk_test.cpp
        ${CMAKE_SOURCE_DIR}/src/wrapper/gpu/Topk.cu)

install(TARGETS wrapper_test DESTINATION bin)
