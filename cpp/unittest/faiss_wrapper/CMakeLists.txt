#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/wrapper wrapper_src)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

set(wrapper_test_src
        ${unittest_srcs}
        ${wrapper_src}
        wrapper_test.cpp)

add_executable(wrapper_test ${wrapper_test_src})

set(faiss_libs
        faiss
        cudart
        cublas
        )
target_link_libraries(wrapper_test ${unittest_libs} ${faiss_libs})
