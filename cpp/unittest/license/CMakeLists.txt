#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_src)

include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda)
link_directories(/usr/local/cuda/lib64)
link_directories(/usr/lib/x86_64-linux-gnu)
link_directories(/usr/lib/nvidia-415)
link_directories(/usr/local/cuda/targets/x86_64-linux/lib/stubs/)
link_directories(/usr/local/cuda/lib64/stubs/)

set(license_src_files
        ${MILVUS_ENGINE_SRC}/license/LicenseLibrary.cpp
        ${MILVUS_ENGINE_SRC}/license/LicenseCheck.cpp
    )


set(db_test_src
    license_library_tests.cpp
    license_check_test.cpp
    ${license_src_files})

cuda_add_executable(license_test ${db_test_src})

set(db_libs
        nvidia-ml
        cudart
        cublas
        boost_system_static
        boost_filesystem_static
        lz4
        crypto
        boost_serialization_static
        )

target_link_libraries(license_test ${db_libs} ${unittest_libs})
