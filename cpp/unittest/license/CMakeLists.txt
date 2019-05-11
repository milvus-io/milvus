#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
include_directories(../../src)

aux_source_directory(../../src/db db_srcs)
aux_source_directory(../../src/config config_files)
aux_source_directory(../../src/cache cache_srcs)
aux_source_directory(../../src/wrapper wrapper_src)

include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda)
link_directories(/usr/local/cuda/lib64)
link_directories(/usr/lib/x86_64-linux-gnu)
link_directories(/usr/lib/nvidia-415)

set(require_files
        ../../src/license/License.cpp
        ../../src/license/LicensePublic.cpp
        ../../src/license/LicenseCreateuuidshafile.cpp
    )

set(db_test_src
    ${require_files}
    license_tests.cpp)

cuda_add_executable(license_test ${db_test_src})

set(db_libs
        nvidia-ml
        cudart
        cublas
        boost_system
        boost_filesystem
        lz4
        crypto
        boost_serialization
        )

target_link_libraries(license_test ${unittest_libs} ${db_libs})
