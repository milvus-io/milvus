#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_srcs)
aux_source_directory(./ test_srcs)

include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

include_directories(/usr/include/mysql)

#add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)

set(scheduler_test_src
        ${unittest_srcs}
        ${scheduler_resource_srcs}
        ${scheduler_srcs}
        ${test_srcs}
        )

cuda_add_executable(scheduler_test ${scheduler_test_src})

set(scheduler_libs
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        mysqlpp
        )

target_link_libraries(scheduler_test ${scheduler_libs} ${unittest_libs})

install(TARGETS scheduler_test DESTINATION bin)

