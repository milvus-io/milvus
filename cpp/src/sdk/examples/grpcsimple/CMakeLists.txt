#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

aux_source_directory(src src_files)

include_directories(src)
include_directories(../../include)

link_directories(${CMAKE_BINARY_DIR})

add_executable(sdk_simple
        main.cpp
        ${src_files}
        )

target_link_libraries(sdk_simple
        milvus_sdk
        pthread
        )

install(TARGETS sdk_simple DESTINATION bin)
