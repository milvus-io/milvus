#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

set(vecwise_engine_src
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        )

add_library(vecwise_engine SHARED ${vecwise_engine_src})