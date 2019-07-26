#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

if (MILVUS_ENABLE_THRIFT STREQUAL "ON")
    add_subdirectory(thriftsimple)
else()
    add_subdirectory(grpcsimple)
endif()