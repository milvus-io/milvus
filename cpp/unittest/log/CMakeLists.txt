#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------
set(log_test_src ${unittest_srcs} log_test.cpp)

add_executable(log_test ${log_test_src})

target_link_libraries(log_test ${unittest_libs})
