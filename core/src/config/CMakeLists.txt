#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

# library
set( CONFIG_SRCS    ConfigMgr.h
                    ConfigMgr.cpp
                    ConfigType.h
                    ConfigType.cpp
                    ServerConfig.h
                    ServerConfig.cpp
                    )

add_library( config ${CONFIG_SRCS} )
add_dependencies( config yaml-cpp )


if ( BUILD_UNIT_TEST )
    add_executable( ConfigTypeTest )
    target_sources( ConfigTypeTest PRIVATE ConfigTypeTest1
                                           ConfigTypeTest2 )
    target_link_libraries( ConfigTypeTest
                PRIVATE    config
                           gtest
                           gtest_main
                           gmock
                           gmock_main
                           yaml-cpp )

    add_test ( NAME     ConfigTypeTest
               COMMAND  $<TARGET_FILE:ConfigTypeTest>
               )
endif()
