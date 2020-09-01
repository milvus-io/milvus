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
set( CONFIG_SRCS    ConfigInit.cpp
                    ConfigMgr.h
                    ConfigMgr.cpp
                    ConfigType.h
                    ConfigType.cpp
                    ServerConfig.h
                    ServerConfig.cpp
                    )

set( CONFIG_LIBS    yaml-cpp
                    fiu
                    )

create_library(
        TARGET      config
        SRCS        ${CONFIG_SRCS}
        LIBS        ${CONFIG_LIBS}
)

if ( BUILD_UNIT_TEST )
    create_library(
            TARGET      config-fiu
            SRCS        ${CONFIG_SRCS}
            LIBS        ${CONFIG_LIBS}
            DEFS        FIU_ENABLE
    )
    target_compile_definitions(config-fiu PRIVATE FIU_ENABLE)

    set(GTEST_LIBS gtest gtest_main gmock gmock_main)

    create_executable(
            TARGET      ConfigMgrTest
            SRCS        ConfigMgrTest
            LIBS        config-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ConfigMgrTest
            COMMAND  $<TARGET_FILE:ConfigMgrTest>
            )

    create_executable(
            TARGET      ServerConfigTest
            SRCS        ServerConfigTest
            LIBS        config-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ServerConfigTest
            COMMAND  $<TARGET_FILE:ServerConfigTest>
            )

    create_executable(
            TARGET      ConfigTypeTest
            SRCS        ConfigTypeTest1 ConfigTypeTest2
            LIBS        config-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ConfigTypeTest
               COMMAND  $<TARGET_FILE:ConfigTypeTest>
               )
endif()
