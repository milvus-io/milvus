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
set( VALUE_SRCS     config/ConfigInit.cpp
                    config/ConfigMgr.h
                    config/ConfigMgr.cpp
                    config/ServerConfig.h
                    config/ServerConfig.cpp
                    status/StatusInit.cpp
                    status/StatusMgr.h
                    status/StatusMgr.cpp
                    status/ServerStatus.h
                    status/ServerStatus.cpp
                    Value.h
                    ValueMgr.h
                    ValueMgr.cpp
                    ValueType.h
                    ValueType.cpp
                    )

set( VALUE_LIBS     yaml-cpp
                    fiu
                    )

create_library(
        TARGET      value
        SRCS        ${VALUE_SRCS}
        LIBS        ${VALUE_LIBS}
)

if ( BUILD_UNIT_TEST )
    create_library(
            TARGET      value-fiu
            SRCS        ${VALUE_SRCS}
            LIBS        ${VALUE_LIBS}
            DEFS        FIU_ENABLE
    )
    target_compile_definitions(value-fiu PRIVATE FIU_ENABLE)

    set(GTEST_LIBS gtest gtest_main gmock gmock_main)

    create_executable(
            TARGET      ConfigMgrTest
            SRCS        config/ConfigMgrTest
            LIBS        value-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ConfigMgrTest
            COMMAND  $<TARGET_FILE:ConfigMgrTest>
            )

    create_executable(
            TARGET      ServerConfigTest
            SRCS        config/ServerConfigTest
            LIBS        value-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ServerConfigTest
            COMMAND  $<TARGET_FILE:ServerConfigTest>
            )

    create_executable(
            TARGET      ValueTypeTest
            SRCS        ValueTypeTest1 ValueTypeTest2
            LIBS        value-fiu ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     ValueTypeTest
               COMMAND  $<TARGET_FILE:ValueTypeTest>
               )
endif()
