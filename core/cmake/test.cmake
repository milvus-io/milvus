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

set(gtest_libraries
    gtest
    gmock
    gtest_main
    gmock_main
    )

macro(ADD_TEST)
    cmake_parse_arguments(UT "" "TARGET" "SOURCES;LIBS" ${ARGN})
    #    message("UT_TARGET = ${UT_TARGET}")
    #    message("UT_SOURCES = ${UT_SOURCES}")
    #    message("UT_LIBS = ${UT_LIBS}")
    #    message("ARGN = ${ARGN}")

    add_executable(${UT_TARGET} ${UT_SOURCES})
    target_link_libraries(${UT_TARGET} ${UT_LIBS})
endmacro()
