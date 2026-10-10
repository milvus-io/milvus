# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(milvus_check_aws_runtime target)
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        return()
    endif()

    # Linker maps include archives from transitive targets and response files;
    # GNU ld also lists archives from which no objects were extracted. Check
    # before stripping: hidden CRT state is absent from the dynamic symbol table.
    set(map_file "${CMAKE_CURRENT_BINARY_DIR}/${target}-$<CONFIG>.aws.map")
    set(check_script "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/CheckAwsRuntime.cmake")
    target_link_options(${target} PRIVATE "LINKER:-Map,${map_file}")
    set_property(TARGET ${target} APPEND PROPERTY LINK_DEPENDS "${check_script}")
    set(check_command ${CMAKE_COMMAND}
            "-DMAP_FILE=${map_file}"
            "-DBINARY_FILE=$<TARGET_FILE:${target}>"
            "-DNM=${CMAKE_NM}"
            -P "${check_script}")
    get_target_property(target_source_dir ${target} SOURCE_DIR)
    if(target_source_dir STREQUAL CMAKE_CURRENT_SOURCE_DIR)
        add_custom_command(TARGET ${target} POST_BUILD
            COMMAND ${check_command}
            COMMENT "Checking ${target} uses a shared AWS runtime"
            VERBATIM)
    else()
        # A FetchContent target is defined in another directory, where CMake
        # does not allow us to attach a POST_BUILD command. Consumers depend
        # on this check so it also runs when building all_tests directly.
        add_custom_target(${target}_aws_runtime_check ALL
            COMMAND ${check_command}
            DEPENDS ${target}
            COMMENT "Checking ${target} uses a shared AWS runtime"
            VERBATIM)
    endif()
endfunction()
