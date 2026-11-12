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

if(NOT DEFINED WORK_DIR)
    message(FATAL_ERROR "WORK_DIR is required")
endif()
get_filename_component(check_module "${CMAKE_CURRENT_LIST_DIR}/../cmake/CompileChecks.cmake" ABSOLUTE)
file(MAKE_DIRECTORY "${WORK_DIR}/source")
file(WRITE "${WORK_DIR}/source/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.18)
project(compile_checks LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(MILVUS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    file(WRITE "${CMAKE_BINARY_DIR}/unsupported" "${CMAKE_SYSTEM_NAME}/${CMAKE_CXX_COMPILER_ID}")
    return()
endif()
file(REMOVE "${CMAKE_BINARY_DIR}/unsupported")
# Match the warning suppression used by the real Linux build.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error -Wno-all")
include("${CHECK_MODULE}")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/clean.cpp" "int value(long n) { return static_cast<int>(n); }\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/narrow.cpp" "int value(long n) { int result{n}; return result; }\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/plain.c" "int value(void) { return 0; }\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/pch.hxx" "#include <vector>\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/bad_pch.cpp" "int value() { return 0; }\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/src/CMakeLists.txt" [==[
add_library(valid_code OBJECT ../clean.cpp)
target_precompile_headers(valid_code PRIVATE ../pch.hxx)
add_library(narrow OBJECT ../narrow.cpp)
add_library(bad_pch OBJECT ../bad_pch.cpp)
target_precompile_headers(bad_pch PRIVATE ../pch.hxx)
set_source_files_properties(../bad_pch.cpp PROPERTIES COMPILE_OPTIONS -fno-exceptions)
add_library(plain_c OBJECT ../plain.c)
add_library(interface_only INTERFACE)
add_custom_target(custom_only)
add_subdirectory(pb)
]==])
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/src/pb/CMakeLists.txt"
    "add_library(generated_pb OBJECT ../../narrow.cpp)\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/CMakeLists.txt"
    "add_library(vendor OBJECT ../narrow.cpp)\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/unittest/nested/CMakeLists.txt"
    "add_library(test_code OBJECT ../../clean.cpp)\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/unittest/CMakeLists.txt" "add_subdirectory(nested)\n")
file(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/CMakeLists.txt"
    "add_library(bench_code OBJECT ../clean.cpp)\n")
add_subdirectory(thirdparty)
foreach(directory src unittest benchmark)
    add_subdirectory("${directory}")
    milvus_enable_cpp_compile_checks("${CMAKE_CURRENT_SOURCE_DIR}/${directory}")
endforeach()
foreach(target valid_code narrow bad_pch test_code bench_code)
    get_target_property(flags ${target} COMPILE_OPTIONS)
    foreach(warning narrowing invalid-pch)
        if(NOT "${flags}" MATCHES "-Werror=${warning}")
            message(FATAL_ERROR "Missing ${warning} check on ${target}")
        endif()
    endforeach()
endforeach()
foreach(target generated_pb vendor)
    get_target_property(flags ${target} COMPILE_OPTIONS)
    if("${flags}" MATCHES "-Werror=")
        message(FATAL_ERROR "Checks leaked to ${target}")
    endif()
endforeach()
]=])

execute_process(COMMAND "${CMAKE_COMMAND}" -S "${WORK_DIR}/source" -B "${WORK_DIR}/build"
        "-DCHECK_MODULE=${check_module}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result STREQUAL "0")
    message(FATAL_ERROR "Cannot configure compile checks: ${output}${error}")
endif()
if(EXISTS "${WORK_DIR}/build/unsupported")
    file(READ "${WORK_DIR}/build/unsupported" platform)
    message(STATUS "Compile check regression requires Linux/GNU; skipping on ${platform}")
    return()
endif()

function(check_build target expected_error)
    execute_process(COMMAND "${CMAKE_COMMAND}" --build "${WORK_DIR}/build" --target "${target}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(expected_error STREQUAL "")
        if(NOT result STREQUAL "0")
            message(FATAL_ERROR "${target} unexpectedly failed: ${output}${error}")
        endif()
    elseif(result STREQUAL "0" OR NOT "${output}${error}" MATCHES "${expected_error}")
        message(FATAL_ERROR "${target} did not fail with ${expected_error}: ${output}${error}")
    endif()
    message(STATUS "Compile check regression passed: ${target}")
endfunction()

foreach(target valid_code plain_c test_code bench_code generated_pb vendor)
    check_build("${target}" "")
endforeach()
check_build(narrow "-Werror=narrowing")
check_build(bad_pch "-Werror=invalid-pch")
file(READ "${WORK_DIR}/build/compile_commands.json" commands)
string(JSON count LENGTH "${commands}")
math(EXPR last "${count} - 1")
foreach(index RANGE ${last})
    string(JSON source GET "${commands}" ${index} file)
    string(JSON command GET "${commands}" ${index} command)
    if(source MATCHES "plain\\.c$" AND command MATCHES "-Werror=")
        message(FATAL_ERROR "C++ checks leaked to C compilation: ${command}")
    endif()
endforeach()
