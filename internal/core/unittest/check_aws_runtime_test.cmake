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

file(MAKE_DIRECTORY "${WORK_DIR}")
set(binary_file "${WORK_DIR}/clean")
set(map_file "${WORK_DIR}/link.map")
file(WRITE "${WORK_DIR}/clean.cpp" "int main() { return 0; }\n")
execute_process(COMMAND "${CXX}" "${WORK_DIR}/clean.cpp" -o "${binary_file}"
    RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Cannot build AWS runtime check fixture")
endif()

function(check_runtime expected_result expected_error)
    execute_process(COMMAND "${CMAKE_COMMAND}"
            "-DMAP_FILE=${map_file}" "-DBINARY_FILE=${binary_file}" "-DNM=${NM}"
            -P "${CHECK_SCRIPT}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(expected_result STREQUAL "pass")
        if(NOT result EQUAL 0)
            message(FATAL_ERROR "Valid runtime rejected: ${output}${error}")
        endif()
    elseif(result EQUAL 0 OR NOT "${error}" MATCHES "${expected_error}")
        message(FATAL_ERROR "Invalid runtime not diagnosed: ${output}${error}")
    endif()
endfunction()

# Arrow may remain static; AWS shared libraries must not match the archive check.
file(WRITE "${map_file}" "LOAD /deps/libarrow.a\nLOAD /deps/libaws-c-common.so.1\n")
check_runtime(pass "")

# Include both unused LOAD entries and extracted archive-member entries.
foreach(archive aws-cpp-sdk-core aws-crt-cpp aws-c-common aws-checksums)
    file(WRITE "${map_file}" "LOAD /deps/lib${archive}.a\n")
    check_runtime(fail "Static AWS archive")
    file(WRITE "${map_file}" "/deps/lib${archive}.a(allocator.c.o)\n")
    check_runtime(fail "Static AWS archive")
endforeach()

# nm -D would miss this private CRT copy. The full symbol table must reject it
# even when the linker map no longer contains the original AWS archive path.
set(binary_file "${WORK_DIR}/embedded.so")
file(WRITE "${WORK_DIR}/embedded.cpp"
    "extern \"C\" __attribute__((visibility(\"hidden\"))) void aws_json_value_get_from_object() {}\n")
execute_process(COMMAND "${CXX}" -shared -fPIC "${WORK_DIR}/embedded.cpp" -o "${binary_file}"
    RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Cannot build hidden AWS runtime check fixture")
endif()
file(WRITE "${map_file}" "LOAD /deps/prelinked.o\n")
check_runtime(fail "Embedded AWS runtime")

execute_process(COMMAND "${STRIP}" --strip-all "${binary_file}"
    RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Cannot strip AWS runtime check fixture")
endif()
check_runtime(fail "unstripped binary")

message(STATUS "AWS runtime link checks passed")
