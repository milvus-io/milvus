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

if(NOT EXISTS "${MAP_FILE}" OR NOT EXISTS "${BINARY_FILE}" OR NOT NM)
    message(FATAL_ERROR "AWS runtime check requires a linker map, an unstripped binary and nm")
endif()

file(STRINGS "${MAP_FILE}" aws_archives
    REGEX "libaws-(cpp-sdk-[^ /()\t]+|crt-cpp|c-[^ /()\t]+|checksums)\\.a([ ()\t]|$)")
if(aws_archives)
    list(GET aws_archives 0 first_archive)
    message(FATAL_ERROR "Static AWS archive in ${BINARY_FILE}: ${first_archive}")
endif()

execute_process(COMMAND "${NM}" --defined-only --demangle "${BINARY_FILE}"
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE defined_symbols
    ERROR_VARIABLE nm_error)
if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR "Cannot inspect AWS runtime in ${BINARY_FILE}: ${nm_error}")
endif()
if(NOT defined_symbols)
    message(FATAL_ERROR "AWS runtime check requires an unstripped binary: ${BINARY_FILE}")
endif()

# These out-of-line functions belong to the shared SDK/CRT libraries. Finding
# even a local definition here means the target embeds another runtime copy.
set(runtime_symbols
    aws_common_library_init
    aws_json_module_init
    aws_json_value_new_from_string
    aws_json_value_get_from_object
    aws_mem_acquire
    aws_partitions_config_new_from_string
    aws_s3_library_init
    Aws::InitAPI
    Aws::Crt::ApiHandle::ApiHandle)
list(JOIN runtime_symbols "|" runtime_pattern)
string(REGEX MATCH
    "(^|\n)[^\n]*[ \t](${runtime_pattern})([ \t(]|\n|$)"
    embedded_runtime "${defined_symbols}")
if(embedded_runtime)
    message(FATAL_ERROR "Embedded AWS runtime in ${BINARY_FILE}: ${embedded_runtime}")
endif()
