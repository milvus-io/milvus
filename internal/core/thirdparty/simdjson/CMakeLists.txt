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

FetchContent_Declare(
    simdjson
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v5.0.2.tar.gz
    URL_HASH SHA256=09de4289a26d3a0453569901a3837450b5b98a5c317d635eeee8e2a82b1ebe6b
)
FetchContent_MakeAvailable(simdjson)

# v5.0.2 leaves these header functions with external linkage when
# SIMDJSON_CONSTEXPR_STRING is empty. Keep their definitions ODR-safe.
set(_key_selector "${simdjson_SOURCE_DIR}/include/simdjson/generic/ondemand/key_selector.h")
file(READ "${_key_selector}" _key_selector_original)
set(_key_selector_fixed "${_key_selector_original}")
foreach(_helper append_uint append_byte)
    set(_declaration "SIMDJSON_CONSTEXPR_STRING void ${_helper}(")
    string(FIND "${_key_selector_fixed}" "inline ${_declaration}" _inline_position)
    if(_inline_position EQUAL -1)
        string(REPLACE "${_declaration}" "inline ${_declaration}"
            _key_selector_fixed "${_key_selector_fixed}")
    endif()
endforeach()
if(NOT _key_selector_fixed STREQUAL _key_selector_original)
    file(WRITE "${_key_selector}" "${_key_selector_fixed}")
endif()

set( SIMDJSON_INCLUDE_DIR ${simdjson_SOURCE_DIR}/include CACHE INTERNAL "Path to simdjson include directory" )
