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

add_source_at_current_directory()

add_subdirectory(artifact)

# Add loon_ffi subdirectory to include loon FFI source files
add_subdirectory(loon_ffi)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/loon_ffi)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/plugin)

# Folly's x86 package enables SSE4.2 in its headers. UnboundedQueue instantiates
# F14's CRC32 intrinsics, so this translation unit must enable the same ISA.
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
    set_property(SOURCE AsyncLoadExecutor.cpp APPEND PROPERTY COMPILE_OPTIONS -msse4.2)
    # The target-wide PCH uses the default ISA and cannot be reused with SSE4.2.
    set_source_files_properties(AsyncLoadExecutor.cpp PROPERTIES SKIP_PRECOMPILE_HEADERS ON)
endif()

add_library(milvus_storage OBJECT ${SOURCE_FILES})
target_link_libraries(milvus_storage PUBLIC milvus_conan_deps)
