# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

add_source_at_current_directory_recursively()
add_library(milvus_monitor OBJECT ${SOURCE_FILES})

# jemalloc_stats_c.cpp needs jemalloc headers on Linux, ensure jemalloc is built first
if (LINUX)
    add_dependencies(milvus_monitor jemalloc)
    target_include_directories(milvus_monitor PRIVATE ${CMAKE_INSTALL_PREFIX}/include)
endif()
