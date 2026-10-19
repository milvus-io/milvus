# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(milvus_grep OBJECT Grep.cpp SegmentGrep.cpp)
target_compile_features(milvus_grep PUBLIC cxx_std_17)
set_target_properties(milvus_grep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(milvus_grep PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")
target_link_libraries(milvus_grep PUBLIC tantivy_binding milvus_conan_deps)
# The imported binding provides the installed generated header. Explicitly
# order object compilation after installation, including clean parallel builds.
if(TARGET install_tantivy_target)
    add_dependencies(milvus_grep install_tantivy_target)
endif()
