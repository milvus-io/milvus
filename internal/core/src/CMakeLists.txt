# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(MILVUS_GPU_VERSION STREQUAL "ON")
    project(core CXX C CUDA)
else()
    project(core CXX C)
endif()

include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})

set(FOUND_OPENBLAS "unknown")
add_subdirectory( pb )
add_subdirectory( exceptions )
add_subdirectory( utils )
add_subdirectory( log )
add_subdirectory( config )
add_subdirectory( common )
add_subdirectory( storage )
add_subdirectory( index )
add_subdirectory( query )
add_subdirectory( segcore )
add_subdirectory( indexbuilder )
if(USE_DYNAMIC_SIMD)
    add_subdirectory( simd )
endif()
