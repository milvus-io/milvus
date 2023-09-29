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

find_package(OpenMP REQUIRED)

set(FURTHEST_NEIGHBOR_SEARCH_FILES
    kmeans.cpp 
    kgraph.cpp 
)

add_library(milvus_fnsearcher SHARED ${FURTHEST_NEIGHBOR_SEARCH_FILES})

if(USE_DYNAMIC_SIMD)
        target_link_libraries(milvus_fnsearcher milvus_index milvus_simd)
else() 
        target_link_libraries(milvus_fnsearcher milvus_index)
endif()

install(TARGETS milvus_fnsearcher DESTINATION "${CMAKE_INSTALL_LIBDIR}")
