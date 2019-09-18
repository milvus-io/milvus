#-------------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#-------------------------------------------------------------------------------

include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(knowhere_src
        ${MILVUS_ENGINE_SRC}/wrapper/data_transfer.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/vec_impl.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/vec_index.cpp)

set(helper
        utils.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.h)

set(knowhere_libs
        knowhere
        cudart
        cublas
        )

add_executable(knowhere_test knowhere_test.cpp ${knowhere_src} ${helper})
target_link_libraries(knowhere_test ${knowhere_libs} ${unittest_libs})

install(TARGETS knowhere_test DESTINATION unittest)