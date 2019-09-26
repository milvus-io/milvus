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

set(wrapper_files
        ${MILVUS_ENGINE_SRC}/wrapper/DataTransfer.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/VecImpl.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/VecIndex.cpp)

set(util_files
        utils.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        )

set(knowhere_libs
        knowhere
        cudart
        cublas
        )

add_executable(wrapper_test wrapper_test.cpp ${wrapper_files} ${util_files})
target_link_libraries(wrapper_test ${knowhere_libs} ${unittest_libs})

install(TARGETS wrapper_test DESTINATION unittest)