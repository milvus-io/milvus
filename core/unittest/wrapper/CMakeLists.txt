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

set(test_files
        test_knowhere.cpp
        test_wrapper.cpp)
if (MILVUS_GPU_VERSION)
    set(test_files ${test_files}
            test_hybrid_index.cpp)
endif ()

set(wrapper_files
        ${MILVUS_ENGINE_SRC}/wrapper/DataTransfer.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/VecImpl.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/VecIndex.cpp
        )

set(storage_files
        ${MILVUS_ENGINE_SRC}/storage/file/FileIOReader.cpp
        ${MILVUS_ENGINE_SRC}/storage/file/FileIOWriter.cpp
        )

set(util_files
        utils.cpp
        ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.cc
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        )

add_executable(test_wrapper
        ${test_files}
        ${wrapper_files}
        ${storage_files}
        ${util_files}
        ${common_files})

target_link_libraries(test_wrapper
        knowhere
        ${unittest_libs})

install(TARGETS test_wrapper DESTINATION unittest)
