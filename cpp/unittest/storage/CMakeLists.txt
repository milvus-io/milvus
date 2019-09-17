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

aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 s3_client_src)

set(util_files
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp)

# Make sure that your call to link_directories takes place before your call to the relevant add_executable.
include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(s3_client_test_src
        ${unittest_srcs}
        ${s3_client_src}
        ${require_files}
        S3ClientTest.cpp
        ${MILVUS_ENGINE_SRC}/db/Status.cpp
        )

add_executable(s3_test
        ${s3_client_test_src}
        ${config_files}
        ${util_files}
        )

set(s3_client_libs
        stdc++
        aws-cpp-sdk-s3
        aws-cpp-sdk-core
        boost_filesystem_static
        )
target_link_libraries(s3_test
        ${s3_client_libs}
        ${unittest_libs}
        curl
        crypto)

install(TARGETS s3_test DESTINATION unittest)