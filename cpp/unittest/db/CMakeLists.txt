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

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} test_files)

include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(db_test_files
        ${cache_files}
        ${config_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_scheduler_files}
        ${metrics_files}
        ${scheduler_files}
        ${knowhere_files}
        ${helper_files}
        ${test_files}
    )

cuda_add_executable(db_test ${db_test_files})

set(db_libs
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        mysqlpp
        )

target_link_libraries(db_test ${db_libs} knowhere ${unittest_libs})

install(TARGETS db_test DESTINATION unittest)

