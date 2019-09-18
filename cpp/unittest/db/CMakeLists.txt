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

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/knowhere knowhere_src)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/action scheduler_action_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/event scheduler_event_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/job scheduler_job_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_srcs)

aux_source_directory(./ test_srcs)

set(util_files
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc)


include_directories(/usr/local/cuda/include)
link_directories("/usr/local/cuda/lib64")

include_directories(/usr/include/mysql)

set(db_test_src
    ${config_files}
    ${cache_srcs}
    ${db_main_files}
    ${db_engine_files}
    ${db_insert_files}
    ${db_meta_files}
    ${db_scheduler_srcs}
    ${wrapper_src}
    ${scheduler_action_srcs}
    ${scheduler_event_srcs}
    ${scheduler_job_srcs}
    ${scheduler_resource_srcs}
    ${scheduler_task_srcs}
    ${scheduler_srcs}
    ${knowhere_src}
    ${util_files}
    ${require_files}
    ${test_srcs}
    )

cuda_add_executable(db_test ${db_test_src})

set(db_libs
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        mysqlpp
        )

set(knowhere_libs
        knowhere
        cudart
        cublas
        )

target_link_libraries(db_test ${db_libs} ${knowhere_libs} ${unittest_libs})

install(TARGETS db_test DESTINATION unittest)

