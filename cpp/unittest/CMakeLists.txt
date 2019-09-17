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

link_directories(
        "${CMAKE_BINARY_DIR}/lib"
)

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)

set(unittest_srcs
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

set(require_files
        ${MILVUS_ENGINE_SRC}/server/ServerConfig.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        )

set(unittest_libs
        yaml-cpp
        gtest
        gmock
        gtest_main
        gmock_main
        pthread
        metrics
        gfortran
        prometheus-cpp-pull
        prometheus-cpp-push
        prometheus-cpp-core
        dl
        z
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        cudart
        )

foreach(dir ${CORE_INCLUDE_DIRS})
    include_directories(${dir})
endforeach()

add_subdirectory(server)
add_subdirectory(db)
add_subdirectory(knowhere)
add_subdirectory(metrics)
add_subdirectory(scheduler)
#add_subdirectory(storage)