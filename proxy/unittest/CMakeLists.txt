#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------


include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)


set( ENTRY_FILE ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp )

set( UNITTEST_LIBS  libboost_system.a
                    libboost_filesystem.a
                    yaml-cpp
                    gtest
                    gtest_main
                    libstdc++fs.a
                    pthread
                    opentracing::opentracing
                    opentracing::mocktracer
                    z
                    )

add_subdirectory(message_client)
add_subdirectory(config)
