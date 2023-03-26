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

include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Boost REQUIRED)
find_package(arrow REQUIRED)
find_package(TBB REQUIRED)
find_package(fmt REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(opentelemetry-cpp REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(marisa REQUIRED)
find_package(AWSSDK REQUIRED)
find_package(folly REQUIRED)

milvus_add_pkg_config("milvus_segcore")

file(GLOB_RECURSE SRC_FILES
        ${MILVUS_ENGINE_SRC}/*.cpp
        ${MILVUS_ENGINE_SRC}/*.cc
        )
add_library(milvus_segcore SHARED ${SRC_FILES})
target_link_libraries(milvus_segcore
  Boost::boost
  TBB::tbb
  fmt::fmt
  protobuf::libprotobuf
  nlohmann_json::nlohmann_json
  opentelemetry-cpp::opentelemetry-cpp
  marisa::marisa
  arrow::libarrow
  arrow::libparquet
  aws-sdk-cpp::aws-sdk-cpp
  yaml-cpp
  Folly::folly
  knowhere
  simdjson
)
install(TARGETS milvus_segcore DESTINATION "${CMAKE_INSTALL_LIBDIR}")
