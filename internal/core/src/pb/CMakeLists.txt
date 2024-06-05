# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

find_package(Protobuf REQUIRED)

file(GLOB_RECURSE milvus_proto_srcs
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(milvus_proto STATIC
    ${milvus_proto_srcs}
)
message(STATUS "milvus proto sources: " ${milvus_proto_srcs})

target_link_libraries( milvus_proto PUBLIC ${CONAN_LIBS} )
