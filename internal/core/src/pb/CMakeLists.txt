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

get_property(PROTOC_EXCUTABLE GLOBAL PROPERTY PROTOC_EXCUTABLE )


set(proto_file_names common.proto etcd_meta.proto schema.proto milvus.proto index_cgo_msg.proto plan.proto segcore.proto)

set( PROTO_PATH          "${MILVUS_SOURCE_DIR}/../proto/" )

FOREACH(proto_file ${proto_file_names})
    STRING(REGEX REPLACE "[^/]proto" "" proto_src_file_name ${proto_file})
    LIST(APPEND MILVUS_PROTO_SRCS "${proto_src_file_name}.pb.cc")

    ADD_CUSTOM_COMMAND(
	OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/${proto_src_file_name}.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/${proto_src_file_name}.pb.cc"
	COMMAND ${PROTOC_EXCUTABLE} --proto_path=${PROTO_PATH}/
	--cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/ ${proto_file}
	DEPENDS ${PROTO_PATH}/${proto_file}
    )

ENDFOREACH(proto_file)

add_library(milvus_proto STATIC
	${MILVUS_PROTO_SRCS}
        )
message(${MILVUS_PROTO_SRCS})

target_link_libraries(milvus_proto
	libprotobuf
        )

add_dependencies( milvus_proto protoc )
