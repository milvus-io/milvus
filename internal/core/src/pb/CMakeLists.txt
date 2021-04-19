get_property(PROTOC_EXCUTABLE GLOBAL PROPERTY PROTOC_EXCUTABLE )


set(proto_file_names common.proto etcd_meta.proto schema.proto message.proto service_msg.proto)

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

target_link_libraries(milvus_proto
	libprotobuf
        )

add_dependencies( milvus_proto protoc )
