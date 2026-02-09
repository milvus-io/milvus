milvus_add_pkg_config("rdkafka")

get_target_property(_RDKAFKA_INCLUDE_DIRS RdKafka::rdkafka INTERFACE_INCLUDE_DIRECTORIES)
install(DIRECTORY "${_RDKAFKA_INCLUDE_DIRS}/librdkafka/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/librdkafka/")
