milvus_add_pkg_config("rdkafka")

get_target_property(_RDKAFKA_INCLUDE_DIRS RdKafka::rdkafka INTERFACE_INCLUDE_DIRECTORIES)
list(GET _RDKAFKA_INCLUDE_DIRS 0 _RDKAFKA_INCLUDE_DIR)
install(DIRECTORY "${_RDKAFKA_INCLUDE_DIR}/librdkafka/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/librdkafka/")
