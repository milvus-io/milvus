milvus_add_pkg_config("rdkafka")

install(DIRECTORY "${CONAN_INCLUDE_DIRS_LIBRDKAFKA}/librdkafka/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/librdkafka/")
