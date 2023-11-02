milvus_add_pkg_config("rdkafka")

find_package(RdKafka REQUIRED)

install(DIRECTORY "${RdKafka_INCLUDE_DIRS}/librdkafka/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/librdkafka/")
