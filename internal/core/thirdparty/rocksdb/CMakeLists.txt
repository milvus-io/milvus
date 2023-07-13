milvus_add_pkg_config("rocksdb")

install(DIRECTORY "${CONAN_INCLUDE_DIRS_ROCKSDB}/rocksdb/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/rocksdb/")
