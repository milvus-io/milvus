milvus_add_pkg_config("rocksdb")

get_target_property(_ROCKSDB_INCLUDE_DIRS RocksDB::rocksdb-shared INTERFACE_INCLUDE_DIRECTORIES)
install(DIRECTORY "${_ROCKSDB_INCLUDE_DIRS}/rocksdb/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/rocksdb/")
