milvus_add_pkg_config("rocksdb")

get_target_property(_ROCKSDB_INCLUDE_DIRS RocksDB::rocksdb-shared INTERFACE_INCLUDE_DIRECTORIES)
list(GET _ROCKSDB_INCLUDE_DIRS 0 _ROCKSDB_INCLUDE_DIR)
install(DIRECTORY "${_ROCKSDB_INCLUDE_DIR}/rocksdb/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/rocksdb/")
