milvus_add_pkg_config("rocksdb")

find_package(RocksDB REQUIRED)

install(DIRECTORY "${RocksDB_INCLUDE_DIRS}/rocksdb/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/rocksdb/")
