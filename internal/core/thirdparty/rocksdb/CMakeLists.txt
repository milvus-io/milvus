if (MSYS)
  configure_file(rocksdb-win.pc.in "${CMAKE_CURRENT_BINARY_DIR}/rocksdb.pc" @ONLY)
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/rocksdb.pc" DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
else()
  milvus_add_pkg_config("rocksdb")
endif()

find_package(RocksDB REQUIRED)

install(DIRECTORY "${RocksDB_INCLUDE_DIR}/rocksdb/" DESTINATION "${CMAKE_INSTALL_PREFIX}/include/rocksdb/")
