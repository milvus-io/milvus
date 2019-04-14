add_subdirectory(SRC)
if(BUILD_TESTING)
  add_subdirectory(TESTING)
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/blas.pc.in ${CMAKE_CURRENT_BINARY_DIR}/blas.pc @ONLY)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/blas.pc
  DESTINATION ${PKG_CONFIG_DIR}
  )
