string(TOLOWER "${CMAKE_INSTALL_PREFIX}" _PREFIX)
string(TOLOWER "${ITK_BINARY_DIR}" _BUILD)
if("${_PREFIX}" STREQUAL "${_BUILD}")
  message(FATAL_ERROR
    "The current CMAKE_INSTALL_PREFIX points at the build tree:\n"
    "  ${CMAKE_INSTALL_PREFIX}\n"
    "This is not supported."
    )
endif()
