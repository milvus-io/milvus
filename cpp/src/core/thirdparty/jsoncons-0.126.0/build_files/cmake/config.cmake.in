# jsoncons cmake module
# This module sets the following variables in your project::
#
#   jsoncons_FOUND - true if jsoncons found on the system
#   jsoncons_INCLUDE_DIRS - the directory containing jsoncons headers
#   jsoncons_LIBRARY - empty

@PACKAGE_INIT@

if(NOT TARGET @PROJECT_NAME@)
  include("${CMAKE_CURRENT_LIST_DIR}/@PROJECT_NAME@Targets.cmake")
  get_target_property(@PROJECT_NAME@_INCLUDE_DIRS jsoncons INTERFACE_INCLUDE_DIRECTORIES)
endif()
