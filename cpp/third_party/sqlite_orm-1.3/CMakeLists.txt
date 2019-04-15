cmake_minimum_required (VERSION 3.2)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(sqlite_orm)

option(SqliteOrm_BuildTests "Build sqlite_orm unit tests" OFF)

set(SqliteOrm_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/include")
add_library(sqlite_orm INTERFACE)
target_include_directories(sqlite_orm INTERFACE include)

# Tests
include(CTest)
if(SqliteOrm_BuildTests AND BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

#add_subdirectory(examples)

set(SQLITE_ORM_INCLUDE_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/include/sqlite_orm" CACHE PATH "The directory the headers are installed in")
install(FILES include/sqlite_orm/sqlite_orm.h DESTINATION ${SQLITE_ORM_INCLUDE_INSTALL_DIR})