cmake_minimum_required (VERSION 3.2)

option(SqliteOrm_SysSqlite "Use system version of sqlite library" OFF)

if(SqliteOrm_SysSqlite)
    message(FATAL_ERROR "WIP: please, disable the SqliteOrm_SysSqlite option.")
else()
    add_subdirectory(third_party/sqlite)
endif()

add_executable(unit_tests tests.cpp)

target_link_libraries(unit_tests PRIVATE sqlite_orm sqlite3)

add_test(NAME "All_in_one_unit_test"
    COMMAND unit_tests
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
