get_filename_component(_IMPORT_PREFIX "${CMAKE_CURRENT_LIST_DIR}/../3rdparty/googletest/" ABSOLUTE)

find_package(Threads QUIET)

add_library(gmock_main STATIC EXCLUDE_FROM_ALL
  ${_IMPORT_PREFIX}/googletest/src/gtest-all.cc
  ${_IMPORT_PREFIX}/googlemock/src/gmock-all.cc
  ${_IMPORT_PREFIX}/googlemock/src/gmock_main.cc
)

target_include_directories(gmock_main
  PUBLIC
    ${_IMPORT_PREFIX}/googletest/include
    ${_IMPORT_PREFIX}/googlemock/include
  PRIVATE
    ${_IMPORT_PREFIX}/googletest
    ${_IMPORT_PREFIX}/googlemock
)

target_link_libraries(gmock_main
  PRIVATE
    Threads::Threads
)
add_library(GTest::gmock_main ALIAS gmock_main)
