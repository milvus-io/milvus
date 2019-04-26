cmake_minimum_required (VERSION 3.2)

set(SQLITE3_ARCH_NAME "${CMAKE_CURRENT_BINARY_DIR}/sqlite.zip")
set(SQLITE3_LINK "https://www.sqlite.org/2017/sqlite-amalgamation-3210000.zip")
set(SQLITE3_ARCH_SHA1 "ebe33c20d37a715db95288010c1009cd560f2452")

add_custom_command(
    OUTPUT ${SQLITE3_ARCH_NAME}
    COMMAND
    ${CMAKE_COMMAND} -DSQLITE3_ARCH_NAME=${SQLITE3_ARCH_NAME} -DSQLITE3_LINK=${SQLITE3_LINK} -DSQLITE3_ARCH_SHA1=${SQLITE3_ARCH_SHA1} -P "${CMAKE_CURRENT_SOURCE_DIR}/DownloadSqlite3.cmake")

add_custom_command(
    OUTPUT sqlite-amalgamation-3210000 sqlite-amalgamation-3210000/sqlite3.c sqlite-amalgamation-3210000/sqlite3.h
    DEPENDS ${SQLITE3_ARCH_NAME}
    COMMAND
    ${CMAKE_COMMAND} -E tar xfz ${SQLITE3_ARCH_NAME})

add_library(sqlite3 sqlite-amalgamation-3210000/sqlite3.c)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_include_directories(sqlite3 INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/sqlite-amalgamation-3210000")
target_link_libraries(sqlite3 PRIVATE ${CMAKE_DL_LIBS} Threads::Threads)
