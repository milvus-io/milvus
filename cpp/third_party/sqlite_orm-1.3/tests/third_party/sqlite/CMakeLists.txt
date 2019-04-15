cmake_minimum_required (VERSION 3.2)

set(SQLITE3_ARCH_NAME "${CMAKE_CURRENT_BINARY_DIR}/sqlite.zip")
set(SQLITE3_LINK "https://www.sqlite.org/2017/sqlite-amalgamation-3210000.zip")
set(SQLITE3_ARCH_SHA1 "ebe33c20d37a715db95288010c1009cd560f2452")

add_custom_command(
    OUTPUT ${SQLITE3_ARCH_NAME}
    COMMAND
    ${CMAKE_COMMAND} -DSQLITE3_ARCH_NAME=${SQLITE3_ARCH_NAME} -DSQLITE3_LINK=${SQLITE3_LINK} -DSQLITE3_ARCH_SHA1=${SQLITE3_ARCH_SHA1} -P "${CMAKE_CURRENT_SOURCE_DIR}/DownloadSqlite3.cmake")

add_custom_command(
    OUTPUT sqlite-amalgamation-3210000 sqlite-amalgamation-3210000/sqlite3.c
    DEPENDS ${SQLITE3_ARCH_NAME}
    COMMAND
    ${CMAKE_COMMAND} -E tar xfz ${SQLITE3_ARCH_NAME})

add_library(sqlite3 sqlite-amalgamation-3210000/sqlite3.c)
target_link_libraries(sqlite3 dl pthread)
