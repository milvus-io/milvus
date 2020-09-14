project(sulvim_core)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

cmake_minimum_required(VERSION 3.16)
set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )
set (CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include_directories(src)
add_subdirectory(src)
add_subdirectory(unittest)

install(
        DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/dog_segment/
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/include
        FILES_MATCHING PATTERN "*_c.h"
)

install(FILES ${CMAKE_BINARY_DIR}/src/dog_segment/libmilvus_dog_segment.so
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/lib)
