project(CLIENT_CPP LANGUAGES CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

cmake_minimum_required(VERSION 3.16)
set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )
set (CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lprotobuf -fPIC -pthread -Wall -Wno-unused-variable -Wno-sign-compare -Werror")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(OUTDIR ${CMAKE_CURRENT_SOURCE_DIR}/build)
SET(EXECUTABLE_OUTPUT_PATH ${OUTDIR})

if( CMAKE_BUILD_TYPE STREQUAL "Release" )
    message(STATUS "Building Release version")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_ARROW_ARRAY_SIZE=1073741824") #1G
else()
    message(STATUS "Building Debug version")
    add_definitions(-DDEBUG_RENDER)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_ARROW_ARRAY_SIZE=16777216") #16M
endif()
if ("${MAKE}" STREQUAL "")
    if (NOT MSVC)
        find_program(MAKE make)
    endif ()
endif ()

set(CLIENT_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(CLIENT_SRC ${PROJECT_SOURCE_DIR}/src)

add_subdirectory(src)
add_subdirectory(unittest)
#add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/unittest)