cmake_minimum_required(VERSION 3.14...3.17 FATAL_ERROR)
project(wrapper)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT GIT_ARROW_REPO)
    set(GIT_ARROW_REPO "https://github.com/apache/arrow.git")
endif ()
message(STATUS "Arrow Repo:" ${GIT_ARROW_REPO})

if (NOT GIT_ARROW_TAG)
    set(GIT_ARROW_TAG "apache-arrow-2.0.0")
endif ()
message(STATUS "Arrow Tag:" ${GIT_ARROW_TAG})

###################################################################################################
# - cmake modules ---------------------------------------------------------------------------------

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})

###################################################################################################
# - build arrow ------------------------------------------------------------------------------------

message(STATUS "BUILDING ARROW")
include(ConfigureArrow)

if (ARROW_FOUND)
    message(STATUS "Apache Arrow found in ${ARROW_INCLUDE_DIR}")
else ()
    message(FATAL_ERROR "Apache Arrow not found, please check your settings.")
endif (ARROW_FOUND)

add_library(arrow STATIC IMPORTED ${ARROW_LIB})
add_library(parquet STATIC IMPORTED ${PARQUET_LIB})
add_library(thrift STATIC IMPORTED ${THRIFT_LIB})
add_library(utf8proc STATIC IMPORTED ${UTF8PROC_LIB})

if (ARROW_FOUND)
    set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIB})
    set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${PARQUET_LIB})
    set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THRIFT_LIB})
    set_target_properties(utf8proc PROPERTIES IMPORTED_LOCATION ${UTF8PROC_LIB})
endif (ARROW_FOUND)

###################################################################################################

include_directories(${ARROW_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR})

add_library(wrapper STATIC)
target_sources(wrapper PUBLIC ParquetWrapper.cpp
        PayloadStream.cpp)

target_link_libraries(wrapper PUBLIC parquet arrow thrift utf8proc pthread)

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()
install(TARGETS wrapper DESTINATION ${CMAKE_INSTALL_PREFIX})
install(FILES ${ARROW_LIB} ${PARQUET_LIB} ${THRIFT_LIB} ${UTF8PROC_LIB} DESTINATION ${CMAKE_INSTALL_PREFIX})

add_subdirectory(test)