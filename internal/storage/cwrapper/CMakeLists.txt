cmake_minimum_required(VERSION 3.14...3.17 FATAL_ERROR)
project(wrapper)

set(CMAKE_CXX_STANDARD 17)

###################################################################################################
# - cmake modules ---------------------------------------------------------------------------------

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})

###################################################################################################
# - build arrow ------------------------------------------------------------------------------------

message(STATUS "BUILDING ARROW")
include(ConfigureArrow)

if(ARROW_FOUND)
    message(STATUS "Apache Arrow found in ${ARROW_INCLUDE_DIR}")
else()
    message(FATAL_ERROR "Apache Arrow not found, please check your settings.")
endif(ARROW_FOUND)

add_library(arrow STATIC IMPORTED ${ARROW_LIB})
add_library(parquet STATIC IMPORTED ${PARQUET_LIB})
add_library(thrift STATIC IMPORTED ${THRIFT_LIB})
add_library(utf8proc STATIC IMPORTED ${UTF8PROC_LIB})

if(ARROW_FOUND)
    set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIB})
    set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${PARQUET_LIB})
    set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THRIFT_LIB})
    set_target_properties(utf8proc PROPERTIES IMPORTED_LOCATION ${UTF8PROC_LIB})
endif(ARROW_FOUND)

###################################################################################################

include_directories(${ARROW_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR})

add_library(wrapper ParquetWrapper.cpp ParquetWrapper.h ColumnType.h PayloadStream.h PayloadStream.cpp)

add_subdirectory(test)