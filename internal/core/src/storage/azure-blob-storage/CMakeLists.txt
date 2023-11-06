# Copyright (c) Microsoft Corporation. All rights reserved.
# SPDX-License-Identifier: MIT

cmake_minimum_required (VERSION 3.12)
set(CMAKE_CXX_STANDARD 17)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules")
message("${CMAKE_CURRENT_SOURCE_DIR}")
include(AzureVcpkg)
az_vcpkg_integrate()

project(azure-blob-storage)

find_program(NUGET_EXE NAMES nuget)

if(NOT NUGET_EXE)
    message(FATAL "CMake could not find the nuget command line tool. Please install it from https://www.nuget.org/downloads!")
else()
    exec_program(${NUGET_EXE}
        ARGS install "Microsoft.Attestation.Client" -Version 0.1.181 -ExcludeVersion -OutputDirectory ${CMAKE_BINARY_DIR}/packages)
endif()

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-return-type -Wno-pedantic")
add_library(blob-chunk-manager SHARED AzureBlobChunkManager.cpp)
target_link_libraries(blob-chunk-manager PUBLIC Azure::azure-identity Azure::azure-storage-blobs)

install(TARGETS blob-chunk-manager DESTINATION "${CMAKE_INSTALL_LIBDIR}")

if ( BUILD_UNIT_TEST STREQUAL "ON" )
    add_subdirectory(test)
endif ()