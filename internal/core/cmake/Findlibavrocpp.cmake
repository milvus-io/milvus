# Findlibavrocpp.cmake
# Bridge module for milvus-storage's find_package(libavrocpp) call.
#
# The libavrocpp conan recipe exports cmake_file_name="avro-cpp" and
# cmake_target_name="avro-cpp::avrocpp_s", but milvus-storage expects
# find_package(libavrocpp) and target libavrocpp::libavrocpp.
# This module creates the expected target using conan variables.

if(TARGET libavrocpp::libavrocpp)
    set(libavrocpp_FOUND TRUE)
    return()
endif()

if(CONAN_LIBAVROCPP_ROOT)
    set(libavrocpp_FOUND TRUE)

    # Collect transitive dependency include dirs needed by avro headers
    # (avro/Exception.hh includes fmt/core.h, avro headers use boost, etc.)
    set(_LIBAVROCPP_INCLUDE_DIRS
        ${CONAN_INCLUDE_DIRS_LIBAVROCPP}
        ${CONAN_INCLUDE_DIRS_FMT}
        ${CONAN_INCLUDE_DIRS_BOOST}
        ${CONAN_INCLUDE_DIRS_SNAPPY}
        ${CONAN_INCLUDE_DIRS_ZLIB}
    )

    add_library(libavrocpp::libavrocpp INTERFACE IMPORTED)
    set_target_properties(libavrocpp::libavrocpp PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${_LIBAVROCPP_INCLUDE_DIRS}"
        INTERFACE_LINK_DIRECTORIES "${CONAN_LIB_DIRS_LIBAVROCPP}"
        INTERFACE_LINK_LIBRARIES "${CONAN_LIBS_LIBAVROCPP}"
    )
else()
    set(libavrocpp_FOUND FALSE)
    if(libavrocpp_FIND_REQUIRED)
        message(FATAL_ERROR "libavrocpp not found. Ensure it is installed via conan.")
    endif()
endif()
