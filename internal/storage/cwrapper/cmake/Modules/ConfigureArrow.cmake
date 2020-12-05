set(ARROW_ROOT ${CMAKE_BINARY_DIR}/arrow)

set(ARROW_CMAKE_ARGS " -DARROW_WITH_LZ4=OFF"
        " -DARROW_WITH_ZSTD=OFF"
        " -DARROW_WITH_BROTLI=OFF"
        " -DARROW_WITH_SNAPPY=OFF"
        " -DARROW_WITH_ZLIB=OFF"
        " -DARROW_BUILD_STATIC=ON"
        " -DARROW_BUILD_SHARED=OFF"
        " -DARROW_BOOST_USE_SHARED=OFF"
        " -DARROW_BUILD_TESTS=OFF"
        " -DARROW_TEST_MEMCHECK=OFF"
        " -DARROW_BUILD_BENCHMARKS=OFF"
        " -DARROW_CUDA=OFF"
        " -DARROW_JEMALLOC=OFF"
        " -DARROW_PYTHON=OFF"
        " -DARROW_BUILD_UTILITIES=OFF"
        " -DARROW_PARQUET=ON"
        " -DPARQUET_BUILD_SHARED=OFF"
        " -DARROW_S3=OFF"
        " -DCMAKE_VERBOSE_MAKEFILE=ON")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/Templates/Arrow.CMakeLists.txt.cmake"
    "${ARROW_ROOT}/CMakeLists.txt")

file(MAKE_DIRECTORY "${ARROW_ROOT}/build")
file(MAKE_DIRECTORY "${ARROW_ROOT}/install")

execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE ARROW_CONFIG
    WORKING_DIRECTORY ${ARROW_ROOT})

if(ARROW_CONFIG)
  message(FATAL_ERROR "Configuring Arrow failed: " ${ARROW_CONFIG})
endif(ARROW_CONFIG)

set(PARALLEL_BUILD -j)
if($ENV{PARALLEL_LEVEL})
  set(NUM_JOBS $ENV{PARALLEL_LEVEL})
  set(PARALLEL_BUILD "${PARALLEL_BUILD}${NUM_JOBS}")
endif($ENV{PARALLEL_LEVEL})

if(${NUM_JOBS})
  if(${NUM_JOBS} EQUAL 1)
    message(STATUS "ARROW BUILD: Enabling Sequential CMake build")
  elseif(${NUM_JOBS} GREATER 1)
    message(STATUS "ARROW BUILD: Enabling Parallel CMake build with ${NUM_JOBS} jobs")
  endif(${NUM_JOBS} EQUAL 1)
else()
  message(STATUS "ARROW BUILD: Enabling Parallel CMake build with all threads")
endif(${NUM_JOBS})

execute_process(
    COMMAND ${CMAKE_COMMAND} --build .. -- ${PARALLEL_BUILD}
    RESULT_VARIABLE ARROW_BUILD
    WORKING_DIRECTORY ${ARROW_ROOT}/build)

if(ARROW_BUILD)
  message(FATAL_ERROR "Building Arrow failed: " ${ARROW_BUILD})
endif(ARROW_BUILD)

message(STATUS "Arrow installed here: " ${ARROW_ROOT}/install)
set(ARROW_LIBRARY_DIR "${ARROW_ROOT}/install/lib")
set(ARROW_INCLUDE_DIR "${ARROW_ROOT}/install/include")

find_library(ARROW_LIB arrow
    NO_DEFAULT_PATH
    HINTS "${ARROW_LIBRARY_DIR}")
message(STATUS "Arrow library: " ${ARROW_LIB})

find_library(PARQUET_LIB parquet
        NO_DEFAULT_PATH
        HINTS "${ARROW_LIBRARY_DIR}")
message(STATUS "Parquet library: " ${PARQUET_LIB})

find_library(THRIFT_LIB thrift
        NO_DEFAULT_PATH
        HINTS "${ARROW_ROOT}/build/thrift_ep-install/lib")
message(STATUS "Thirft library: " ${THRIFT_LIB})

find_library(UTF8PROC_LIB utf8proc
        NO_DEFAULT_PATH
        HINTS "${ARROW_ROOT}/build/utf8proc_ep-install/lib")
message(STATUS "utf8proc library: " ${UTF8PROC_LIB})

if(ARROW_LIB AND PARQUET_LIB AND THRIFT_LIB AND UTF8PROC_LIB)
  set(ARROW_FOUND TRUE)
endif(ARROW_LIB AND PARQUET_LIB AND THRIFT_LIB AND UTF8PROC_LIB)

message(STATUS "FlatBuffers installed here: " ${FLATBUFFERS_ROOT})
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_ROOT}/include")
set(FLATBUFFERS_LIBRARY_DIR "${FLATBUFFERS_ROOT}/lib")

add_definitions(-DARROW_METADATA_V4)
