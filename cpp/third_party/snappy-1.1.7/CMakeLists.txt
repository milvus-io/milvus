cmake_minimum_required(VERSION 3.1)
project(Snappy VERSION 1.1.7 LANGUAGES C CXX)

# BUILD_SHARED_LIBS is a standard CMake variable, but we declare it here to make
# it prominent in the GUI.
option(BUILD_SHARED_LIBS "Build shared libraries(DLLs)." OFF)

option(SNAPPY_BUILD_TESTS "Build Snappy's own tests." ON)

include(TestBigEndian)
test_big_endian(SNAPPY_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("byteswap.h" HAVE_BYTESWAP_H)
check_include_file("stddef.h" HAVE_STDDEF_H)
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("sys/endian.h" HAVE_SYS_ENDIAN_H)
check_include_file("sys/mman.h" HAVE_SYS_MMAN_H)
check_include_file("sys/resource.h" HAVE_SYS_RESOURCE_H)
check_include_file("sys/time.h" HAVE_SYS_TIME_H)
check_include_file("sys/uio.h" HAVE_SYS_UIO_H)
check_include_file("unistd.h" HAVE_UNISTD_H)
check_include_file("windows.h" HAVE_WINDOWS_H)

include(CheckLibraryExists)
check_library_exists(z zlibVersion "" HAVE_LIBZ)
check_library_exists(lzo2 lzo1x_1_15_compress "" HAVE_LIBLZO2)

include(CheckCXXSourceCompiles)
check_cxx_source_compiles(
    "int main(void) { return __builtin_expect(0, 1); }" HAVE_BUILTIN_EXPECT)

check_cxx_source_compiles(
    "int main(void) { return __builtin_ctzll(0); }" HAVE_BUILTIN_CTZ)

include(CheckSymbolExists)
check_symbol_exists("mmap" "sys/mman.h" HAVE_FUNC_MMAP)
check_symbol_exists("sysconf" "unistd.h" HAVE_FUNC_SYSCONF)

find_package(GTest QUIET)
if(GTEST_FOUND)
  set(HAVE_GTEST 1)
endif(GTEST_FOUND)

find_package(Gflags QUIET)
if(GFLAGS_FOUND)
  set(HAVE_GFLAGS 1)
endif(GFLAGS_FOUND)

configure_file(
  "${PROJECT_SOURCE_DIR}/cmake/config.h.in"
  "${PROJECT_BINARY_DIR}/config.h"
)

# We don't want to define HAVE_ macros in public headers. Instead, we use
# CMake's variable substitution with 0/1 variables, which will be seen by the
# preprocessor as constants.
set(HAVE_STDINT_H_01  ${HAVE_STDINT_H})
set(HAVE_STDDEF_H_01  ${HAVE_STDDEF_H})
set(HAVE_SYS_UIO_H_01 ${HAVE_SYS_UIO_H})
if(NOT HAVE_STDINT_H_01)
  set(HAVE_STDINT_H_01 0)
endif(NOT HAVE_STDINT_H_01)
if(NOT HAVE_STDDEF_H_01)
  set(HAVE_STDDEF_H_01 0)
endif(NOT HAVE_STDDEF_H_01)
if(NOT HAVE_SYS_UIO_H_01)
  set(HAVE_SYS_UIO_H_01 0)
endif(NOT HAVE_SYS_UIO_H_01)

configure_file(
  "${PROJECT_SOURCE_DIR}/snappy-stubs-public.h.in"
  "${PROJECT_BINARY_DIR}/snappy-stubs-public.h")

add_library(snappy "")
target_sources(snappy
  PRIVATE
    "${PROJECT_SOURCE_DIR}/snappy-internal.h"
    "${PROJECT_SOURCE_DIR}/snappy-stubs-internal.h"
    "${PROJECT_SOURCE_DIR}/snappy-c.cc"
    "${PROJECT_SOURCE_DIR}/snappy-sinksource.cc"
    "${PROJECT_SOURCE_DIR}/snappy-stubs-internal.cc"
    "${PROJECT_SOURCE_DIR}/snappy.cc"
    "${PROJECT_BINARY_DIR}/config.h"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy-c.h>
    $<INSTALL_INTERFACE:include/snappy-c.h>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy-sinksource.h>
    $<INSTALL_INTERFACE:include/snappy-sinksource.h>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy.h>
    $<INSTALL_INTERFACE:include/snappy.h>
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/snappy-stubs-public.h>
    $<INSTALL_INTERFACE:include/snappy-stubs-public.h>
)
target_include_directories(snappy
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(snappy
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(snappy PRIVATE -DHAVE_CONFIG_H)
if(BUILD_SHARED_LIBS)
  set_target_properties(snappy PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif(BUILD_SHARED_LIBS)

if(SNAPPY_BUILD_TESTS)
  enable_testing()

  add_executable(snappy_unittest "")
  target_sources(snappy_unittest
    PRIVATE
      "${PROJECT_SOURCE_DIR}/snappy_unittest.cc"
      "${PROJECT_SOURCE_DIR}/snappy-test.cc"
  )
  target_compile_definitions(snappy_unittest PRIVATE -DHAVE_CONFIG_H)
  target_link_libraries(snappy_unittest snappy ${GFLAGS_LIBRARIES})

  if(HAVE_LIBZ)
    target_link_libraries(snappy_unittest z)
  endif(HAVE_LIBZ)
  if(HAVE_LIBLZO2)
    target_link_libraries(snappy_unittest lzo2)
  endif(HAVE_LIBLZO2)

  target_include_directories(snappy_unittest
    BEFORE PRIVATE
      "${PROJECT_SOURCE_DIR}"
      "${GTEST_INCLUDE_DIRS}"
      "${GFLAGS_INCLUDE_DIRS}"
  )

  add_test(
    NAME snappy_unittest
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    COMMAND "${PROJECT_BINARY_DIR}/snappy_unittest")
endif(SNAPPY_BUILD_TESTS)

include(GNUInstallDirs)
install(TARGETS snappy
  EXPORT SnappyTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(
  FILES
    "${PROJECT_SOURCE_DIR}/snappy-c.h"
    "${PROJECT_SOURCE_DIR}/snappy-sinksource.h"
    "${PROJECT_SOURCE_DIR}/snappy.h"
    "${PROJECT_BINARY_DIR}/snappy-stubs-public.h"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/SnappyConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
)
install(
  EXPORT SnappyTargets
  NAMESPACE Snappy::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Snappy"
)
install(
  FILES
    "${PROJECT_SOURCE_DIR}/cmake/SnappyConfig.cmake"
    "${PROJECT_BINARY_DIR}/SnappyConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Snappy"
)
