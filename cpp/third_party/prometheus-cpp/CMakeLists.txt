
cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(prometheus-cpp)

include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(ENABLE_PULL "Build prometheus-cpp pull library" ON)
option(ENABLE_PUSH "Build prometheus-cpp push library" ON)
option(ENABLE_COMPRESSION "Enable gzip compression" ON)
option(ENABLE_TESTING "Build tests" ON)
option(USE_THIRDPARTY_LIBRARIES "Use 3rdParty submodules" ON)
option(OVERRIDE_CXX_STANDARD_FLAGS "Force building with -std=c++11 even if the CXXLFAGS are configured differently" ON)

if(OVERRIDE_CXX_STANDARD_FLAGS)
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_EXTENSIONS Off)
endif()

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads)

if(ENABLE_TESTING)
  if(USE_THIRDPARTY_LIBRARIES)
    find_package(googlemock-3rdparty CONFIG REQUIRED)
  else()
    find_package(GTest 1.8.1 CONFIG REQUIRED)
  endif()
  find_package(benchmark CONFIG)
  enable_testing()
endif()

# build flags for CI system

if(ENABLE_WARNINGS_AS_ERRORS)
  add_compile_options(
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:AppleClang>>:-Werror>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:AppleClang>>:-Wall>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:AppleClang>>:-Wextra>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:AppleClang>>:-pedantic-errors>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:GNU>>:-Werror>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:GNU>>:-Wall>
    $<$<AND:$<STREQUAL:$<COMPILE_LANGUAGE>,CXX>,$<CXX_COMPILER_ID:GNU>>:-pedantic-errors>
  )
endif()

# prometheus-cpp

add_subdirectory(core)

if(ENABLE_PULL)
  add_subdirectory(pull)
endif()

if(ENABLE_PUSH)
  add_subdirectory(push)
endif()

# install

include(CMakePackageConfigHelpers)

install(
  EXPORT ${PROJECT_NAME}-targets
  NAMESPACE ${PROJECT_NAME}::
  FILE ${PROJECT_NAME}-targets.cmake
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/${PROJECT_NAME}-config.cmake.in"
  ${PROJECT_NAME}-config.cmake
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  NO_CHECK_REQUIRED_COMPONENTS_MACRO
  PATH_VARS CMAKE_INSTALL_INCLUDEDIR
)

install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)

# summary

include(FeatureSummary)
add_feature_info("Pull" "${ENABLE_PULL}" "support for pulling metrics")
add_feature_info("Push" "${ENABLE_PUSH}" "support for pushing metrics to a push-gateway")
add_feature_info("Compression" "${ENABLE_COMPRESSION}" "support for zlib compression of metrics")
feature_summary(WHAT ALL)
