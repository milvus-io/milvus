set(gtest_force_shared_crt ${MSVC_SHARED_RT} CACHE BOOL
  "Use shared (DLL) run-time lib even when Google Test built as a static lib.")
add_subdirectory(gtest-1.8.0)
include_directories(SYSTEM gtest-1.8.0/googlemock/include)
include_directories(SYSTEM gtest-1.8.0/googletest/include)

if(WIN32 AND BUILD_SHARED_LIBS)
  add_definitions("-DGTEST_LINKED_AS_SHARED_LIBRARY")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" OR
   CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(yaml_test_flags "-Wno-variadic-macros -Wno-sign-compare")

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(yaml_test_flags "${yaml_test_flags} -Wno-c99-extensions")
  endif()

  if(CMAKE_COMPILER_IS_GNUCXX)
    set(yaml_test_flags "${yaml_test_flags} -std=gnu++11")
  else()
    set(yaml_test_flags "${yaml_test_flags} -std=c++11")
  endif()
endif()

file(GLOB test_headers [a-z_]*.h)
file(GLOB test_sources [a-z_]*.cpp integration/[a-z_]*.cpp node/[a-z_]*.cpp)
file(GLOB test_new_api_sources new-api/[a-z]*.cpp)

list(APPEND test_sources ${test_new_api_sources})
add_sources(${test_sources} ${test_headers})

include_directories(${YAML_CPP_SOURCE_DIR}/test)

add_executable(run-tests
	${test_sources}
	${test_headers}
)
set_target_properties(run-tests PROPERTIES
  COMPILE_FLAGS "${yaml_c_flags} ${yaml_cxx_flags} ${yaml_test_flags}"
)
target_link_libraries(run-tests yaml-cpp gmock)

add_test(yaml-test ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/run-tests)
