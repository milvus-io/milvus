# Server coverage uses GCC's gcov runtime, including its explicit dump API.
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
    NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR
    NOT CMAKE_C_COMPILER_ID STREQUAL "GNU" OR MILVUS_GPU_VERSION)
    message(FATAL_ERROR "BUILD_SERVER_COVERAGE requires a Linux CPU build with GCC")
endif()

add_compile_options(
    "$<$<COMPILE_LANGUAGE:C,CXX>:--coverage>"
    "$<$<COMPILE_LANGUAGE:C,CXX>:-fprofile-abs-path>"
    "$<$<COMPILE_LANGUAGE:C,CXX>:-fprofile-update=atomic>"
)
add_link_options(--coverage -Wl,--dynamic-list-data)

# Object-only caches may omit the .gcno sidecars needed to decode the data.
set(MILVUS_USE_CCACHE OFF)
set(CMAKE_C_COMPILER_LAUNCHER "")
set(CMAKE_CXX_COMPILER_LAUNCHER "")
set(MILVUS_PCH_CXX_COMPILER_LAUNCHER "")
