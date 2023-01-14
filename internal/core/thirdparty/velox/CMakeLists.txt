#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

set(VELOX_SOURCE_URL "https://github.com/jiaoew1991/velox")
set(VELOX_COMMIT_ID "milvus")

macro(build_velox)
  message(STATUS "Building velox-${VELOX_COMMIT_ID} from source")
  set(VELOX_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
  set(VELOX_CMAKE_ARGS 
    ${EP_COMMON_CMAKE_ARGS}
    -DVELOX_ENABLE_DUCKDB=OFF
    -DVELOX_ENABLE_TPCH_CONNECTOR=OFF
    -DVELOX_ENABLE_SPARK_FUNCTIONS=OFF
    -DVELOX_ENABLE_PARSE=OFF
    -DVELOX_BUILD_TESTING=OFF
    -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
    -DCMAKE_PREFIX_PATH=${CONAN_BISON_ROOT}$<SEMICOLON>${CONAN_FLEX_ROOT}$<SEMICOLON>${CONAN_PROTOBUF_ROOT}
    -DCMAKE_MODULE_PATH=${CMAKE_BINARY_DIR}/conan/
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
    -DCMAKE_LIBRARY_ARCHITECTURE=${CMAKE_LIBRARY_ARCHITECTURE}
    )
  if(APPLE) 
    set(VELOX_CMAKE_ARGS ${VELOX_CMAKE_ARGS} -DCMAKE_AR=/usr/local/opt/llvm/bin/llvm-ar)
  endif()
  message(STATUS ${CMAKE_BUILD_TYPE})


  externalproject_add(velox_ep
    GIT_REPOSITORY ${VELOX_SOURCE_URL}
    GIT_TAG        ${VELOX_COMMIT_ID}
    GIT_SHALLOW    true
    # SOURCE_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/src
    CMAKE_ARGS     ${VELOX_CMAKE_ARGS}
    PREFIX         ${CMAKE_BINARY_DIR}/3rdparty_download/velox-subbuild
    BINARY_DIR     velox-bin
    INSTALL_DIR    ${CMAKE_INSTALL_PREFIX}
    UPDATE_COMMAND ""
    )

  ExternalProject_Get_Property(velox_ep INSTALL_DIR)

  if(NOT IS_DIRECTORY ${INSTALL_DIR}/include)
    file(MAKE_DIRECTORY "${INSTALL_DIR}/include")
  endif()

  add_library(velox_bundled STATIC IMPORTED)
  set_target_properties(velox_bundled
    PROPERTIES
    IMPORTED_GLOBAL     TRUE
    IMPORTED_LOCATION   ${CMAKE_BINARY_DIR}/thirdparty/velox/velox-bin/libvelox_bundled.a
    INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})
  add_dependencies(velox_bundled velox_ep)
  target_link_libraries(velox_bundled INTERFACE ${CONAN_LIBS})

  if(APPLE)
    target_link_libraries(velox_bundled INTERFACE c++abi)
  endif()

  execute_process(
    COMMAND bash -c "( source ${CMAKE_BINARY_DIR}/3rdparty_download/velox-subbuild/src/velox_ep/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
    OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
    RESULT_VARIABLE COMMAND_STATUS)
  set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS})
  message("Change cxx flags ${CMAKE_CXX_FLAGS}")

endmacro()

set(VELOX_SOURCE "AUTO")
if (VELOX_SOURCE STREQUAL "AUTO")
    find_package(velox)
    message(STATUS "velox libraries: ${VELOX_LIBRARIES}")
    message(STATUS "velox found: ${VELOX_FOUND}")

    if (VELOX_FOUND)
        add_library(velox)
    else()
        build_velox()
    endif()
elseif (VELOX_SOURCE STREQUAL "BUNDLED")
    build_velox()
elseif (VELOX_SOURCE STREQUAL "SYSTEM")
    find_package(velox REQUIRED)
    add_library(velox)
endif ()
