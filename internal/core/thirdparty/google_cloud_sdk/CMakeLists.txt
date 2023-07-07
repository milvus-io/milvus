# -------------------------------------------------------------------------------
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
# -------------------------------------------------------------------------------

set(GOOGLE_SDK_VERSION "v2.5.0")
set(GOOGLE_CRC32_VERSION "1.1.2")
set(GOOGLE_GRPC_VERSION "v1.50.1")
set(GOOGLE_ABSEIL_VERSION "20220623.1")

macro(build_google_sdk_s3)
    message(STATUS "Building GOOGLE_ABSEIL-${GOOGLE_ABSEIL_VERSION} from source")

    set(GOOGLE_ABSEIL_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    set(GOOGLE_ABSEIL_BUILD_COMMAND make  ${MAKE_BUILD_ARGS})

    set(GOOGLE_ABSEIL_INSTALL_COMMAND make install)
    if (LINUX)
        set(GOOGLE_PATCH_COMMAND sed -i "s/^#define ABSL_OPTION_USE_\\(.*\\) 2/#define ABSL_OPTION_USE_\\1 0/" "absl/base/options.h")
    endif()

    set(GOOGLE_ABSEIL_CMAKE_ARGS
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
        "-DABSL_BUILD_TESTING=OFF"
        "-DBUILD_SHARED_LIBS=yes")

    ExternalProject_Add(google_abseil_ep
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG ${GOOGLE_ABSEIL_VERSION}
        PREFIX ${CMAKE_BINARY_DIR}/3rdparty_download/google-abseil-subbuild
        PATCH_COMMAND ${GOOGLE_PATCH_COMMAND}
        BUILD_COMMAND ${GOOGLE_ABSEIL_BUILD_COMMAND}
        INSTALL_COMMAND ${GOOGLE_ABSEIL_INSTALL_COMMAND}
        CMAKE_ARGS ${GOOGLE_ABSEIL_CMAKE_ARGS}
    )

    message(STATUS "Building GOOGLE_CRC32-${GOOGLE_CRC32_VERSION} from source")

    set(GOOGLE_CRC32_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    set(GOOGLE_CRC32_BUILD_COMMAND  make ${MAKE_BUILD_ARGS})
    set(GOOGLE_CRC32_INSTALL_COMMAND make install)

    set(GOOGLE_CRC32_CMAKE_ARGS
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
        "-DBUILD_SHARED_LIBS=yes"
        "-DCRC32C_BUILD_TESTS=OFF"
        "-DCRC32C_BUILD_BENCHMARKS=OFF"
        "-DCRC32C_USE_GLOG=OFF")

    ExternalProject_Add(google_crc32_ep
        GIT_REPOSITORY https://github.com/google/crc32c.git
        GIT_TAG ${GOOGLE_CRC32_VERSION}
        PREFIX ${CMAKE_BINARY_DIR}/3rdparty_download/google-crc32-subbuild
        BUILD_COMMAND ${GOOGLE_CRC32_BUILD_COMMAND}
        INSTALL_COMMAND ${GOOGLE_CRC32_INSTALL_COMMAND}
        CMAKE_ARGS ${GOOGLE_CRC32_CMAKE_ARGS}
    )

    message(STATUS "Building GOOGLE_JSON-${NLOHMANN_JSON_VERSION} from source")

    set(GOOGLE_JSON_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    set(GOOGLE_JSON_BUILD_COMMAND make  ${MAKE_BUILD_ARGS})
    set(GOOGLE_JSON_INSTALL_COMMAND make install)

    set(GOOGLE_JSON_CMAKE_ARGS
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
        "-DBUILD_SHARED_LIBS=yes"
        "-DBUILD_TESTING=OFF"
        "-DJSON_BuildTests=OFF")

    ExternalProject_Add(google_json_ep
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG ${GOOGLE_JSON_VERSION}
        PREFIX ${CMAKE_BINARY_DIR}/3rdparty_download/google-json-subbuild
        BUILD_COMMAND ${GOOGLE_JSON_BUILD_COMMAND}
        INSTALL_COMMAND ${GOOGLE_JSON_INSTALL_COMMAND}
        CMAKE_ARGS ${GOOGLE_JSON_CMAKE_ARGS}
    )

    message(STATUS "Building GOOGLE_SDK-${GOOGLE_SDK_VERSION} from source")

    set(GOOGLE_SDK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    set(GOOGLE_SDK_BUILD_COMMAND make ${MAKE_BUILD_ARGS})
    set(GOOGLE_SDK_INSTALL_COMMAND make install)

    if (APPLE)
        if(EXISTS "/usr/local/opt/openssl")
            set(OPENSSL_DIR "/usr/local/opt/openssl")
        else()
            set(OPENSSL_DIR "/opt/homebrew/opt/openssl")
        endif()
        set(GOOGLE_SDK_CMAKE_ARGS
            "-DCMAKE_BUILD_TYPE=Release"
            "-DBUILD_DEPS=OFF"
            "-DBUILD_TESTING=OFF"
            "-DBUILD_SHARED_LIBS=ON"
            "-DGOOGLE_CLOUD_CPP_ENABLE=storage"
            "-DOPENSSL_ROOT_DIR=${OPENSSL_DIR}"
            "-DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF"
            "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
    else() 
        set(GOOGLE_SDK_CMAKE_ARGS
            "-DCMAKE_BUILD_TYPE=Release"
            "-DBUILD_DEPS=OFF"
            "-DBUILD_TESTING=OFF"
            "-DBUILD_SHARED_LIBS=ON"
            "-DGOOGLE_CLOUD_CPP_ENABLE=storage"
            "-DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF"
            "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
   endif()

    message(STATUS "GOOGLE_SDK_CMAKE_ARGS: ${GOOGLE_SDK_CMAKE_ARGS}")
    ExternalProject_Add(google_sdk_ep
        GIT_REPOSITORY https://github.com/googleapis/google-cloud-cpp.git
        GIT_TAG ${GOOGLE_SDK_VERSION}
        PREFIX ${CMAKE_BINARY_DIR}/3rdparty_download/google-sdk-subbuild
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${GOOGLE_SDK_BUILD_COMMAND}
        INSTALL_COMMAND ${GOOGLE_SDK_INSTALL_COMMAND}
        CMAKE_ARGS ${GOOGLE_SDK_CMAKE_ARGS}
    )
    add_dependencies(google_sdk_ep google_abseil_ep)
    add_dependencies(google_sdk_ep google_json_ep)
    add_dependencies(google_sdk_ep google_crc32_ep)

    add_library(google_cloud_cpp_storage SHARED IMPORTED)
    set_target_properties(google_cloud_cpp_storage
        PROPERTIES
        IMPORTED_GLOBAL TRUE
        IMPORTED_LOCATION ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}google_cloud_cpp_storage${CMAKE_SHARED_LIBRARY_SUFFIX}
        INTERFACE_INCLUDE_DIRECTORIES ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})
    add_dependencies(google_cloud_cpp_storage google_sdk_ep)

    add_library(google_cloud_cpp_common SHARED IMPORTED)
    set_target_properties(google_cloud_cpp_common
        PROPERTIES
        IMPORTED_GLOBAL TRUE
        IMPORTED_LOCATION ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}google_cloud_cpp_common${CMAKE_SHARED_LIBRARY_SUFFIX}
        INTERFACE_INCLUDE_DIRECTORIES ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})
    add_dependencies(google_cloud_cpp_common google_sdk_ep)

    add_library(google_cloud_cpp_rest_internal SHARED IMPORTED)
    set_target_properties(google_cloud_cpp_rest_internal
        PROPERTIES
        IMPORTED_GLOBAL TRUE
        IMPORTED_LOCATION ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}google_cloud_cpp_rest_internal${CMAKE_SHARED_LIBRARY_SUFFIX}
        INTERFACE_INCLUDE_DIRECTORIES ${GOOGLE_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})
    add_dependencies(google_cloud_cpp_rest_internal google_sdk_ep)
endmacro()

build_google_sdk_s3()
