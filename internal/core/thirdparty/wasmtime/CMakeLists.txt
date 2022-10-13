# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(WASMTIME_VERSION "v1.0.0")
set(WASMTIME_GIT_URL "git@github.com:bytecodealliance/wasmtime.git")
set(WASMTIME_GIT_COMMIT_HASH "9db8498faecf32dfc5c3fa923ac09250656d8f04")

macro(build_wasmtime)
    
    message(STATUS "Building wasmtime-${WASMTIME_VERSION} from source")

    option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

    set (WASMTIME_DIR "${CMAKE_CURRENT_BINARY_DIR}/src")
    message (${WASMTIME_DIR})
    message (wasmtime-crate CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR})

    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        set(WASMTIME_BUILD_TYPE_FLAG "--release")
        set(WASMTIME_BUILD_TYPE "release")
    else()
        set(WASMTIME_BUILD_TYPE "debug")
    endif()

    if(BUILD_SHARED_LIBS)
        if(WIN32)
            set(WASMTIME_INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/wasmtime.dll
                    ${CMAKE_BINARY_DIR})
        elseif(APPLE)
            set(WASMTIME_INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/libwasmtime.dylib
                    ${CMAKE_BINARY_DIR})
        else()
            set(WASMTIME_INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/libwasmtime.so
                    ${CMAKE_BINARY_DIR})
        endif()
    else()
        set(WASMTIME_INSTALL_COMMAND "")
    endif()
    if(BUILD_SHARED_LIBS)
        if(WIN32)
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/wasmtime.dll.lib)
        elseif(APPLE)
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/libwasmtime.dylib)
        else()
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/libwasmtime.so)
        endif()
    else()
        if(WIN32)
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/wasmtime.lib)
        elseif(ANDROID)
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${ANDROID_TARGET}/${WASMTIME_BUILD_TYPE}/libwasmtime.a)
        else()
            set(WASMTIME_BUILD_PRODUCT
                    ${WASMTIME_DIR}/target/${WASMTIME_BUILD_TYPE}/libwasmtime.a)
        endif()
    endif()

    set (WASMTIME_BUILD_COMMAND cargo build ${WASMTIME_BUILD_TYPE_FLAG})

    ExternalProject_Add(
            wasmtime-crate
            GIT_REPOSITORY      ${WASMTIME_GIT_URL}
            GIT_TAG             ${WASMTIME_GIT_COMMIT_HASH}
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            SOURCE_DIR          ${WASMTIME_DIR}
            SOURCE_SUBDIR       "crates/c-api"
            CONFIGURE_COMMAND   ""
            BUILD_IN_SOURCE     1
            BUILD_COMMAND       ${WASMTIME_BUILD_COMMAND}
            BUILD_BYPRODUCTS    ${WASMTIME_BUILD_PRODUCT}
            INSTALL_COMMAND     "${WASMTIME_INSTALL_COMMAND}"
            ${EP_LOG_OPTIONS}
    )

    ExternalProject_Get_Property(
            wasmtime-crate
            DOWNLOAD_DIR
            CONFIGURE_COMMAND
            BUILD_IN_SOURCE
            BUILD_COMMAND
            BUILD_BYPRODUCTS
            INSTALL_COMMAND
            GIT_REPOSITORY
            GIT_TAG
            SOURCE_DIR
            SOURCE_SUBDIR
            )

    MESSAGE("wasmtime-crate DOWNLOAD_DIR            ${DOWNLOAD_DIR}")
    MESSAGE("wasmtime-crate CONFIGURE_COMMAND       ${CONFIGURE_COMMAND}")
    MESSAGE("wasmtime-crate BUILD_IN_SOURCE         ${BUILD_IN_SOURCE}")
    MESSAGE("wasmtime-crate BUILD_COMMAND           ${BUILD_COMMAND}")
    MESSAGE("wasmtime-crate BUILD_BYPRODUCTS        ${BUILD_BYPRODUCTS}")
    MESSAGE("wasmtime-crate INSTALL_COMMAND         ${INSTALL_COMMAND}")
    MESSAGE("wasmtime-crate GIT_REPOSITORY          ${GIT_REPOSITORY}")
    MESSAGE("wasmtime-crate GIT_TAG                 ${GIT_TAG}")
    MESSAGE("wasmtime-crate SOURCE_DIR              ${SOURCE_DIR}")
    MESSAGE("wasmtime-crate SOURCE_SUBDIR           ${SOURCE_SUBDIR}")

    add_library(wasmtime INTERFACE)
    add_dependencies(wasmtime wasmtime-crate)

    if (BUILD_SHARED_LIBS)
        if (NOT WIN32)
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
        endif ()
        target_link_libraries(wasmtime INTERFACE ${WASMTIME_BUILD_PRODUCT})
    else ()
        if (WIN32)
            target_compile_options(wasmtime INTERFACE -DWASM_API_EXTERN= -DWASI_API_EXTERN=)
            target_link_libraries(wasmtime INTERFACE ${WASMTIME_BUILD_PRODUCT}
                    ws2_32 advapi32 userenv ntdll shell32 ole32 bcrypt)
        elseif (APPLE OR ANDROID)
            target_link_libraries(wasmtime INTERFACE ${WASMTIME_BUILD_PRODUCT})
        else ()
            target_link_libraries(wasmtime INTERFACE ${WASMTIME_BUILD_PRODUCT}
                    pthread dl m)
        endif ()
    endif ()

    target_include_directories(wasmtime INTERFACE
            ${WASMTIME_DIR}/crates/c-api/include ${WASMTIME_DIR}/crates/c-api/wasm-c-api/include
            )

    get_target_property(WASMTIME_INTERFACE_INCLUDE_DIRECTORIES wasmtime INTERFACE_INCLUDE_DIRECTORIES)
endmacro()

set(WASMTIME_SOURCE "AUTO")
if (WASMTIME_SOURCE STREQUAL "AUTO")
    message(STATUS "Using wasmtime form system")
    find_package(wasmtime)
    message(STATUS "wasmtime libraries: ${WASMTIME_LIBRARIES}")
    message(STATUS "wasmtime found: ${WASMTIME_FOUND}")

    if (WASMTIME_FOUND)
        add_library(wasmtime)
    else ()
        build_wasmtime()
    endif ()
elseif (WASMTIME_SOURCE STREQUAL "BUNDLED")
    message(STATUS "build wasmtime")
    build_wasmtime()
elseif (WASMTIME_SOURCE STREQUAL "SYSTEM")
    find_package(wasmtime)
    add_library(wasmtime)
endif ()
