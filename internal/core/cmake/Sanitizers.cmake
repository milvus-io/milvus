# Copyright (C) 2026 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_guard(GLOBAL)
set(MILVUS_SANITIZER "none")
set(MILVUS_SANITIZER_LINK_FLAGS "")

if(USE_TSAN AND (USE_ASAN OR WITH_ASAN))
    message(FATAL_ERROR "USE_ASAN/WITH_ASAN and USE_TSAN are mutually exclusive")
endif()

if(USE_TSAN)
    set(MILVUS_SANITIZER "thread")
    set(MILVUS_SANITIZER_LINK_FLAGS "-fsanitize=thread -shared-libsan")
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
        message(FATAL_ERROR "USE_TSAN currently supports 64-bit Linux only")
    endif()
    if(MILVUS_GPU_VERSION OR WITH_CUVS)
        message(FATAL_ERROR "USE_TSAN does not support GPU builds")
    endif()
    include("${CMAKE_CURRENT_LIST_DIR}/Archer.cmake")

    # Test the driver and runtime link, not just acceptance of a compiler flag.
    include(CMakePushCheckState)
    include(CheckCSourceCompiles)
    include(CheckCXXSourceCompiles)
    function(milvus_check_tsan)
        cmake_push_check_state(RESET)
        # A toolchain may normally use STATIC_LIBRARY for try_compile; force an
        # executable here so a missing sanitizer runtime is detected as well.
        set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
        set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
        set(CMAKE_REQUIRED_LINK_OPTIONS "-fsanitize=thread;-shared-libsan;-Wl,-rpath,${MILVUS_TSAN_RUNTIME_DIR}")
        check_c_source_compiles("int value; int main(void) { return value; }"
            MILVUS_C_HAS_TSAN)
        check_cxx_source_compiles("int value; int main() { return value; }"
            MILVUS_CXX_HAS_TSAN)
        cmake_pop_check_state()
    endfunction()
    milvus_check_tsan()
    if(NOT MILVUS_C_HAS_TSAN OR NOT MILVUS_CXX_HAS_TSAN)
        message(FATAL_ERROR "USE_TSAN requires a working ThreadSanitizer compiler and runtime")
    endif()

    # Apply before add_subdirectory: milvus_core aggregates OBJECT libraries,
    # so instrumenting only its final shared-library target is insufficient.
    add_compile_options(
        "$<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=thread>"
        "$<$<COMPILE_LANGUAGE:C,CXX>:-g>"
        "$<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>")
    add_link_options(-fsanitize=thread -shared-libsan)
    message(STATUS "Building Milvus C/C++ targets with ThreadSanitizer")
    if(NOT MILVUS_CONAN_SANITIZER STREQUAL "thread")
        message(WARNING
            "Conan dependencies are not configured for TSan. Use the Conan tsan profile "
            "to rebuild native dependencies; prebuilt dependencies and Rust code "
            "are not instrumented by USE_TSAN.")
    endif()
elseif(USE_ASAN AND (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR MSYS))
    set(MILVUS_SANITIZER "address")
    # Preserve the existing ASan flags while keeping sanitizer selection central.
    add_compile_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
    add_link_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
endif()

if(DEFINED MILVUS_CONFIGURED_SANITIZER AND
   NOT MILVUS_CONFIGURED_SANITIZER STREQUAL MILVUS_SANITIZER AND
   (MILVUS_CONFIGURED_SANITIZER STREQUAL "thread" OR USE_TSAN))
    message(FATAL_ERROR "Changing TSan mode requires a fresh build and install directory")
endif()
set(MILVUS_CONFIGURED_SANITIZER "${MILVUS_SANITIZER}" CACHE INTERNAL
    "Sanitizer used by this build tree" FORCE)

# Prevent accidental reuse of a TSan dependency graph by an ordinary build.
if(MILVUS_CONAN_SANITIZER STREQUAL "thread" AND NOT USE_TSAN)
    message(FATAL_ERROR "TSan Conan dependencies require USE_TSAN=ON")
endif()
