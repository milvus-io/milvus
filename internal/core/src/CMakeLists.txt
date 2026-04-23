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

if(MILVUS_GPU_VERSION STREQUAL "ON")
    project(core CXX C CUDA)
else()
    project(core CXX C)
endif()

option( EMBEDDED_MILVUS "Enable embedded Milvus" OFF )
if ( EMBEDDED_MILVUS )
    add_compile_definitions( EMBEDDED_MILVUS )
endif()

include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})

include_directories(
    ${KNOWHERE_INCLUDE_DIR}
    ${SIMDJSON_INCLUDE_DIR}
    ${TANTIVY_INCLUDE_DIR}
    ${BSONCXX_INCLUDE_DIR}
    ${MILVUS_COMMON_INCLUDE_DIR}
    ${MILVUS_STORAGE_INCLUDE_DIR}
)

# Conan dependency targets shared between milvus_conan_deps (for object library
# include propagation) and LINK_TARGETS (for milvus_core final linking).
set(CONAN_TARGETS
    protobuf::libprotobuf
    glog::glog
    gflags
    Folly::folly
    RocksDB::rocksdb-shared
    arrow::arrow
    gRPC::grpc++
    opentelemetry-cpp::opentelemetry-cpp
    google-cloud-cpp::google-cloud-cpp
    prometheus-cpp::prometheus-cpp
    yaml-cpp
    nlohmann_json::nlohmann_json
    fmt::fmt
    re2::re2
    OpenSSL::SSL
    OpenSSL::Crypto
    CURL::libcurl
    ZLIB::ZLIB
    lz4::lz4
    zstd::libzstd_static
    Snappy::snappy
    roaring::roaring
    marisa::marisa
    GEOS::geos_c
    ICU::uc
    xxHash::xxhash
    TBB::tbb
    libsodium::libsodium
    double-conversion::double-conversion
    xsimd
    unordered_dense::unordered_dense
    abseil::abseil
    rapidjson
    simde::simde
    LibLZMA::LibLZMA
    RdKafka::rdkafka++
    googleapis::googleapis
    boost::boost
    libavrocpp::libavrocpp
    aws-sdk-cpp::aws-sdk-cpp
    Azure::azure-core
)

# Interface library to propagate Conan include directories to object libraries.
# In Conan 1, conan_basic_setup() set global include dirs. In Conan 2, includes
# are per-target, so object libraries need explicit linkage to get include paths.
add_library(milvus_conan_deps INTERFACE)
target_link_libraries(milvus_conan_deps INTERFACE ${CONAN_TARGETS})
# Azure SDK recipe doesn't set includedirs on components in Conan 2.x.
# Locate azure headers via CMAKE_INCLUDE_PATH (set by conan_toolchain.cmake).
# NO_CACHE prevents stale NOTFOUND results across cmake reconfigures.
find_path(_AZURE_INCLUDE_DIR azure/core.hpp NO_CACHE)
if(_AZURE_INCLUDE_DIR)
    target_include_directories(milvus_conan_deps INTERFACE "${_AZURE_INCLUDE_DIR}")
endif()

add_subdirectory( pb )
add_subdirectory( config )
add_subdirectory( common )
add_subdirectory( monitor )
add_subdirectory( storage )
add_subdirectory( index )
add_subdirectory( query )
add_subdirectory( segcore )
add_subdirectory( indexbuilder )
add_subdirectory( clustering )
add_subdirectory( exec )
add_subdirectory( bitset )
add_subdirectory( futures )
add_subdirectory( rescores )
add_subdirectory( plan )
add_subdirectory( minhash )

milvus_add_pkg_config("milvus_core")

# ---- Precompiled Headers (PCH) ----
# Apply the same STL PCH to all OBJECT libraries. Each target generates its
# own PCH binary (small per-target overhead) but avoids cross-directory
# REUSE_FROM issues with CMake < 3.22.
if (MILVUS_USE_PCH)
    # Use generator expression to apply PCH only for C++ compilation.
    # The project enables both C and CXX languages; without this guard
    # CMake would also generate a C PCH containing C++ headers, which fails.
    set(MILVUS_PCH_HEADERS
        # STL headers included in 50+ .cpp files
        $<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CURRENT_SOURCE_DIR}/../cmake/milvus_pch.hxx>
    )
    # NOTE: milvus_bitset and milvus_minhash are excluded because they contain
    # source files compiled with special target flags (-mavx512f, -march=armv8-a+sve)
    # which are incompatible with PCH compiled under default target features.
    foreach(_target
        milvus_common milvus_config milvus_monitor milvus_storage milvus_index
        milvus_query milvus_segcore milvus_indexbuilder milvus_clustering
        milvus_exec milvus_futures milvus_rescores
        milvus_plan)
        target_precompile_headers(${_target} PRIVATE ${MILVUS_PCH_HEADERS})
    endforeach()
endif()

add_library(milvus_core SHARED
    $<TARGET_OBJECTS:milvus_pb>
    $<TARGET_OBJECTS:milvus_config>
    $<TARGET_OBJECTS:milvus_common>
    $<TARGET_OBJECTS:milvus_monitor>
    $<TARGET_OBJECTS:milvus_storage>
    $<TARGET_OBJECTS:milvus_index>
    $<TARGET_OBJECTS:milvus_query>
    $<TARGET_OBJECTS:milvus_segcore>
    $<TARGET_OBJECTS:milvus_indexbuilder>
    $<TARGET_OBJECTS:milvus_clustering>
    $<TARGET_OBJECTS:milvus_exec>
    $<TARGET_OBJECTS:milvus_bitset>
    $<TARGET_OBJECTS:milvus_futures>
    $<TARGET_OBJECTS:milvus_rescores>
    $<TARGET_OBJECTS:milvus_plan>
    $<TARGET_OBJECTS:milvus_minhash>
)

set(LINK_TARGETS
    boost_bitset_ext
    simdjson
    tantivy_binding
    knowhere
    milvus-storage
    milvus-common
    bsoncxx
    ${OpenMP_CXX_FLAGS}
    ${CONAN_TARGETS}
    )

if(USE_OPENDAL)
    set(LINK_TARGETS ${LINK_TARGETS} opendal)
endif()

if(ENABLE_AZURE_FS)
    set(AZURE_BUILD_DIR ON)
    # Arrow already includes Azure SDK when built with_azure=True
    # No need to link additional azure_blob_chunk_manager
endif()

if (ENABLE_GCP_NATIVE)
    set(LINK_TARGETS ${LINK_TARGETS} gcp-native-storage)
endif()

target_link_libraries(milvus_core ${LINK_TARGETS})

install(TARGETS milvus_core DESTINATION "${CMAKE_INSTALL_LIBDIR}")
