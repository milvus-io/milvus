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
    ${CONAN_INCLUDE_DIRS}
    ${MILVUS_COMMON_INCLUDE_DIR}
    ${MILVUS_STORAGE_INCLUDE_DIR}
)

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

# Collect object files for milvus_core
set(MILVUS_CORE_OBJECTS
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

add_library(milvus_core SHARED ${MILVUS_CORE_OBJECTS})

set(LINK_TARGETS
    boost_bitset_ext
    simdjson
    tantivy_binding
    knowhere
    milvus-storage
    milvus-common
    bsoncxx
    ${OpenMP_CXX_FLAGS}
    ${CONAN_LIBS}
    )

# Add RDKit libraries
if(TARGET RDKit::RDKit)
    list(APPEND LINK_TARGETS RDKit::RDKit)
endif()

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

# Add mol_c.o at link time (compiled separately with GCC 11 for C++20 support)
# Cannot add .o file to add_library sources as CMake checks file existence at configure time
if(DEFINED RDKIT_INCLUDE_DIR)
    set(MOL_C_OBJECT ${CMAKE_CURRENT_BINARY_DIR}/common/mol_c.o)
    target_link_libraries(milvus_core ${MOL_C_OBJECT})
    # Ensure mol_c.o is built before linking milvus_core
    add_dependencies(milvus_core mol_c_build)
    # Ensure RDKit shared libraries can be found at runtime
    target_link_directories(milvus_core PUBLIC ${CONAN_LIB_DIRS_RDKIT})
endif()

install(TARGETS milvus_core DESTINATION "${CMAKE_INSTALL_LIBDIR}")
