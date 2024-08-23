# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_at_current_directory()

if (ENABLE_GCP_NATIVE)
    add_definitions(-DENABLE_GCP_NATIVE)
    add_subdirectory(gcp-native-storage)
    set(SOURCE_FILES
        ${SOURCE_FILES}
        GcpNativeChunkManager.cpp
        )
endif()

if (DEFINED AZURE_BUILD_DIR)
    add_definitions(-DAZURE_BUILD_DIR)
    include_directories(azure-blob-storage)
    include_directories("${AZURE_BUILD_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include")
    set(SOURCE_FILES ${SOURCE_FILES} azure/AzureChunkManager.cpp)
    add_library(azure_blob_chunk_manager SHARED IMPORTED)
    set_target_properties(azure_blob_chunk_manager
        PROPERTIES
        IMPORTED_GLOBAL TRUE
        IMPORTED_LOCATION "${AZURE_BUILD_DIR}/libblob-chunk-manager${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
    get_target_property(AZURE_IMPORTED_LOCATION azure_blob_chunk_manager IMPORTED_LOCATION)
    get_target_property(AZURE_INTERFACE_INCLUDE_DIRECTORIES azure_blob_chunk_manager INTERFACE_INCLUDE_DIRECTORIES)
    message("AZURE_IMPORTED_LOCATION: ${AZURE_IMPORTED_LOCATION}")
    message("AZURE_INTERFACE_INCLUDE_DIRECTORIES: ${AZURE_INTERFACE_INCLUDE_DIRECTORIES}")
endif()

if(USE_OPENDAL)
    include_directories(${OPENDAL_INCLUDE_DIR})
    set(SOURCE_FILES ${SOURCE_FILES} opendal/OpenDALChunkManager.cpp)
endif()

add_library(milvus_storage OBJECT ${SOURCE_FILES})
