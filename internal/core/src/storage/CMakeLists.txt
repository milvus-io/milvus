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

option(EMBEDDED_MILVUS "Enable embedded Milvus" OFF)

if(EMBEDDED_MILVUS)
    add_compile_definitions(EMBEDDED_MILVUS)
endif()

milvus_add_pkg_config("milvus_storage")

if (DEFINED AZURE_BUILD_DIR)
    add_definitions(-DAZURE_BUILD_DIR)
    include_directories(azure-blob-storage)
    include_directories("${AZURE_BUILD_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include")
    set(STORAGE_FILES
        ${STORAGE_FILES}
        AzureChunkManager.cpp
        )
endif()

set(STORAGE_FILES
    ${STORAGE_FILES}
    PayloadStream.cpp
    DataCodec.cpp
    Util.cpp
    PayloadReader.cpp
    PayloadWriter.cpp
    BinlogReader.cpp
    IndexData.cpp
    InsertData.cpp
    Event.cpp
    ThreadPool.cpp
    prometheus_client.cpp
    storage_c.cpp
    ChunkManager.cpp
    MinioChunkManager.cpp
    AliyunSTSClient.cpp
    AliyunCredentialsProvider.cpp
    MemFileManagerImpl.cpp
    LocalChunkManager.cpp
    DiskFileManagerImpl.cpp
    ThreadPools.cpp
    ChunkCache.cpp
    TencentCloudCredentialsProvider.cpp
    TencentCloudSTSClient.cpp)

if(USE_OPENDAL)
    list(APPEND STORAGE_FILES OpenDALChunkManager.cpp)
endif()

add_library(milvus_storage SHARED ${STORAGE_FILES})

if (DEFINED AZURE_BUILD_DIR)
    target_link_libraries(milvus_storage PUBLIC
            "-L${AZURE_BUILD_DIR} -lblob-chunk-manager"
            blob-chunk-manager
            milvus_common
            milvus-storage
            pthread
            ${CONAN_LIBS}
            )
else ()
    target_link_libraries(milvus_storage PUBLIC
            milvus_common
            milvus-storage
            pthread
            ${CONAN_LIBS}
            )
endif()

install(TARGETS milvus_storage DESTINATION "${CMAKE_INSTALL_LIBDIR}")
