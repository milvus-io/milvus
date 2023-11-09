# Copyright (c) Microsoft Corporation. All rights reserved.
# SPDX-License-Identifier: MIT

project(azure-blob-test)

add_executable(azure-blob-test test_azure_blob_chunk_manager.cpp ../AzureBlobChunkManager.cpp)
target_link_libraries(azure-blob-test PRIVATE Azure::azure-identity Azure::azure-storage-blobs)
