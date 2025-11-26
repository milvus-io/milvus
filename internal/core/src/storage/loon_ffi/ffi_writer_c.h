// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "common/common_type_c.h"
#include "common/type_c.h"
#include <arrow/c/abi.h>
#include "milvus-storage/ffi_c.h"

// Opaque handle to a Loon packed writer instance
typedef void* LoonWriterHandler;

// NewPackedLoonWriter creates a new packed writer for writing data to Loon storage.
//
// Parameters:
//   - base_path: The base path in object storage where data will be written
//   - schema: Arrow schema defining the structure of data to be written
//   - c_storage_config: Storage configuration (endpoint, credentials, bucket, etc.)
//   - split_pattern: Pattern defining how columns should be split into groups
//   - writer_handle: Output parameter that receives the created writer handle
//
// Returns:
//   - CStatus indicating success or failure with error message
CStatus
NewPackedLoonWriter(const char* base_path,
                    struct ArrowSchema* schema,
                    CStorageConfig c_storage_config,
                    const char* split_pattern,
                    LoonWriterHandler* writer_handle);

// PackedLoonWrite writes a batch of Arrow arrays to the Loon packed writer.
//
// Parameters:
//   - writer_handle: The writer handle created by NewPackedLoonWriter
//   - arrays: Arrow arrays containing the data to write
//   - array_schemas: Schemas for the individual arrays
//   - schema: The overall schema for the write operation
//
// Returns:
//   - CStatus indicating success or failure with error message
CStatus
PackedLoonWrite(LoonWriterHandler writer_handle,
                struct ArrowArray* arrays,
                struct ArrowSchema* array_schemas,
                struct ArrowSchema* schema);

// ClosePackedLoonWriter finalizes the writer and commits all data to storage.
//
// This function flushes any buffered data, writes the manifest file,
// and releases all resources associated with the writer.
//
// Parameters:
//   - writer_handle: The writer handle to close (will be invalidated after this call)
//   - base_path: The base path where data was written
//   - c_storage_config: Storage configuration for finalizing the write
//   - latest_version: Output parameter that receives the version number of the written data
//
// Returns:
//   - CStatus indicating success or failure with error message
CStatus
ClosePackedLoonWriter(LoonWriterHandler writer_handle,
                      const char* base_path,
                      CStorageConfig c_storage_config,
                      int64_t* latest_version);

#ifdef __cplusplus
}
#endif