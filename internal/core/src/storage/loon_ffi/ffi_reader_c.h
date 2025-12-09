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

/**
 * @brief Handle to a packed FFI reader instance.
 *
 * This is an alias for ReaderHandle used to read packed columnar data
 * from storage in Milvus. The reader supports Arrow-based data access
 * through the FFI (Foreign Function Interface) layer.
 */
typedef void* CFFIPackedReader;

/**
 * @brief Creates a new packed FFI reader from a manifest file path.
 *
 * This function initializes a packed reader that can read columnar data
 * from storage based on the manifest file. The manifest contains metadata
 * about the data layout and file locations.
 *
 * @param manifest_path         Path to the manifest file in object storage.
 *                              Must be a valid UTF-8 encoded null-terminated string.
 * @param schema                Arrow schema defining the structure of the data.
 *                              Must be a valid ArrowSchema pointer conforming to
 *                              the Arrow C data interface specification.
 * @param needed_columns        Array of column names to read. If NULL, all columns
 *                              from the schema will be read.
 * @param needed_columns_size   Number of column names in the needed_columns array.
 *                              Must be 0 if needed_columns is NULL.
 * @param c_packed_reader       Output parameter for the created reader handle.
 *                              On success, will contain a valid reader handle that
 *                              must be released by the caller when no longer needed.
 * @param c_storage_config      Storage configuration containing credentials and
 *                              endpoint information for accessing object storage.
 * @param c_plugin_context      Plugin context for extensibility, may be NULL if
 *                              no plugins are used.
 *
 * @return CStatus indicating success or failure. On failure, the error_msg
 *         field contains details about what went wrong.
 *
 * @note The caller is responsible for releasing the reader handle after use.
 * @note The schema pointer must remain valid for the lifetime of the reader.
 */
CStatus
NewPackedFFIReader(const char* manifest_path,
                   struct ArrowSchema* schema,
                   char** needed_columns,
                   int64_t needed_columns_size,
                   CFFIPackedReader* c_packed_reader,
                   CStorageConfig c_storage_config,
                   CPluginContext* c_plugin_context);

/**
 * @brief Creates a new packed FFI reader from manifest content directly.
 *
 * Similar to NewPackedFFIReader, but accepts the manifest content directly
 * as a string instead of reading from a file path. This is useful when the
 * manifest has already been loaded or is generated dynamically.
 *
 * @param manifest_content      The manifest content as a null-terminated string.
 *                              Must be valid JSON or protobuf text format containing
 *                              the manifest data.
 * @param schema                Arrow schema defining the structure of the data.
 *                              Must be a valid ArrowSchema pointer conforming to
 *                              the Arrow C data interface specification.
 * @param needed_columns        Array of column names to read. If NULL, all columns
 *                              from the schema will be read.
 * @param needed_columns_size   Number of column names in the needed_columns array.
 *                              Must be 0 if needed_columns is NULL.
 * @param c_packed_reader       Output parameter for the created reader handle.
 *                              On success, will contain a valid reader handle that
 *                              must be released by the caller when no longer needed.
 * @param c_storage_config      Storage configuration containing credentials and
 *                              endpoint information for accessing object storage.
 * @param c_plugin_context      Plugin context for extensibility, may be NULL if
 *                              no plugins are used.
 *
 * @return CStatus indicating success or failure. On failure, the error_msg
 *         field contains details about what went wrong.
 *
 * @note The caller is responsible for releasing the reader handle after use.
 * @note The schema pointer must remain valid for the lifetime of the reader.
 * @note The manifest content is copied internally, so the input string can
 *       be freed after this call returns.
 */
CStatus
NewPackedFFIReaderWithManifest(const ColumnGroupsHandle column_groups_handle,
                               struct ArrowSchema* schema,
                               char** needed_columns,
                               int64_t needed_columns_size,
                               CFFIPackedReader* c_loon_reader,
                               CStorageConfig c_storage_config,
                               CPluginContext* c_plugin_context);

/**
 * @brief Gets an ArrowArrayStream from the FFI reader for streaming data access.
 *
 * This function returns an ArrowArrayStream that can be used to iterate through
 * record batches. The stream follows the Arrow C Stream Interface specification
 * and must be released by calling stream->release() when done.
 *
 * @param c_packed_reader   The FFI reader handle.
 * @param out_stream        Output parameter for the ArrowArrayStream. The caller
 *                          is responsible for calling stream->release() when done.
 *
 * @return CStatus indicating success or failure. On failure, the error_msg
 *         field contains details about what went wrong.
 *
 * @note The stream must be released by calling out_stream->release(out_stream)
 *       when no longer needed to prevent memory leaks.
 * @note Each call to this function creates a new stream starting from the beginning.
 */
CStatus
GetFFIReaderStream(CFFIPackedReader c_packed_reader,
                   int64_t batch_size,
                   struct ArrowArrayStream* out_stream);

/**
 * @brief Closes and releases the FFI reader.
 *
 * @param c_packed_reader   The FFI reader handle to close.
 *
 * @return CStatus indicating success or failure.
 */
CStatus
CloseFFIReader(CFFIPackedReader c_packed_reader);

#ifdef __cplusplus
}
#endif