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

#include <arrow/c/bridge.h>
#include "common/EasyAssert.h"
#include "storage/loon_ffi/ffi_reader_c.h"
#include "common/common_type_c.h"
#include "milvus-storage/ffi_c.h"
#include "storage/loon_ffi/util.h"
#include "monitor/scope_metric.h"

ReaderHandle
createFFIReader(char* manifest,
                struct ArrowSchema* schema,
                char** needed_columns,
                int64_t needed_columns_size,
                const std::shared_ptr<Properties>& properties) {
    ReaderHandle reader_handler = 0;

    FFIResult result = reader_new(manifest,
                                  schema,
                                  needed_columns,
                                  needed_columns_size,
                                  properties.get(),
                                  &reader_handler);
    if (!IsSuccess(&result)) {
        auto message = GetErrorMessage(&result);
        // Copy the error message before freeing the FFIResult
        std::string error_msg = message ? message : "Unknown error";
        FreeFFIResult(&result);
        throw std::runtime_error(error_msg);
    }

    FreeFFIResult(&result);
    return reader_handler;
}

CStatus
NewPackedFFIReader(const char* manifest_path,
                   struct ArrowSchema* schema,
                   char** needed_columns,
                   int64_t needed_columns_size,
                   CFFIPackedReader* c_packed_reader,
                   CStorageConfig c_storage_config,
                   CPluginContext* c_plugin_context) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto properties = MakePropertiesFromStorageConfig(c_storage_config);

        AssertInfo(properties != nullptr, "properties is nullptr");

        // Use manifest_path if provided
        char* manifest =
            manifest_path ? const_cast<char*>(manifest_path) : nullptr;
        ReaderHandle reader_handle = createFFIReader(
            manifest, schema, needed_columns, needed_columns_size, properties);
        *c_packed_reader = reader_handle;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
NewPackedFFIReaderWithManifest(const char* manifest_content,
                               struct ArrowSchema* schema,
                               char** needed_columns,
                               int64_t needed_columns_size,
                               CFFIPackedReader* c_packed_reader,
                               CStorageConfig c_storage_config,
                               CPluginContext* c_plugin_context) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto properties = MakePropertiesFromStorageConfig(c_storage_config);

        AssertInfo(properties != nullptr, "properties is nullptr");

        // Use manifest_content as the manifest parameter
        char* manifest =
            manifest_content ? const_cast<char*>(manifest_content) : nullptr;
        ReaderHandle reader_handle = createFFIReader(
            manifest, schema, needed_columns, needed_columns_size, properties);
        *c_packed_reader = reader_handle;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
GetFFIReaderStream(CFFIPackedReader c_packed_reader,
                   int64_t buffer_size,
                   struct ArrowArrayStream* out_stream) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto reader_handle = static_cast<ReaderHandle>(c_packed_reader);

        // Default parameters for get_record_batch_reader
        const char* predicate = nullptr;  // No filtering
        FFIResult result =
            get_record_batch_reader(reader_handle, predicate, out_stream);

        if (!IsSuccess(&result)) {
            auto message = GetErrorMessage(&result);
            std::string error_msg =
                message ? message : "Failed to get record batch reader";
            FreeFFIResult(&result);
            return milvus::FailureCStatus(milvus::ErrorCode::FileReadFailed,
                                          error_msg);
        }

        FreeFFIResult(&result);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloseFFIReader(CFFIPackedReader c_packed_reader) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto reader_handle = static_cast<ReaderHandle>(c_packed_reader);
        if (reader_handle != 0) {
            reader_destroy(reader_handle);
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
