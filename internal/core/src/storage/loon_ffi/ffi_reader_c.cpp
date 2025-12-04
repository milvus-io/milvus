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
#include "milvus-storage/reader.h"
#include "storage/loon_ffi/util.h"
#include "monitor/scope_metric.h"

ReaderHandle
createFFIReader(ColumnGroupsHandle column_groups_handle,
                struct ArrowSchema* schema,
                char** needed_columns,
                int64_t needed_columns_size,
                const std::shared_ptr<Properties>& properties) {
    ReaderHandle reader_handler = 0;

    FFIResult result = reader_new(column_groups_handle,
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

std::unique_ptr<milvus_storage::api::Reader>
GetLoonReader(
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups,
    struct ArrowSchema* schema,
    char** needed_columns,
    int64_t needed_columns_size,
    const std::shared_ptr<milvus_storage::api::Properties>& properties) {
    auto result = arrow::ImportSchema(schema);
    AssertInfo(result.ok(), "Import arrow schema failed");
    auto arrow_schema = result.ValueOrDie();
    return milvus_storage::api::Reader::create(
        column_groups,
        arrow_schema,
        std::make_shared<std::vector<std::string>>(
            needed_columns, needed_columns + needed_columns_size),
        *properties);
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
        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        AssertInfo(properties != nullptr, "properties is nullptr");

        auto column_groups = GetColumnGroups(manifest_path, properties);
        AssertInfo(column_groups != nullptr, "column groups is nullptr");

        auto reader = GetLoonReader(column_groups,
                                    schema,
                                    needed_columns,
                                    needed_columns_size,
                                    properties);

        *c_packed_reader = static_cast<CFFIPackedReader>(reader.release());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
NewPackedFFIReaderWithManifest(const ColumnGroupsHandle column_groups_handle,
                               struct ArrowSchema* schema,
                               char** needed_columns,
                               int64_t needed_columns_size,
                               CFFIPackedReader* c_loon_reader,
                               CStorageConfig c_storage_config,
                               CPluginContext* c_plugin_context) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        auto* cg_ptr = reinterpret_cast<
            std::shared_ptr<milvus_storage::api::ColumnGroups>*>(
            column_groups_handle);
        auto cpp_column_groups = *cg_ptr;

        auto reader = GetLoonReader(cpp_column_groups,
                                    schema,
                                    needed_columns,
                                    needed_columns_size,
                                    properties);

        *c_loon_reader = static_cast<CFFIPackedReader>(reader.release());
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
        auto reader =
            static_cast<milvus_storage::api::Reader*>(c_packed_reader);

        // FFIResult result =
        //     get_record_batch_reader(reader_handle, predicate, out_stream);
        auto result = reader->get_record_batch_reader();
        AssertInfo(result.ok(),
                   "failed to get record batch reader, {}",
                   result.status().ToString());

        auto array_stream = result.ValueOrDie();
        arrow::Status status =
            arrow::ExportRecordBatchReader(array_stream, out_stream);
        AssertInfo(status.ok(),
                   "failed to export record batch reader, {}",
                   status.ToString());

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloseFFIReader(CFFIPackedReader c_packed_reader) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto reader =
            static_cast<milvus_storage::api::Reader*>(c_packed_reader);
        delete reader;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
