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
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "PluginInterface.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/bridge.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "monitor/scope_metric.h"
#include "storage/KeyRetriever.h"
#include "storage/PluginLoader.h"
#include "storage/loon_ffi/ffi_reader_c.h"
#include "storage/loon_ffi/util.h"

/**
 * @brief Creates a Loon reader with optional CMEK decryption support.
 *
 * This function creates a milvus_storage Reader instance and optionally configures
 * it with a key retriever for decrypting encrypted data when CMEK is enabled.
 *
 * @param[in] column_groups Shared pointer to the column groups to read
 * @param[in] schema Arrow schema defining the data structure
 * @param[in] needed_columns Array of column names to read
 * @param[in] needed_columns_size Number of columns in needed_columns array
 * @param[in] properties Storage properties for reader configuration
 * @param[in] c_plugin_context Optional plugin context for CMEK decryption.
 *                             If non-null, configures the reader with a key retriever
 *                             that can decrypt data encrypted with CMEK.
 *
 * @return Unique pointer to the created Reader instance
 *
 * @throws AssertException if schema import fails or cipher plugin is null when required
 */
std::unique_ptr<milvus_storage::api::Reader>
GetLoonReader(
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups,
    struct ArrowSchema* schema,
    char** needed_columns,
    int64_t needed_columns_size,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    CPluginContext* c_plugin_context) {
    auto result = arrow::ImportSchema(schema);
    AssertInfo(result.ok(), "Import arrow schema failed");
    auto arrow_schema = result.ValueOrDie();
    auto reader = milvus_storage::api::Reader::create(
        column_groups,
        arrow_schema,
        std::make_shared<std::vector<std::string>>(
            needed_columns, needed_columns + needed_columns_size),
        *properties);

    // Configure CMEK decryption if plugin context is provided
    if (c_plugin_context != nullptr) {
        auto plugin_ptr =
            milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(plugin_ptr != nullptr, "cipher plugin is nullptr");
        plugin_ptr->Update(c_plugin_context->ez_id,
                           c_plugin_context->collection_id,
                           std::string(c_plugin_context->key));
        auto key_retriever = std::make_shared<milvus::storage::KeyRetriever>();
        reader->set_keyretriever(
            [key_retriever](const std::string& metadata) -> std::string {
                return key_retriever->GetKey(metadata);
            });
    }

    return reader;
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

        auto loon_manifest = GetLoonManifest(manifest_path, properties);
        AssertInfo(loon_manifest != nullptr, "manifest is nullptr");

        auto reader =
            GetLoonReader(std::make_shared<milvus_storage::api::ColumnGroups>(
                              loon_manifest->columnGroups()),
                          schema,
                          needed_columns,
                          needed_columns_size,
                          properties,
                          c_plugin_context);

        *c_packed_reader = static_cast<CFFIPackedReader>(reader.release());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
NewPackedFFIReaderWithManifest(const LoonManifest* loon_manifest,
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
        auto column_groups =
            std::make_shared<milvus_storage::api::ColumnGroups>();
        auto status = milvus_storage::column_groups_import(
            &loon_manifest->column_groups, column_groups.get());
        AssertInfo(status.ok(),
                   "Failed to import column groups: {}",
                   status.ToString());

        auto reader = GetLoonReader(column_groups,
                                    schema,
                                    needed_columns,
                                    needed_columns_size,
                                    properties,
                                    c_plugin_context);

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
