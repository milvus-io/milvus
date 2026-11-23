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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "PluginInterface.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/common/extend_status.h"
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
#include <arrow/array.h>
#include <arrow/record_batch.h>

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
    auto bound_columns = std::make_shared<std::vector<std::string>>(
        needed_columns, needed_columns + needed_columns_size);
    const auto mapping = ReadMilvusTableFieldIdMapping(*column_groups);
    if (mapping != nullptr) {
        std::vector<std::shared_ptr<arrow::Field>> projected_fields;
        std::unordered_set<std::string> seen_physical;
        if (bound_columns->empty()) {
            projected_fields.reserve(arrow_schema->num_fields());
            seen_physical.reserve(arrow_schema->num_fields());
            for (const auto& field : arrow_schema->fields()) {
                const auto target_id = TryParseCanonicalFieldID(field->name());
                if (!target_id.has_value()) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Arrow schema field '{}' is not a canonical "
                              "field ID",
                              field->name());
                }
                const auto physical =
                    mapping->physical_columns.find(*target_id);
                if (physical == mapping->physical_columns.end()) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Arrow schema field '{}' is absent from field ID "
                              "mapping",
                              field->name());
                }
                if (!seen_physical.insert(physical->second).second) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Duplicate mapped physical column '{}'",
                              physical->second);
                }
                projected_fields.push_back(field->WithName(physical->second));
            }
        } else {
            std::unordered_map<std::string, std::shared_ptr<arrow::Field>>
                fields_by_name;
            fields_by_name.reserve(arrow_schema->num_fields());
            for (const auto& field : arrow_schema->fields()) {
                if (!fields_by_name.emplace(field->name(), field).second) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Duplicate field '{}' in Arrow schema",
                              field->name());
                }
            }
            projected_fields.reserve(bound_columns->size());
            seen_physical.reserve(bound_columns->size());
            for (auto& column : *bound_columns) {
                const auto target_id = TryParseCanonicalFieldID(column);
                if (!target_id.has_value()) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Requested target column '{}' is not a canonical "
                              "field ID",
                              column);
                }
                const auto physical =
                    mapping->physical_columns.find(*target_id);
                if (physical == mapping->physical_columns.end()) {
                    ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                              "Requested target column '{}' is absent from "
                              "field ID mapping",
                              column);
                }
                const auto field_it = fields_by_name.find(column);
                if (field_it != fields_by_name.end() &&
                    seen_physical.insert(physical->second).second) {
                    projected_fields.push_back(
                        field_it->second->WithName(physical->second));
                }
                column = physical->second;
            }
        }
        arrow_schema = arrow::schema(std::move(projected_fields),
                                     arrow_schema->metadata());
    }
    auto reader = milvus_storage::api::Reader::create(
        column_groups, arrow_schema, bound_columns, *properties);

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
NewPackedFFIReaderWithManifest(const LoonManifest* loon_manifest,
                               struct ArrowSchema* schema,
                               char** needed_columns,
                               int64_t needed_columns_size,
                               CFFIPackedReader* c_loon_reader,
                               CStorageConfig c_storage_config,
                               CPluginContext* c_plugin_context,
                               int64_t collection_id,
                               const char* external_source,
                               const char* external_spec) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        if (external_source != nullptr && external_source[0] != '\0') {
            InjectExternalSpecProperties(*properties,
                                         collection_id,
                                         std::string(external_source),
                                         external_spec != nullptr
                                             ? std::string(external_spec)
                                             : std::string());
        }
        auto column_groups =
            std::make_shared<milvus_storage::api::ColumnGroups>();
        auto status = milvus_storage::column_groups_import(
            &loon_manifest->column_groups, column_groups.get());
        if (!status.ok()) {
            auto error = milvus_storage::ToSegcoreError(status);
            return milvus::FailureCStatus(&error);
        }

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
NewPackedFFIReaderWithColumnGroups(const LoonColumnGroups* column_groups,
                                   struct ArrowSchema* schema,
                                   char** needed_columns,
                                   int64_t needed_columns_size,
                                   CFFIPackedReader* c_loon_reader,
                                   CStorageConfig c_storage_config,
                                   CPluginContext* c_plugin_context,
                                   int64_t collection_id,
                                   const char* external_source,
                                   const char* external_spec) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        if (external_source != nullptr && external_source[0] != '\0') {
            InjectExternalSpecProperties(*properties,
                                         collection_id,
                                         std::string(external_source),
                                         external_spec != nullptr
                                             ? std::string(external_spec)
                                             : std::string());
        }
        auto imported_column_groups =
            std::make_shared<milvus_storage::api::ColumnGroups>();
        auto status = milvus_storage::column_groups_import(
            column_groups, imported_column_groups.get());
        if (!status.ok()) {
            auto error = milvus_storage::ToSegcoreError(status);
            return milvus::FailureCStatus(&error);
        }

        auto reader = GetLoonReader(imported_column_groups,
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
        if (!result.ok()) {
            auto error = milvus_storage::ToSegcoreError(result.status());
            return milvus::FailureCStatus(&error);
        }

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
        if (c_packed_reader == nullptr) {
            return milvus::SuccessCStatus();
        }
        auto reader =
            static_cast<milvus_storage::api::Reader*>(c_packed_reader);
        AssertInfo(reader, "cannot close nullptr ffi reader");
        delete reader;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
