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

#include "common/common_type_c.h"
#include "parquet/encryption/encryption.h"
#include "parquet/properties.h"
#include "parquet/types.h"
#include "segcore/column_groups_c.h"
#include "segcore/packed_writer_c.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/PluginLoader.h"
#include "storage/KeyRetriever.h"
#include "storage/StorageV2FSCache.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/memory_pool.h>
#include <arrow/device.h>
#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "monitor/scope_metric.h"

CStatus
NewPackedWriterWithStorageConfig(struct ArrowSchema* schema,
                                 const int64_t buffer_size,
                                 char** paths,
                                 int64_t num_paths,
                                 int64_t part_upload_size,
                                 CColumnGroups column_groups,
                                 CStorageConfig c_storage_config,
                                 CPackedWriter* c_packed_writer,
                                 CPluginContext* c_plugin_context) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto truePaths = std::vector<std::string>(paths, paths + num_paths);

        auto storage_config = milvus_storage::StorageConfig();
        storage_config.part_size = part_upload_size;

        auto trueFs = milvus::storage::StorageV2FSCache::Instance().Get({
            std::string(c_storage_config.address),
            std::string(c_storage_config.bucket_name),
            std::string(c_storage_config.access_key_id),
            std::string(c_storage_config.access_key_value),
            std::string(c_storage_config.root_path),
            std::string(c_storage_config.storage_type),
            std::string(c_storage_config.cloud_provider),
            std::string(c_storage_config.iam_endpoint),
            std::string(c_storage_config.log_level),
            std::string(c_storage_config.region),
            c_storage_config.useSSL,
            std::string(c_storage_config.sslCACert),
            c_storage_config.useIAM,
            c_storage_config.useVirtualHost,
            c_storage_config.requestTimeoutMs,
            false,
            std::string(c_storage_config.gcp_credential_json),
            c_storage_config.use_custom_part_upload,
            c_storage_config.max_connections,
        });
        if (!trueFs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get filesystem");
        }

        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();

        auto columnGroups =
            *static_cast<std::vector<std::vector<int>>*>(column_groups);

        parquet::WriterProperties::Builder builder;
        auto plugin_ptr =
            milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
        if (plugin_ptr != nullptr && c_plugin_context != nullptr) {
            plugin_ptr->Update(c_plugin_context->ez_id,
                               c_plugin_context->collection_id,
                               std::string(c_plugin_context->key));
            auto got = plugin_ptr->GetEncryptor(
                c_plugin_context->ez_id, c_plugin_context->collection_id);
            parquet::FileEncryptionProperties::Builder file_encryption_builder(
                got.first->GetKey());
            auto metadata = milvus::storage::EncodeKeyMetadata(
                c_plugin_context->ez_id,
                c_plugin_context->collection_id,
                got.second);

            builder.encryption(
                file_encryption_builder.footer_key_metadata(metadata)
                    ->algorithm(parquet::ParquetCipher::AES_GCM_V1)
                    ->build());
        }

        auto writer_properties = builder.build();
        auto result =
            milvus_storage::PackedRecordBatchWriter::Make(trueFs,
                                                          truePaths,
                                                          trueSchema,
                                                          storage_config,
                                                          columnGroups,
                                                          buffer_size,
                                                          writer_properties);
        AssertInfo(result.ok(),
                   "[StorageV2] Failed to create packed writer: " +
                       result.status().ToString());
        auto writer = result.ValueOrDie();
        *c_packed_writer =
            new std::shared_ptr<milvus_storage::PackedRecordBatchWriter>(
                std::move(writer));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
NewPackedWriter(struct ArrowSchema* schema,
                const int64_t buffer_size,
                char** paths,
                int64_t num_paths,
                int64_t part_upload_size,
                CColumnGroups column_groups,
                CPackedWriter* c_packed_writer,
                CPluginContext* c_plugin_context) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto truePaths = std::vector<std::string>(paths, paths + num_paths);

        auto conf = milvus_storage::StorageConfig();
        conf.part_size = part_upload_size;

        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                          .GetArrowFileSystem();
        if (!trueFs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileWriteFailed,
                "[StorageV2] Failed to get filesystem");
        }

        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();

        auto columnGroups =
            *static_cast<std::vector<std::vector<int>>*>(column_groups);

        parquet::WriterProperties::Builder builder;
        auto plugin_ptr =
            milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
        if (plugin_ptr != nullptr && c_plugin_context != nullptr) {
            plugin_ptr->Update(c_plugin_context->ez_id,
                               c_plugin_context->collection_id,
                               std::string(c_plugin_context->key));

            auto got = plugin_ptr->GetEncryptor(
                c_plugin_context->ez_id, c_plugin_context->collection_id);
            parquet::FileEncryptionProperties::Builder file_encryption_builder(
                got.first->GetKey());
            auto metadata = milvus::storage::EncodeKeyMetadata(
                c_plugin_context->ez_id,
                c_plugin_context->collection_id,
                got.second);
            builder.encryption(
                file_encryption_builder.footer_key_metadata(metadata)
                    ->algorithm(parquet::ParquetCipher::AES_GCM_V1)
                    ->build());
        }

        auto writer_properties = builder.build();
        auto result =
            milvus_storage::PackedRecordBatchWriter::Make(trueFs,
                                                          truePaths,
                                                          trueSchema,
                                                          conf,
                                                          columnGroups,
                                                          buffer_size,
                                                          writer_properties);
        AssertInfo(result.ok(),
                   "[StorageV2] Failed to create packed writer: " +
                       result.status().ToString());
        auto writer = result.ValueOrDie();
        *c_packed_writer =
            new std::shared_ptr<milvus_storage::PackedRecordBatchWriter>(
                std::move(writer));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
WriteRecordBatch(CPackedWriter c_packed_writer,
                 struct ArrowArray* arrays,
                 struct ArrowSchema* array_schemas,
                 struct ArrowSchema* schema) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto packed_writer = *static_cast<
            std::shared_ptr<milvus_storage::PackedRecordBatchWriter>*>(
            c_packed_writer);

        auto import_schema = arrow::ImportSchema(schema);
        if (!import_schema.ok()) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileWriteFailed,
                "Failed to import schema: " +
                    import_schema.status().ToString());
        }
        auto arrow_schema = import_schema.ValueOrDie();

        int num_fields = arrow_schema->num_fields();
        std::vector<std::shared_ptr<arrow::Array>> all_arrays;
        all_arrays.reserve(num_fields);

        for (int i = 0; i < num_fields; i++) {
            auto array = arrow::ImportArray(&arrays[i], &array_schemas[i]);
            if (!array.ok()) {
                return milvus::FailureCStatus(
                    milvus::ErrorCode::FileWriteFailed,
                    "Failed to import array " + std::to_string(i) + ": " +
                        array.status().ToString());
            }
            all_arrays.push_back(array.ValueOrDie());
        }

        auto record_batch = arrow::RecordBatch::Make(
            arrow_schema, all_arrays[0]->length(), all_arrays);

        auto status = packed_writer->Write(record_batch);
        if (!status.ok()) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed,
                                          status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloseWriter(CPackedWriter c_packed_writer) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto packed_writer = static_cast<
            std::shared_ptr<milvus_storage::PackedRecordBatchWriter>*>(
            c_packed_writer);
        auto status = (*packed_writer)->Close();
        delete packed_writer;
        if (!status.ok()) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed,
                                          status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
GetFileSize(const char* path, int64_t* size) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                          .GetArrowFileSystem();
        if (!trueFs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get filesystem");
        }

        auto result = trueFs->GetFileInfo(path);
        if (!result.ok()) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get file info");
        }
        *size = result.ValueOrDie().size();

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
GetFileSizeWithStorageConfig(const char* path,
                             int64_t* size,
                             CStorageConfig c_storage_config) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto trueFs = milvus::storage::StorageV2FSCache::Instance().Get({
            std::string(c_storage_config.address),
            std::string(c_storage_config.bucket_name),
            std::string(c_storage_config.access_key_id),
            std::string(c_storage_config.access_key_value),
            std::string(c_storage_config.root_path),
            std::string(c_storage_config.storage_type),
            std::string(c_storage_config.cloud_provider),
            std::string(c_storage_config.iam_endpoint),
            std::string(c_storage_config.log_level),
            std::string(c_storage_config.region),
            c_storage_config.useSSL,
            std::string(c_storage_config.sslCACert),
            c_storage_config.useIAM,
            c_storage_config.useVirtualHost,
            c_storage_config.requestTimeoutMs,
            false,
            std::string(c_storage_config.gcp_credential_json),
            c_storage_config.use_custom_part_upload,
            c_storage_config.max_connections,
        });

        if (!trueFs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get filesystem");
        }

        auto result = trueFs->GetFileInfo(path);
        if (!result.ok()) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get file info");
        }
        *size = result.ValueOrDie().size();

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}