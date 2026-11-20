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
#include "fmt/core.h"
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

namespace {
std::shared_ptr<parquet::WriterProperties>
GetPackedWriterProperties(const CPluginContext* context) {
    parquet::WriterProperties::Builder builder;
    if (context == nullptr) {
        return builder.build();
    }
    auto cipher =
        milvus::storage::PluginLoader::GetInstance().getCipherPlugin();
    if (!cipher) {
        ThrowInfo(milvus::ErrorCode::Unsupported, "missing cipher plugin");
    }
    AssertInfo(context->key && *context->key, "missing writer keyset");
    std::pair<std::shared_ptr<milvus::storage::plugin::IEncryptor>, std::string>
        keys;
    try {
        keys = cipher->GetEncryptorForKeyset(
            context->ez_id, context->collection_id, context->key);
    } catch (const std::runtime_error&) {
        ThrowInfo(milvus::ErrorCode::UnexpectedError,
                  "cipher plugin rejected the writer keyset");
    }
    AssertInfo(keys.first != nullptr, "cipher plugin returned no encryptor");
    auto footer_key = keys.first->GetKey();
    auto length = footer_key.size();
    AssertInfo(!keys.second.empty() &&
                   keys.second.find('\0') == std::string::npos &&
                   (length == 16 || length == 24 || length == 32),
               "cipher plugin returned unusable encryption parameters");
    parquet::FileEncryptionProperties::Builder encryption(footer_key);
    auto metadata = milvus::storage::EncodeKeyMetadata(
        context->ez_id, context->collection_id, keys.second);
    builder.encryption(encryption.footer_key_metadata(metadata)
                           ->algorithm(parquet::ParquetCipher::AES_GCM_V1)
                           ->build());
    return builder.build();
}

CStatus
OpenPackedWriter(ArrowSchema* schema,
                 int64_t buffer_size,
                 char** paths,
                 int64_t num_paths,
                 int64_t part_upload_size,
                 CColumnGroups groups,
                 const CStorageConfig* config,
                 CPackedWriter* writer,
                 const CPluginContext* context) {
    try {
        auto fs = milvus::storage::GetPackedFileSystem(config);
        if (!fs) {
            return milvus::FailureCStatus(
                config != nullptr ? milvus::ErrorCode::FileReadFailed
                                  : milvus::ErrorCode::FileWriteFailed,
                "[StorageV2] Failed to get filesystem");
        }
        auto file_paths = std::vector<std::string>(paths, paths + num_paths);
        auto arrow_schema = arrow::ImportSchema(schema).ValueOrDie();
        auto& column_groups =
            *static_cast<std::vector<std::vector<int>>*>(groups);
        milvus_storage::StorageConfig storage_config;
        storage_config.part_size = part_upload_size;
        auto properties = GetPackedWriterProperties(context);
        auto result =
            milvus_storage::PackedRecordBatchWriter::Make(fs,
                                                          file_paths,
                                                          arrow_schema,
                                                          storage_config,
                                                          column_groups,
                                                          buffer_size,
                                                          properties);
        AssertInfo(result.ok(),
                   "[StorageV2] Failed to create packed writer: " +
                       result.status().ToString());
        *writer = new std::shared_ptr<milvus_storage::PackedRecordBatchWriter>(
            result.ValueOrDie());
        return milvus::SuccessCStatus();
    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
}  // namespace

CStatus
NewPackedWriterWithStorageConfig(ArrowSchema* schema,
                                 int64_t buffer_size,
                                 char** paths,
                                 int64_t num_paths,
                                 int64_t part_upload_size,
                                 CColumnGroups groups,
                                 CStorageConfig config,
                                 CPackedWriter* writer,
                                 CPluginContext* context) {
    SCOPE_CGO_CALL_METRIC();
    return OpenPackedWriter(schema,
                            buffer_size,
                            paths,
                            num_paths,
                            part_upload_size,
                            groups,
                            &config,
                            writer,
                            context);
}

CStatus
NewPackedWriter(ArrowSchema* schema,
                int64_t buffer_size,
                char** paths,
                int64_t num_paths,
                int64_t part_upload_size,
                CColumnGroups groups,
                CPackedWriter* writer,
                CPluginContext* context) {
    SCOPE_CGO_CALL_METRIC();
    return OpenPackedWriter(schema,
                            buffer_size,
                            paths,
                            num_paths,
                            part_upload_size,
                            groups,
                            nullptr,
                            writer,
                            context);
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
                    fmt::format("Failed to import array {}: {}",
                                i,
                                array.status().ToString()));
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
        auto trueFs = milvus::storage::GetPackedFileSystem(&c_storage_config);

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
