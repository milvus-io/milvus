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

#include "segcore/column_groups_c.h"
#include "segcore/packed_writer_c.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/memory_pool.h>
#include <arrow/device.h>
#include <cstring>
#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "monitor/scope_metric.h"
#include "log/Log.h"

// Deep copy ArrowArray and return a copied RecordBatch
// This function creates a complete deep copy of the ArrowArray and returns it as a RecordBatch
static std::shared_ptr<arrow::RecordBatch>
ArrowArrayDeepCopyToRecordBatch(const struct ArrowArray* src,
                                const struct ArrowSchema* schema) {
    if (src == nullptr || schema == nullptr) {
        return nullptr;  // Invalid arguments
    }

    try {
        // Import the record batch from the original array using the copied schema
        auto record_batch =
            arrow::ImportRecordBatch(const_cast<struct ArrowArray*>(src),
                                     const_cast<struct ArrowSchema*>(schema))
                .ValueOrDie();

        // Get the default CPU memory manager for deep copy
        auto memory_manager = arrow::default_cpu_memory_manager();

        // For true deep copy, we'll use Arrow's CopyTo() function
        std::vector<std::shared_ptr<arrow::Array>> copied_arrays;
        for (int i = 0; i < record_batch->num_columns(); i++) {
            auto original_array = record_batch->column(i);

            // Use Arrow's CopyTo() function for true deep copy
            auto copied_data_result =
                original_array->data()->CopyTo(memory_manager);
            if (!copied_data_result.ok()) {
                LOG_ERROR("[StorageV2] Failed to deep copy array data: {}",
                          copied_data_result.status().ToString());
                return nullptr;
            }
            auto copied_data = copied_data_result.ValueOrDie();
            auto copied_array = arrow::MakeArray(copied_data);
            copied_arrays.push_back(copied_array);
        }

        // Create and return a new RecordBatch with the truly copied arrays
        return arrow::RecordBatch::Make(
            record_batch->schema(), record_batch->num_rows(), copied_arrays);

    } catch (const std::exception& e) {
        LOG_ERROR("[StorageV2] Exception during deep copy: {}", e.what());
        return nullptr;  // Arrow operation failed
    }
}

CStatus
NewPackedWriterWithStorageConfig(struct ArrowSchema* schema,
                                 const int64_t buffer_size,
                                 char** paths,
                                 int64_t num_paths,
                                 int64_t part_upload_size,
                                 CColumnGroups column_groups,
                                 CStorageConfig c_storage_config,
                                 CPackedWriter* c_packed_writer) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto truePaths = std::vector<std::string>(paths, paths + num_paths);

        auto storage_config = milvus_storage::StorageConfig();
        storage_config.part_size = part_upload_size;

        milvus_storage::ArrowFileSystemConfig conf;
        conf.address = std::string(c_storage_config.address);
        conf.bucket_name = std::string(c_storage_config.bucket_name);
        conf.access_key_id = std::string(c_storage_config.access_key_id);
        conf.access_key_value = std::string(c_storage_config.access_key_value);
        conf.root_path = std::string(c_storage_config.root_path);
        conf.storage_type = std::string(c_storage_config.storage_type);
        conf.cloud_provider = std::string(c_storage_config.cloud_provider);
        conf.iam_endpoint = std::string(c_storage_config.iam_endpoint);
        conf.log_level = std::string(c_storage_config.log_level);
        conf.region = std::string(c_storage_config.region);
        conf.useSSL = c_storage_config.useSSL;
        conf.sslCACert = std::string(c_storage_config.sslCACert);
        conf.useIAM = c_storage_config.useIAM;
        conf.useVirtualHost = c_storage_config.useVirtualHost;
        conf.requestTimeoutMs = c_storage_config.requestTimeoutMs;
        conf.gcp_credential_json =
            std::string(c_storage_config.gcp_credential_json);
        conf.use_custom_part_upload = c_storage_config.use_custom_part_upload;
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                          .GetArrowFileSystem();
        if (!trueFs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get filesystem");
        }

        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();

        auto columnGroups =
            *static_cast<std::vector<std::vector<int>>*>(column_groups);

        auto writer = std::make_unique<milvus_storage::PackedRecordBatchWriter>(
            trueFs,
            truePaths,
            trueSchema,
            storage_config,
            columnGroups,
            buffer_size);
        AssertInfo(writer, "[StorageV2] writer pointer is null");

        *c_packed_writer = writer.release();
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
                CPackedWriter* c_packed_writer) {
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

        auto writer = std::make_unique<milvus_storage::PackedRecordBatchWriter>(
            trueFs, truePaths, trueSchema, conf, columnGroups, buffer_size);
        AssertInfo(writer, "[StorageV2] writer pointer is null");

        *c_packed_writer = writer.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
WriteRecordBatch(CPackedWriter c_packed_writer,
                 struct ArrowArray* array,
                 struct ArrowSchema* schema) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto packed_writer =
            static_cast<milvus_storage::PackedRecordBatchWriter*>(
                c_packed_writer);

        // Deep copy the ArrowArray and get a copied RecordBatch
        auto record_batch = ArrowArrayDeepCopyToRecordBatch(array, schema);
        if (record_batch == nullptr) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed,
                                          "Failed to copy ArrowArray");
        }
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
        auto packed_writer =
            static_cast<milvus_storage::PackedRecordBatchWriter*>(
                c_packed_writer);
        auto status = packed_writer->Close();
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