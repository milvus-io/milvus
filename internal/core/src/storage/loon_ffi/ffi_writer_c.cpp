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
#include <arrow/array.h>
#include <exception>
#include "log/Log.h"
#include "common/EasyAssert.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/transaction/transaction.h"
#include "milvus-storage/writer.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "common/common_type_c.h"
#include "milvus-storage/ffi_c.h"
#include "storage/loon_ffi/util.h"
#include "monitor/scope_metric.h"

CStatus
NewPackedLoonWriter(const char* base_path,
                    struct ArrowSchema* schema,
                    CStorageConfig c_storage_config,
                    const char* split_pattern,
                    LoonWriterHandler* writer_handle) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto schema_result = arrow::ImportSchema(schema);
        AssertInfo(schema_result.ok(), "Import arrow schema failed");
        auto arrow_schema = schema_result.ValueOrDie();

        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        if (split_pattern != nullptr) {
            auto result = milvus_storage::api::SetValue(
                *properties, "writer.policy", "schema_based", true);
            AssertInfo(result == std::nullopt, "Set writer.policy failed");
            result = milvus_storage::api::SetValue(
                *properties,
                "writer.split.schema_based.patterns",
                split_pattern,
                true);
            AssertInfo(result == std::nullopt,
                       "Set writer.schema_base_patterns failed, error {}",
                       result.value());
        }
        std::unique_ptr<milvus_storage::api::ColumnGroupPolicy> policy;

        auto policy_status =
            milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
                *properties, arrow_schema)
                .Value(&policy);
        AssertInfo(policy_status.ok(), "create column group policy failed");

        auto writer = milvus_storage::api::Writer::create(
            base_path, arrow_schema, std::move(policy), *properties);
        *writer_handle = writer.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
PackedLoonWrite(LoonWriterHandler writer_handle,
                struct ArrowArray* arrays,
                struct ArrowSchema* array_schemas,
                struct ArrowSchema* schema) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto writer = static_cast<milvus_storage::api::Writer*>(writer_handle);
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

        auto status = writer->write(record_batch);
        AssertInfo(
            status.ok(), "failed to write batch, error: {}", status.ToString());
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
ClosePackedLoonWriter(LoonWriterHandler writer_handle,
                      const char* base_path,
                      CStorageConfig c_storage_config,
                      int64_t* latest_version) {
    SCOPE_CGO_CALL_METRIC();
    try {
        auto writer = static_cast<milvus_storage::api::Writer*>(writer_handle);
        // delete writer;
        auto status = writer->flush();
        AssertInfo(status.ok(),
                   "failed to flush writer, error: {}",
                   status.ToString());

        auto result = writer->close();
        AssertInfo(result.ok(), "failed to close writer");

        auto cgs = result.ValueOrDie();

        auto manifest_content = cgs->serialize().ValueOrDie();

        delete writer;

        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        AssertInfo(properties != nullptr, "properties is null");

        // commit first
        {
            auto transaction = std::make_unique<
                milvus_storage::api::transaction::TransactionImpl<
                    milvus_storage::api::ColumnGroups>>(*properties, base_path);
            auto status = transaction->begin();
            AssertInfo(status.ok(),
                       "failed to begin transaction, error: {}",
                       status.ToString());

            auto commit_result = transaction->commit(
                cgs,
                milvus_storage::api::transaction::UpdateType::APPENDFILES,
                milvus_storage::api::transaction::TransResolveStrategy::
                    RESOLVE_FAIL);

            AssertInfo(commit_result.ok(),
                       "failed to commit transaction, error: {}",
                       commit_result.status().ToString());

            AssertInfo(commit_result.ValueOrDie(),
                       "failed to commit transaction, error: {}",
                       commit_result.status().ToString());
        }

        // get latest version, temp solution here
        {
            auto transaction = std::make_unique<
                milvus_storage::api::transaction::TransactionImpl<
                    milvus_storage::api::ColumnGroups>>(*properties, base_path);
            auto latest_manifest_result = transaction->get_latest_manifest();
            AssertInfo(latest_manifest_result.ok(),
                       "failed to get latest manifest, error: {}",
                       latest_manifest_result.status().ToString());
            *latest_version = transaction->read_version();
            LOG_WARN(
                "Loon writer transaction(base path {}) succeeded with latest "
                "version: {}",
                base_path,
                *latest_version);
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}