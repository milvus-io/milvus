// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/record_batch.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "milvus-storage/transaction/transaction.h"
#include "milvus-storage/writer.h"
#include "DataGen.h"

namespace milvus::test {

// Generate a schema_based column group pattern from a Schema.
// Mirrors Go's SelectedDataTypePolicy + RemanentShortPolicy:
//   - Each vector field -> its own group
//   - All scalar fields -> one group
// Pattern format: groups separated by ",", columns within a group by "|".
inline std::string
GenerateColumnGroupPattern(const SchemaPtr& schema) {
    std::vector<std::string> scalar_ids;
    std::vector<std::string> vector_groups;

    for (const auto& field_id : schema->get_field_ids()) {
        const auto& meta = (*schema)[field_id];
        auto id_str = std::to_string(field_id.get());
        if (IsVectorDataType(meta.get_data_type())) {
            vector_groups.push_back(id_str);
        } else {
            scalar_ids.push_back(id_str);
        }
    }

    std::string pattern;
    if (!scalar_ids.empty()) {
        for (size_t i = 0; i < scalar_ids.size(); ++i) {
            if (i > 0) {
                pattern += "|";
            }
            pattern += scalar_ids[i];
        }
    }
    for (const auto& vg : vector_groups) {
        if (!pattern.empty()) {
            pattern += ",";
        }
        pattern += vg;
    }
    return pattern;
}

// Generates V3 segment data using real milvus-storage APIs.
// Writes actual Parquet files + manifest to base_path.
class V3SegmentTestData {
 public:
    V3SegmentTestData(const SchemaPtr& schema,
                      int64_t n_batch,
                      int64_t per_batch,
                      int64_t dim,
                      const std::string& base_path)
        : schema_(schema), base_path_(base_path) {
        std::filesystem::create_directories(base_path_);

        // Convert schema to Arrow schemas
        auto arrow_schema = schema_->ConvertToArrowSchema();
        loon_schema_ = schema_->ConvertToLoonArrowSchema();

        // Generate data batches and remap to Loon schema
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.reserve(n_batch);
        for (int64_t i = 0; i < n_batch; ++i) {
            auto dataset = milvus::segcore::DataGen(schema_, per_batch, 42 + i);
            auto batch = milvus::segcore::ConvertToArrowRecordBatch(
                dataset, dim, arrow_schema);
            // Remap to Loon schema (field IDs as column names).
            // Both schemas iterate field_ids_ in the same order, so
            // columns align directly.
            auto loon_batch = arrow::RecordBatch::Make(
                loon_schema_, batch->num_rows(), batch->columns());
            batches.push_back(std::move(loon_batch));
        }
        total_rows_ = n_batch * per_batch;

        // Set up Writer properties with schema_based column group policy
        auto pattern = GenerateColumnGroupPattern(schema_);
        milvus_storage::api::Properties props;
        milvus_storage::api::SetValue(
            props, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
        milvus_storage::api::SetValue(props,
                                      PROPERTY_WRITER_POLICY,
                                      LOON_COLUMN_GROUP_POLICY_SCHEMA_BASED);
        milvus_storage::api::SetValue(
            props, PROPERTY_WRITER_SCHEMA_BASE_PATTERNS, pattern.c_str());

        auto policy_result =
            milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
                props, loon_schema_);
        AssertInfo(policy_result.ok(),
                   "Failed to create column group policy: {}",
                   policy_result.status().ToString());
        auto policy = std::move(policy_result).ValueOrDie();

        // Write data using Writer API
        auto writer = milvus_storage::api::Writer::create(
            base_path_, loon_schema_, std::move(policy), props);
        for (auto& batch : batches) {
            auto status = writer->write(batch);
            AssertInfo(
                status.ok(), "Failed to write batch: {}", status.ToString());
        }
        auto close_result = writer->close();
        AssertInfo(close_result.ok(),
                   "Failed to close writer: {}",
                   close_result.status().ToString());
        auto column_groups = std::move(close_result).ValueOrDie();

        // Commit manifest via Transaction
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        auto txn_result =
            milvus_storage::api::transaction::Transaction::Open(fs, base_path_);
        AssertInfo(txn_result.ok(),
                   "Failed to open transaction: {}",
                   txn_result.status().ToString());
        auto txn = std::move(txn_result).ValueOrDie();
        txn->AppendFiles(*column_groups);
        auto commit_result = txn->Commit();
        AssertInfo(commit_result.ok(),
                   "Failed to commit transaction: {}",
                   commit_result.status().ToString());
        auto version = commit_result.ValueOrDie();

        // Read back from manifest to get the committed column groups
        auto read_txn_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs, base_path_, version);
        AssertInfo(read_txn_result.ok(),
                   "Failed to open read transaction: {}",
                   read_txn_result.status().ToString());
        auto read_txn = std::move(read_txn_result).ValueOrDie();
        auto manifest_result = read_txn->GetManifest();
        AssertInfo(manifest_result.ok(),
                   "Failed to get manifest: {}",
                   manifest_result.status().ToString());
        auto manifest = manifest_result.ValueOrDie();
        column_groups_ = std::make_shared<milvus_storage::api::ColumnGroups>(
            manifest->columnGroups());
    }

    ~V3SegmentTestData() {
        if (std::filesystem::exists(base_path_)) {
            std::filesystem::remove_all(base_path_);
        }
    }

    // Non-copyable, non-movable
    V3SegmentTestData(const V3SegmentTestData&) = delete;
    V3SegmentTestData&
    operator=(const V3SegmentTestData&) = delete;
    V3SegmentTestData(V3SegmentTestData&&) = delete;
    V3SegmentTestData&
    operator=(V3SegmentTestData&&) = delete;

    std::unique_ptr<milvus_storage::api::ChunkReader>
    CreateChunkReader(int64_t cg_index) const {
        auto reader =
            milvus_storage::api::Reader::create(column_groups_, loon_schema_);
        auto result = reader->get_chunk_reader(cg_index);
        AssertInfo(result.ok(),
                   "Failed to create chunk reader for cg_index {}: {}",
                   cg_index,
                   result.status().ToString());
        return std::move(result).ValueOrDie();
    }

    std::unordered_map<FieldId, FieldMeta>
    GetFieldMetas(int64_t cg_index) const {
        std::unordered_map<FieldId, FieldMeta> result;
        const auto& cg = column_groups_->at(cg_index);
        for (const auto& col_name : cg->columns) {
            auto fid = FieldId(std::stoll(col_name));
            result.emplace(fid, (*schema_)[fid]);
        }
        return result;
    }

    std::unordered_map<FieldId, FieldMeta>
    GetAllFieldMetas() const {
        return schema_->get_fields();
    }

    std::shared_ptr<arrow::Schema>
    GetLoonSchema() const {
        return loon_schema_;
    }

    std::shared_ptr<milvus_storage::api::ColumnGroups>
    GetColumnGroups() const {
        return column_groups_;
    }

    int64_t
    TotalRows() const {
        return total_rows_;
    }

    int64_t
    NumColumnGroups() const {
        return static_cast<int64_t>(column_groups_->size());
    }

    const std::string&
    BasePath() const {
        return base_path_;
    }

 private:
    SchemaPtr schema_;
    std::shared_ptr<arrow::Schema> loon_schema_;
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups_;
    std::string base_path_;
    int64_t total_rows_ = 0;
};

}  // namespace milvus::test
