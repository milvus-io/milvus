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

#include "storage/loon_ffi/util.h"
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "segcore/default_fs.h"

#include "arrow/record_batch.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
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

// Int64ColumnFromValues builds an arrow::int64() column. Used for the RowID,
// Timestamp and primary-key columns every StorageV3 fixture needs.
inline std::shared_ptr<arrow::Array>
Int64ColumnFromValues(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    auto status = builder.AppendValues(values);
    AssertInfo(
        status.ok(), "Failed to append int64 values: {}", status.ToString());
    std::shared_ptr<arrow::Array> out;
    status = builder.Finish(&out);
    AssertInfo(
        status.ok(), "Failed to finish int64 builder: {}", status.ToString());
    return out;
}

// ArrayFromScalarFieldProtos encodes ARRAY / nested-ARRAY rows the way the
// storage layer expects them: an arrow::binary() column whose every value is a
// serialized ScalarFieldProto. This is the encoding ChunkWriter parses back, so
// a proto written here round-trips to an identical proto on read.
inline std::shared_ptr<arrow::Array>
ArrayFromScalarFieldProtos(const std::vector<ScalarFieldProto>& rows,
                           const std::vector<bool>& valid = {}) {
    AssertInfo(valid.empty() || valid.size() == rows.size(),
               "valid bitmap size {} does not match row count {}",
               valid.size(),
               rows.size());
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (!valid.empty() && !valid[i]) {
            auto status = builder.AppendNull();
            AssertInfo(
                status.ok(), "Failed to append null: {}", status.ToString());
            continue;
        }
        auto serialized = rows[i].SerializeAsString();
        auto status = builder.Append(serialized.data(), serialized.size());
        AssertInfo(
            status.ok(), "Failed to append array row: {}", status.ToString());
    }
    std::shared_ptr<arrow::Array> out;
    auto status = builder.Finish(&out);
    AssertInfo(
        status.ok(), "Failed to finish array builder: {}", status.ToString());
    return out;
}

// Generates V3 segment data using real milvus-storage APIs.
// Writes actual Parquet files + manifest to base_path.
class V3SegmentTestData {
 public:
    V3SegmentTestData(const SchemaPtr& schema,
                      int64_t n_batch,
                      int64_t per_batch,
                      int64_t dim,
                      const std::string& root_path,
                      const std::string& base_path)
        : schema_(schema),
          // Keys are complete paths: the loon local filesystem is rooted at
          // "/" and base_path lives under root_path (see LoonFSRootPath).
          base_path_((std::filesystem::path(root_path) / base_path).string()),
          root_path_(root_path) {
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

        writeAndCommit(batches, GenerateColumnGroupPattern(schema_));
    }

    // Caller-supplied-columns constructor.
    //
    // Writes exactly the given columns, in the order
    // write_schema->ConvertToLoonArrowSchema() defines, and commits a manifest.
    // No growing segment is involved. This replaces the former practice of
    // inserting into a throwaway growing segment purely to flush it through the
    // FlushGrowingSegmentData C API.
    //
    //   columns         one arrow array per field of write_schema, same order.
    //   pattern         writer.split.schema_based.patterns, e.g. "0|1|100,101".
    //                   Each comma-separated element is a std::regex matched
    //                   against the column name, so "|" is regex alternation.
    //                   Empty falls back to the schema-derived pattern.
    //   rows_per_batch  <= 0 writes one batch; otherwise the columns are sliced
    //                   into batches of this many rows. Use it to reproduce
    //                   batch boundaries that previously came from forcing
    //                   SegcoreConfig chunk_rows on the growing segment.
    V3SegmentTestData(const SchemaPtr& write_schema,
                      const std::vector<std::shared_ptr<arrow::Array>>& columns,
                      int64_t num_rows,
                      const std::string& root_path,
                      const std::string& base_path,
                      const std::string& pattern = "",
                      int64_t rows_per_batch = 0)
        : schema_(write_schema),
          base_path_((std::filesystem::path(root_path) / base_path).string()),
          root_path_(root_path) {
        std::filesystem::create_directories(base_path_);
        loon_schema_ = schema_->ConvertToLoonArrowSchema();

        AssertInfo(
            static_cast<int>(columns.size()) == loon_schema_->num_fields(),
            "column count {} does not match loon schema field count {}",
            columns.size(),
            loon_schema_->num_fields());
        for (size_t i = 0; i < columns.size(); ++i) {
            AssertInfo(columns[i] != nullptr, "column {} is null", i);
            AssertInfo(columns[i]->length() == num_rows,
                       "column {} has {} rows, expected {}",
                       i,
                       columns[i]->length(),
                       num_rows);
        }

        const int64_t batch_rows =
            rows_per_batch > 0 ? rows_per_batch : num_rows;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (int64_t offset = 0; offset < num_rows; offset += batch_rows) {
            const int64_t len = std::min(batch_rows, num_rows - offset);
            std::vector<std::shared_ptr<arrow::Array>> sliced;
            sliced.reserve(columns.size());
            for (const auto& column : columns) {
                sliced.push_back(column->Slice(offset, len));
            }
            batches.push_back(
                arrow::RecordBatch::Make(loon_schema_, len, sliced));
        }
        // A zero-row segment still needs one empty batch so the writer emits a
        // column group per pattern rather than nothing at all.
        if (batches.empty()) {
            batches.push_back(
                arrow::RecordBatch::Make(loon_schema_, 0, columns));
        }
        total_rows_ = num_rows;

        writeAndCommit(
            batches,
            pattern.empty() ? GenerateColumnGroupPattern(schema_) : pattern);
    }

    ~V3SegmentTestData() {
        // if (std::filesystem::exists(base_path_)) {
        //     std::filesystem::remove_all(base_path_);
        // }
    }

    // Non-copyable, non-movable
    V3SegmentTestData(const V3SegmentTestData&) = delete;
    V3SegmentTestData&
    operator=(const V3SegmentTestData&) = delete;
    V3SegmentTestData(V3SegmentTestData&&) = delete;
    V3SegmentTestData&
    operator=(V3SegmentTestData&&) = delete;

    std::unique_ptr<milvus_storage::api::ChunkReader>
    CreateChunkReader(int64_t cg_index,
                      const std::shared_ptr<std::vector<std::string>>&
                          needed_columns = nullptr) const {
        milvus_storage::api::Properties reader_props;
        milvus_storage::api::SetValue(
            reader_props, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
        milvus_storage::api::SetValue(
            reader_props, PROPERTY_FS_ROOT_PATH, kLoonLocalFSRootPath);
        auto reader = milvus_storage::api::Reader::create(
            column_groups_, loon_schema_, needed_columns, reader_props);
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

    int64_t
    Version() const {
        return committed_version_;
    }

    std::string
    ManifestPathJson() const {
        return "{\"base_path\":\"" + base_path_ +
               "\",\"ver\":" + std::to_string(committed_version_) + "}";
    }

 private:
    // writeAndCommit writes the batches as one StorageV3 segment under
    // base_path_ and commits a manifest, then reads the committed column
    // groups back. Shared by both constructors so the generated-data and
    // caller-supplied-data paths cannot drift.
    void
    writeAndCommit(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        const std::string& pattern) {
        // Set up Writer properties with schema_based column group policy
        // pattern is supplied by the caller.
        milvus_storage::api::Properties props;
        milvus_storage::api::SetValue(
            props, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
        milvus_storage::api::SetValue(
            props, PROPERTY_FS_ROOT_PATH, kLoonLocalFSRootPath);
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
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
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
        committed_version_ = commit_result.ValueOrDie();

        // Read back from manifest to get the committed column groups
        auto read_txn_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs, base_path_, committed_version_);
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

    SchemaPtr schema_;
    std::shared_ptr<arrow::Schema> loon_schema_;
    std::shared_ptr<milvus_storage::api::ColumnGroups> column_groups_;
    std::string base_path_;
    std::string root_path_;
    int64_t total_rows_ = 0;
    int64_t committed_version_ = 0;
};

}  // namespace milvus::test
