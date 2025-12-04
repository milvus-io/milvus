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

#include "index/json_stats/parquet_writer.h"
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace milvus::index {

JsonStatsParquetWriter::JsonStatsParquetWriter(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    milvus_storage::StorageConfig& storage_config,
    size_t buffer_size,
    size_t batch_size,
    std::shared_ptr<parquet::WriterProperties> writer_props)
    : fs_(fs),
      storage_config_(storage_config),
      buffer_size_(buffer_size),
      batch_size_(batch_size),
      writer_props_(writer_props),
      unflushed_row_count_(0),
      all_row_count_(0) {
}

JsonStatsParquetWriter::~JsonStatsParquetWriter() {
    Close();
}

void
JsonStatsParquetWriter::Close() {
    // check packed_writer_ initialized
    if (packed_writer_) {
        Flush();
        // ignore close status here
        auto _ = packed_writer_->Close();
    }
}

void
JsonStatsParquetWriter::Flush() {
    WriteCurrentBatch();
}

void
JsonStatsParquetWriter::UpdatePathSizeMap(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
    auto index = 0;
    for (const auto& group : column_groups_) {
        auto path = file_paths_[index];
        for (const auto& column_index : group) {
            size_t size = GetArrowArrayMemorySize(arrays[column_index]);
            path_size_map_[path] += size;
            total_size_ += size;
        }
        index++;
    }
}

size_t
JsonStatsParquetWriter::WriteCurrentBatch() {
    if (unflushed_row_count_ == 0) {
        return 0;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders_) {
        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);
        AssertInfo(
            status.ok(), "failed to finish builder: {}", status.ToString());
        arrays.push_back(array);
        builder->Reset();
    }

    UpdatePathSizeMap(arrays);

    auto batch =
        arrow::RecordBatch::Make(schema_, unflushed_row_count_, arrays);
    auto status = packed_writer_->Write(batch);
    AssertInfo(
        status.ok(), "failed to write batch, error: {}", status.ToString());

    auto res = unflushed_row_count_;
    unflushed_row_count_ = 0;
    return res;
}

void
JsonStatsParquetWriter::Init(const ParquetWriteContext& context) {
    schema_ = context.schema;
    builders_ = context.builders;
    builders_map_ = context.builders_map;
    kv_metadata_ = context.kv_metadata;
    column_groups_ = context.column_groups;
    file_paths_ = context.file_paths;
    auto result = milvus_storage::PackedRecordBatchWriter::Make(fs_,
                                                                file_paths_,
                                                                schema_,
                                                                storage_config_,
                                                                column_groups_,
                                                                buffer_size_);
    AssertInfo(result.ok(),
               "[StorageV2] Failed to create packed writer: " +
                   result.status().ToString());
    packed_writer_ = result.ValueOrDie();
    for (const auto& [key, value] : kv_metadata_) {
        packed_writer_->AddUserMetadata(key, value);
    }
}

size_t
JsonStatsParquetWriter::AddCurrentRow() {
    unflushed_row_count_++;
    all_row_count_++;
    if (unflushed_row_count_ >= batch_size_) {
        WriteCurrentBatch();
    }
    return all_row_count_;
}

void
JsonStatsParquetWriter::AppendSharedRow(const uint8_t* data, size_t length) {
    auto builder = builders_.at(builders_.size() - 1);
    auto shared_builder =
        std::static_pointer_cast<arrow::BinaryBuilder>(builder);

    if (length == 0) {
        auto status = shared_builder->AppendNull();
        AssertInfo(status.ok(), "failed to append null data");
    } else {
        auto status = shared_builder->Append(data, length);
        AssertInfo(status.ok(), "failed to append binary data");
    }
}

void
JsonStatsParquetWriter::AppendValue(const std::string& key,
                                    const std::string& value) {
    auto it = builders_map_.find(key);
    if (it == builders_map_.end()) {
        ThrowInfo(
            ErrorCode::UnexpectedError, "builder for key {} not found", key);
    }

    auto builder = it->second;
    auto ast = AppendDataToBuilder(value, builder);
    AssertInfo(ast.ok(), "failed to append data to builder");
}

void
JsonStatsParquetWriter::AppendRow(
    const std::map<std::string, std::string>& row_data) {
    for (const auto& [key, value] : row_data) {
        auto it = builders_map_.find(key);
        if (it == builders_map_.end()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "builder for key {} not found",
                      key);
        }

        auto builder = it->second;
        auto status = AppendDataToBuilder(value, builder);
        AssertInfo(status.ok(), "failed to append data to builder");
    }

    AddCurrentRow();
}

arrow::Status
JsonStatsParquetWriter::AppendDataToBuilder(
    const std::string& value, std::shared_ptr<arrow::ArrayBuilder> builder) {
    auto type_id = builder->type()->id();

    if (value.empty()) {
        return builder->AppendNull();
    }

    arrow::Status ast;
    try {
        switch (type_id) {
            case arrow::Type::INT8: {
                auto int8_builder =
                    std::static_pointer_cast<arrow::Int8Builder>(builder);
                ast = int8_builder->Append(ConvertValue<int8_t>(value));
                break;
            }
            case arrow::Type::INT16: {
                auto int16_builder =
                    std::static_pointer_cast<arrow::Int16Builder>(builder);
                ast = int16_builder->Append(ConvertValue<int16_t>(value));
                break;
            }
            case arrow::Type::INT32: {
                auto int32_builder =
                    std::static_pointer_cast<arrow::Int32Builder>(builder);
                ast = int32_builder->Append(ConvertValue<int32_t>(value));
                break;
            }
            case arrow::Type::INT64: {
                auto int64_builder =
                    std::static_pointer_cast<arrow::Int64Builder>(builder);
                ast = int64_builder->Append(ConvertValue<int64_t>(value));
                break;
            }
            case arrow::Type::FLOAT: {
                auto float_builder =
                    std::static_pointer_cast<arrow::FloatBuilder>(builder);
                ast = float_builder->Append(ConvertValue<float>(value));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto double_builder =
                    std::static_pointer_cast<arrow::DoubleBuilder>(builder);
                ast = double_builder->Append(ConvertValue<double>(value));
                break;
            }
            case arrow::Type::BOOL: {
                auto bool_builder =
                    std::static_pointer_cast<arrow::BooleanBuilder>(builder);
                ast = bool_builder->Append(ConvertValue<bool>(value));
                break;
            }
            case arrow::Type::STRING: {
                auto string_builder =
                    std::static_pointer_cast<arrow::StringBuilder>(builder);
                ast = string_builder->Append(value);
                break;
            }
            case arrow::Type::BINARY: {
                auto binary_builder =
                    std::static_pointer_cast<arrow::BinaryBuilder>(builder);
                ast = binary_builder->Append(value.data(), value.size());
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported JSON type: {} for value {}",
                          type_id,
                          value);
        }
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "failed to append data to builder: {}",
                  e.what());
    }
    return ast;
}

}  // namespace milvus::index