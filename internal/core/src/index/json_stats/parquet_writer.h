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

#include <memory>
#include <string>
#include <vector>
#include <map>
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/writer.h"
#include "index/json_stats/utils.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/common/config.h"

namespace milvus::index {

template <typename T>
inline T
ConvertValue(const std::string& value);

template <>
inline int8_t
ConvertValue<int8_t>(const std::string& value) {
    return static_cast<int8_t>(std::stoi(value));
}
template <>
inline int16_t
ConvertValue<int16_t>(const std::string& value) {
    return static_cast<int16_t>(std::stoi(value));
}
template <>
inline int32_t
ConvertValue<int32_t>(const std::string& value) {
    return std::stoi(value);
}
template <>
inline int64_t
ConvertValue<int64_t>(const std::string& value) {
    return std::stoll(value);
}
template <>
inline float
ConvertValue<float>(const std::string& value) {
    return std::stof(value);
}
template <>
inline double
ConvertValue<double>(const std::string& value) {
    return std::stod(value);
}
template <>
inline bool
ConvertValue<bool>(const std::string& value) {
    return value == "true";
}

template <typename BuilderType,
          typename CType = typename BuilderType::value_type>
inline arrow::Status
AppendValueImpl(std::shared_ptr<BuilderType> builder,
                const std::string& value) {
    return builder->Append(ConvertValue<CType>(value));
}

struct ParquetWriteContext {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders_map;
    std::vector<std::pair<std::string, std::string>> kv_metadata;
    std::vector<std::string> file_paths;
    std::vector<std::vector<int>> column_groups;
};

struct TableStatsInfo {
    std::shared_ptr<arrow::Schema> schema;
    std::map<JsonKey, JsonKeyLayoutType> column_map;
};

class ColumnGroupingStrategy {
 public:
    virtual ~ColumnGroupingStrategy() = default;

    virtual std::vector<std::vector<int>>
    CreateGroups(const TableStatsInfo& table_info) const = 0;
};

class DefaultColumnGroupingStrategy : public ColumnGroupingStrategy {
 public:
    std::vector<std::vector<int>>
    CreateGroups(const TableStatsInfo& table_info) const override {
        // put all columns into one group
        std::vector<std::vector<int>> column_groups;
        column_groups.reserve(1);
        std::vector<int> group;
        group.reserve(table_info.schema->num_fields());
        for (size_t i = 0; i < table_info.schema->num_fields(); ++i) {
            group.push_back(i);
        }
        column_groups.push_back(group);
        return column_groups;
    }
};

enum class ColumnGroupingStrategyType {
    DEFAULT,
};

class ColumnGroupingStrategyFactory {
 public:
    static std::unique_ptr<ColumnGroupingStrategy>
    CreateStrategy(ColumnGroupingStrategyType type) {
        switch (type) {
            case ColumnGroupingStrategyType::DEFAULT:
                return std::make_unique<DefaultColumnGroupingStrategy>();
            default:
                ThrowInfo(ErrorCode::UnexpectedError,
                          "Unknown ColumnGroupingStrategyType");
        }
    }
};

struct ParquetWriterFactory {
    static ParquetWriteContext
    CreateContext(const std::map<JsonKey, JsonKeyLayoutType>& column_map,
                  const std::string& path_prefix) {
        ParquetWriteContext context;
        context.schema = CreateArrowSchema(column_map);
        auto builders = CreateArrowBuilders(column_map);
        context.builders = std::move(builders.first);
        context.builders_map = std::move(builders.second);
        context.kv_metadata = CreateParquetKVMetadata(column_map);
        context.column_groups =
            std::move(ColumnGroupingStrategyFactory::CreateStrategy(
                          ColumnGroupingStrategyType::DEFAULT)
                          ->CreateGroups(TableStatsInfo{
                              context.schema,
                              column_map,
                          }));
        auto column_group_id = 0;
        for (const auto& group : context.column_groups) {
            auto file_log_id = 0;
            context.file_paths.push_back(CreateColumnGroupParquetPath(
                path_prefix, column_group_id, file_log_id));
            LOG_INFO("create column group parquet path: {} for column group {}",
                     context.file_paths.back(),
                     column_group_id);
            column_group_id++;
        }
        return context;
    }
};

class JsonStatsParquetWriter {
 public:
    JsonStatsParquetWriter(
        std::shared_ptr<arrow::fs::FileSystem> fs,
        milvus_storage::StorageConfig& storage_config,
        size_t buffer_size,
        size_t batch_size,
        std::shared_ptr<parquet::WriterProperties> writer_props =
            parquet::default_writer_properties());

    ~JsonStatsParquetWriter();

    void
    Init(const ParquetWriteContext& context);

    void
    AppendValue(const std::string& key, const std::string& value);

    void
    AppendRow(const std::map<std::string, std::string>& row_data);

    void
    AppendSharedRow(const uint8_t* data, size_t length);

    void
    Flush();

    void
    Close();

    size_t
    WriteCurrentBatch();

    size_t
    AddCurrentRow();

    std::map<std::string, int64_t>
    GetPathsToSize() const {
        return path_size_map_;
    }

    int64_t
    GetTotalSize() const {
        return total_size_;
    }

    void
    UpdatePathSizeMap(const std::vector<std::shared_ptr<arrow::Array>>& arrays);

 private:
    arrow::Status
    AppendDataToBuilder(const std::string& value,
                        std::shared_ptr<arrow::ArrayBuilder> builder);

    // init info
    std::shared_ptr<arrow::Schema> schema_{nullptr};

    // for column groups
    std::vector<std::string> file_paths_;
    std::vector<std::vector<int>> column_groups_;
    std::map<std::string, int64_t> path_size_map_;
    size_t total_size_{0};
    std::shared_ptr<parquet::WriterProperties> writer_props_;
    size_t buffer_size_;
    size_t batch_size_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    milvus_storage::StorageConfig storage_config_;
    std::shared_ptr<milvus_storage::PackedRecordBatchWriter> packed_writer_;
    std::vector<std::pair<std::string, std::string>> kv_metadata_;

    // cache for builders
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders_map_;
    size_t unflushed_row_count_{0};
    size_t all_row_count_{0};
    size_t current_row_count_{0};
};

}  // namespace milvus::index