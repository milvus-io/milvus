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

#include <folly/ExceptionWrapper.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <any>
#include <functional>
#include <istream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "common/ChunkDataView.h"
#include "common/FieldData.h"
#include "common/OpContext.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/bson_view.h"
#include "common/jsmn.h"
#include "common/protobuf_utils.h"
#include "folly/FBVector.h"
#include "glog/logging.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/SkipIndex.h"
#include "index/json_stats/bson_inverted.h"
#include "index/json_stats/parquet_writer.h"
#include "index/json_stats/utils.h"
#include "log/Log.h"
#include "mmap/ChunkedColumnInterface.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/MemFileManagerImpl.h"

class CollectSingleJsonStatsInfoAccessor;
// Forward declaration of test accessor in global namespace for friend declaration
class TraverseJsonForBuildStatsAccessor;

namespace milvus::index {
class JsonKeyStats : public ScalarIndex<std::string> {
 public:
    explicit JsonKeyStats(
        const storage::FileManagerContext& ctx,
        bool is_load,
        int64_t json_stats_max_shredding_columns = 1024,
        double json_stats_shredding_ratio_threshold = 0.3,
        int64_t json_stats_write_batch_size = 81920,
        uint32_t tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION);

    ~JsonKeyStats() override;

 public:
    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas, bool nullable);

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    void
    Load(const BinarySet& binary_set, const Config& config) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Load not supported for JsonKeyStats");
    }

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "BuildWithDataset should be deprecated");
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::JSONSTATS;
    }

    void
    Build(const Config& config = {}) override;

    int64_t
    Count() override {
        return num_rows_;
    }

    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return false;
    }

    int64_t
    Size() override {
        return Count();
    }

    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "BuildWithRawDataForUT Not supported for JsonKeyStats");
    }

    void
    Build(size_t n,
          const std::string* values,
          const bool* valid_data = nullptr) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Build not supported for JsonKeyStats");
    }

    const TargetBitmap
    In(size_t n, const std::string* values) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "In not supported for JsonKeyStats");
    }

    const TargetBitmap
    IsNull() override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "IsNull not supported for JsonKeyStats");
    }

    TargetBitmap
    IsNotNull() override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "IsNotNull not supported for JsonKeyStats");
    }

    const TargetBitmap
    NotIn(size_t n, const std::string* values) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "NotIn not supported for JsonKeyStats");
    }

    const TargetBitmap
    Range(const std::string& value, OpType op) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Range not supported for JsonKeyStats");
    }

    const TargetBitmap
    Range(const std::string& lower_bound_value,
          bool lb_inclusive,
          const std::string& upper_bound_value,
          bool ub_inclusive) override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Range not supported for JsonKeyStats");
    }

    std::optional<std::string>
    Reverse_Lookup(size_t offset) const override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Reverse_Lookup not supported for JsonKeyStats");
    }

 public:
    PinWrapper<BsonInvertedIndex*>
    GetBsonIndex(milvus::OpContext* op_ctx) const {
        if (bson_index_cache_slot_ == nullptr) {
            return PinWrapper<BsonInvertedIndex*>(nullptr);
        }
        auto ca = SemiInlineGet(bson_index_cache_slot_->PinCells(op_ctx, {0}));
        auto index = ca->get_cell_of(0);
        return PinWrapper<BsonInvertedIndex*>(ca, index);
    }

    void
    ExecuteForSharedData(
        milvus::OpContext* op_ctx,
        PinWrapper<BsonInvertedIndex*>& bson_index_cache,
        const std::string& path,
        std::function<void(BsonView bson, uint32_t row_id, uint32_t offset)>
            func) {
        if (bson_index_cache.get() == nullptr) {
            bson_index_cache = GetBsonIndex(op_ctx);
        }
        if (bson_index_cache.get() == nullptr || shared_column_ == nullptr) {
            return;
        }
        bson_index_cache.get()->TermQuery(
            path,
            [this, &func, op_ctx](const uint32_t* row_id_array,
                                  const uint32_t* offset_array,
                                  const int64_t array_len) {
                shared_column_->BulkRawBsonAt(
                    op_ctx, func, row_id_array, offset_array, array_len);
            });
    }

    int64_t
    ExecutorForGettingValid(milvus::OpContext* op_ctx,
                            const std::string& path,
                            TargetBitmapView valid_res) {
        size_t processed_size = 0;
        // if path is not in shredding_columns_, return 0
        if (shredding_columns_.find(path) == shredding_columns_.end()) {
            return processed_size;
        }
        auto column = shredding_columns_[path];
        auto num_data_chunk = column->num_chunks();

        for (size_t i = 0; i < num_data_chunk; i++) {
            auto chunk_size = column->chunk_row_nums(i);
            auto pw = column->ChunkDataView(op_ctx, i);
            auto data_view = pw.get();
            const bool* valid_data = data_view.ValidData();
            ApplyOnlyValidData(
                valid_data, valid_res + processed_size, chunk_size);
            processed_size += chunk_size;
        }
        AssertInfo(processed_size == valid_res.size(),
                   "Processed size {} is not equal to num_rows {}",
                   processed_size,
                   valid_res.size());
        return processed_size;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ExecutorForShreddingData(
        milvus::OpContext* op_ctx,
        // path is field_name in shredding_columns_
        const std::string& path,
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, std::string, int)>
            skip_func,
        TargetBitmapView res,
        TargetBitmapView valid_res,
        ValTypes... values) {
        int64_t processed_size = 0;
        // if path is not in shredding_columns_, return 0
        if (shredding_columns_.find(path) == shredding_columns_.end()) {
            return processed_size;
        }
        auto column = shredding_columns_[path];
        auto num_data_chunk = column->num_chunks();
        auto num_rows = column->NumRows();

        for (size_t i = 0; i < num_data_chunk; i++) {
            auto chunk_size = column->chunk_row_nums(i);

            if (!skip_func || !skip_func(skip_index_, path, i)) {
                auto pw = column->ChunkDataView(op_ctx, i);
                auto any_view = pw.get();
                if constexpr (std::is_same_v<T, std::string_view>) {
                    // Get string data as string_view directly
                    auto data_view = any_view.as<std::string_view>();
                    auto row_count = data_view->RowCount();
                    auto data = data_view->Data();
                    const bool* valid_data = data_view->ValidData();
                    func(data,
                         valid_data,
                         chunk_size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                } else {
                    auto data_view = any_view.as<T>();
                    const T* data = static_cast<const T*>(data_view->Data());
                    const bool* valid_data = data_view->ValidData();
                    func(data,
                         valid_data,
                         chunk_size,
                         res + processed_size,
                         valid_res + processed_size,
                         values...);
                }
            } else {
                auto pw = column->ChunkDataView(op_ctx, i);
                auto chunk = pw.get();
                const bool* valid_data = chunk.ValidData();
                ApplyValidData(valid_data,
                               res + processed_size,
                               valid_res + processed_size,
                               chunk_size);
            }

            processed_size += chunk_size;
        }
        AssertInfo(processed_size == num_rows,
                   "Processed size {} is not equal to num_rows {}",
                   processed_size,
                   num_rows);
        return processed_size;
    }

    std::set<std::string>
    GetShreddingFields(const std::string& pointer) {
        std::set<std::string> fields;
        if (key_field_map_.find(pointer) != key_field_map_.end()) {
            for (const auto& field : key_field_map_[pointer]) {
                if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end()) {
                    fields.insert(field);
                }
            }
        }
        return fields;
    }

    // return all shredding fields whose pointers start with the given prefix
    // for example, prefix "/a/b" will include fields for "/a/b" and "/a/b/..."
    std::set<std::string>
    GetShreddingFieldsWithPrefix(const std::string& prefix) {
        std::set<std::string> fields;
        for (const auto& [path, field_names] : key_field_map_) {
            if (path.size() >= prefix.size() &&
                path.compare(0, prefix.size(), prefix) == 0 &&
                (path.size() == prefix.size() || path[prefix.size()] == '/')) {
                for (const auto& field : field_names) {
                    if (shred_field_data_type_map_.find(field) !=
                        shred_field_data_type_map_.end()) {
                        fields.insert(field);
                    }
                }
            }
        }
        return fields;
    }

    std::string
    GetShreddingField(const std::string& pointer, JSONType type) {
        if (key_field_map_.find(pointer) == key_field_map_.end()) {
            return "";
        }
        for (const auto& field : key_field_map_[pointer]) {
            if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end() &&
                shred_field_data_type_map_[field] == type) {
                return field;
            }
        }
        return "";
    }

    std::set<std::string>
    GetShreddingFields(const std::string& pointer,
                       std::vector<JSONType> types) {
        std::set<std::string> fields;
        if (key_field_map_.find(pointer) == key_field_map_.end()) {
            return fields;
        }
        for (const auto& field : key_field_map_[pointer]) {
            if (shred_field_data_type_map_.find(field) !=
                    shred_field_data_type_map_.end() &&
                std::find(types.begin(),
                          types.end(),
                          shred_field_data_type_map_[field]) != types.end()) {
                fields.insert(field);
            }
        }
        return fields;
    }

    JSONType
    GetShreddingJsonType(const std::string& field_name) {
        if (shred_field_data_type_map_.find(field_name) !=
            shred_field_data_type_map_.end()) {
            return shred_field_data_type_map_[field_name];
        }
        return JSONType::UNKNOWN;
    }

 private:
    void
    CollectSingleJsonStatsInfo(const char* json_str,
                               std::map<JsonKey, KeyStatsInfo>& infos);

    std::string
    PrintKeyInfo(const std::map<JsonKey, KeyStatsInfo>& infos) {
        std::stringstream ss;
        for (const auto& [key, info] : infos) {
            ss << key.ToString() << " -> " << info.ToString() << "\t";
        }
        return ss.str();
    }

    std::map<JsonKey, KeyStatsInfo>
    CollectKeyInfo(const std::vector<FieldDataPtr>& field_datas, bool nullable);

    void
    TraverseJsonForStats(const char* json,
                         jsmntok* tokens,
                         int& index,
                         std::vector<std::string>& path,
                         std::map<JsonKey, KeyStatsInfo>& infos);

    void
    AddKeyStatsInfo(const std::vector<std::string>& paths,
                    JSONType type,
                    uint8_t* value,
                    std::map<JsonKey, KeyStatsInfo>& infos);

    std::string
    PrintJsonKeyLayoutType(const std::map<JsonKey, JsonKeyLayoutType>& infos) {
        std::stringstream ss;
        std::unordered_map<JsonKeyLayoutType, std::vector<std::string>>
            type_to_keys;
        for (const auto& [key, type] : infos) {
            type_to_keys[type].push_back(key.ToString());
        }
        for (const auto& [type, keys] : type_to_keys) {
            ss << ToString(type) << " -> [" << Join(keys, ", ") << "]\n";
        }
        return ss.str();
    }

    std::map<JsonKey, JsonKeyLayoutType>
    ClassifyJsonKeyLayoutType(const std::map<JsonKey, KeyStatsInfo>& infos);

    void
    BuildKeyStats(const std::vector<FieldDataPtr>& field_datas, bool nullable);

    void
    BuildKeyStatsForRow(const char* json_str, uint32_t row_id);

    void
    BuildKeyStatsForNullRow();

    std::string
    GetShreddingDir();

    std::string
    GetSharedKeyIndexDir();

    std::string
    GetMetaFilePath();

    void
    WriteMetaFile();

    void
    LoadMetaFile(const std::string& meta_file_path);

    void
    AddKeyStats(const std::vector<std::string>& path,
                JSONType type,
                const std::string& value,
                std::map<JsonKey, std::string>& values);

    void
    TraverseJsonForBuildStats(const char* json,
                              jsmntok* tokens,
                              int& index,
                              std::vector<std::string>& path,
                              std::map<JsonKey, std::string>& values);

    bool
    IsBoolean(const std::string& str) {
        return str == "true" || str == "false";
    }

    bool
    IsInt8(const std::string& str) {
        std::istringstream iss(str);
        int8_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int8_t>::min() &&
               num <= std::numeric_limits<int8_t>::max();
    }

    bool
    IsInt16(const std::string& str) {
        std::istringstream iss(str);
        int16_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int16_t>::min() &&
               num <= std::numeric_limits<int16_t>::max();
    }

    bool
    IsInt32(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof() &&
               num >= std::numeric_limits<int32_t>::min() &&
               num <= std::numeric_limits<int32_t>::max();
    }

    bool
    IsInt64(const std::string& str) {
        std::istringstream iss(str);
        int64_t num;
        iss >> num;

        return !iss.fail() && iss.eof();
    }

    bool
    IsFloat(const std::string& str) {
        try {
            float d = std::stof(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    IsDouble(const std::string& str) {
        try {
            double d = std::stod(str);
            return true;
        } catch (...) {
            return false;
        }
    }

    bool
    IsNull(const std::string& str) {
        return str == "null";
    }

    JSONType
    getType(const std::string& str) {
        if (IsBoolean(str)) {
            return JSONType::BOOL;
            // TODO: add int8, int16, int32 support
            // now we only support int64 for build performance
            // } else if (IsInt8(str)) {
            //     return JSONType::INT8;
            // } else if (IsInt16(str)) {
            //     return JSONType::INT16;
            // } else if (IsInt32(str)) {
            //     return JSONType::INT32;
        } else if (IsInt64(str)) {
            return JSONType::INT64;
        } else if (IsFloat(str)) {
            return JSONType::FLOAT;
        } else if (IsDouble(str)) {
            return JSONType::DOUBLE;
        } else if (IsNull(str)) {
            return JSONType::NONE;
        }
        LOG_DEBUG("unknown json type for string: {}", str);
        return JSONType::UNKNOWN;
    }

    void
    LoadShreddingData(const std::vector<std::string>& index_files,
                      const std::string& warmup_policy = "");

    void
    ApplyValidData(const bool* valid_data,
                   TargetBitmapView res,
                   TargetBitmapView valid_res,
                   const int size) {
        if (valid_data != nullptr) {
            for (int i = 0; i < size; i++) {
                if (!valid_data[i]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
    }

    void
    ApplyOnlyValidData(const bool* valid_data,
                       TargetBitmapView valid_res,
                       const int size) {
        if (valid_data != nullptr) {
            for (int i = 0; i < size; i++) {
                if (!valid_data[i]) {
                    valid_res[i] = false;
                }
            }
        }
    }

    void
    GetColumnSchemaFromParquet(int64_t column_group_id,
                               const std::string& file);

    void
    GetCommonMetaFromParquet(const std::string& file);

    void
    LoadColumnGroup(int64_t column_group_id,
                    const std::vector<int64_t>& file_ids,
                    const std::string& warmup_policy = "");

    void
    LoadShreddingMeta(
        std::vector<std::pair<int64_t, std::vector<int64_t>>> sorted_files);

    std::string
    AddBucketName(const std::string& remote_prefix);

    void
    LoadSharedKeyIndex(const std::vector<std::string>& shared_key_index_files,
                       bool enable_mmap,
                       int64_t index_size,
                       const std::string& warmup_policy = "");

 private:
    proto::schema::FieldSchema schema_;
    int64_t segment_id_;
    int64_t field_id_;
    mutable std::mutex mtx_;
    int64_t num_rows_{0};
    bool is_built_ = false;
    std::string path_;
    milvus::storage::ChunkManagerPtr rcm_;
    std::shared_ptr<milvus::storage::MemFileManagerImpl> mem_file_manager_;
    std::shared_ptr<milvus::storage::DiskFileManagerImpl> disk_file_manager_;
    int64_t max_shredding_columns_;
    double shredding_ratio_threshold_;
    int64_t write_batch_size_;

    std::map<JsonKey, JsonKeyLayoutType> key_types_;
    std::set<JsonKey> shared_keys_;
    std::set<JsonKey> column_keys_;
    std::shared_ptr<JsonStatsParquetWriter> parquet_writer_;
    std::shared_ptr<BsonInvertedIndex> bson_inverted_index_;
    // cache slot for bson inverted index when using translator
    std::shared_ptr<milvus::cachinglayer::CacheSlot<BsonInvertedIndex>>
        bson_index_cache_slot_;

    milvus::proto::common::LoadPriority load_priority_;
    // some meta cache for searching
    // json_path -> [json_path_int, json_path_string, ...], only for shredding columns
    std::unordered_map<std::string, std::set<std::string>> key_field_map_;
    // field_name -> data_type, such as json_path_int -> JSONType::INT64, only for real shredding columns
    std::unordered_map<std::string, JSONType> shred_field_data_type_map_;
    // field_name -> field_id, such as json_path_int -> 1001
    std::unordered_map<std::string, int64_t> field_name_to_id_map_;
    // field_id -> field_name, such as 1001 -> json_path_int
    std::unordered_map<int64_t, std::string> field_id_to_name_map_;
    // field_name vector, the sequece is the same as the order of files
    std::vector<std::string> field_names_;
    // field_name -> column
    mutable std::unordered_map<std::string,
                               std::shared_ptr<milvus::ChunkedColumnInterface>>
        shredding_columns_;
    std::string mmap_filepath_;

    std::string shared_column_field_name_;
    std::shared_ptr<milvus::ChunkedColumnInterface> shared_column_;
    SkipIndex skip_index_;

    // Meta file for storing layout type map and other metadata
    JsonStatsMeta json_stats_meta_;
    int64_t meta_file_size_{0};

    // Friend accessor for unit tests to call private methods safely.
    friend class ::TraverseJsonForBuildStatsAccessor;
    friend class ::CollectSingleJsonStatsInfoAccessor;
};

}  // namespace milvus::index
