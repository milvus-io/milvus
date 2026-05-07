// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <cstdint>
#include <shared_mutex>
#include "nlohmann/json_fwd.hpp"

#include "common/ArrayOffsets.h"
#include "common/Slice.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "index/InvertedIndexTantivy.h"
#include "index/ScalarIndex.h"
#include "storage/FileManager.h"
#include "boost/filesystem.hpp"
#include "tantivy-binding.h"

namespace milvus::index {
namespace json {
bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array);
}  // namespace json

const std::string INDEX_NON_EXIST_OFFSET_FILE_NAME =
    "json_index_non_exist_offsets";
const std::string INDEX_ARRAY_OFFSETS_FILE_NAME = "json_index_array_offsets";
class JsonInvertedIndexParseErrorRecorder {
 public:
    struct ErrorInstance {
        std::string json_str;
        std::string pointer;
    };
    struct ErrorStats {
        int64_t count;
        ErrorInstance first_instance;
    };
    void
    Record(const std::string_view& json_str,
           const std::string& pointer,
           const simdjson::error_code& error_code) {
        error_map_[error_code].count++;
        if (error_map_[error_code].count == 1) {
            error_map_[error_code].first_instance = {std::string(json_str),
                                                     pointer};
        }
    }

    void
    PrintErrStats() {
        if (error_map_.empty()) {
            LOG_INFO("No error found");
            return;
        }
        for (const auto& [error_code, stats] : error_map_) {
            LOG_INFO("Error code: {}, count: {}, first instance: {}",
                     error_message(error_code),
                     stats.count,
                     stats.first_instance.json_str);
        }
    }

    std::unordered_map<simdjson::error_code, ErrorStats>&
    GetErrorMap() {
        return error_map_;
    }

 private:
    std::unordered_map<simdjson::error_code, ErrorStats> error_map_;
};

template <typename T>
class JsonInvertedIndex : public index::InvertedIndexTantivy<T> {
 public:
    JsonInvertedIndex(
        const JsonCastType& cast_type,
        const std::string& nested_path,
        const storage::FileManagerContext& ctx,
        const int64_t tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION,
        const JsonCastFunction& cast_function =
            JsonCastFunction::FromString("unknown"))
        : nested_path_(nested_path),
          cast_type_(cast_type),
          cast_function_(cast_function) {
        this->schema_ = ctx.fieldDataMeta.field_schema;
        // Array cast builds per-element tantivy docs (one doc per array
        // element); flag the base class so null_offset_ is interpreted as
        // row-level and element/row bitmap semantics stay correct.
        this->is_nested_index_ =
            cast_type.data_type() == JsonCastType::DataType::ARRAY;
        this->file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(ctx);
        this->disk_file_manager_ =
            std::make_shared<storage::DiskFileManagerImpl>(ctx);

        if (ctx.for_loading_index) {
            return;
        }
        this->path_ = this->disk_file_manager_->GetLocalTempIndexObjectPrefix();

        this->d_type_ = cast_type_.ToTantivyType();
        boost::filesystem::create_directories(this->path_);
        std::string field_name = std::to_string(
            this->disk_file_manager_->GetFieldDataMeta().field_id);
        this->wrapper_ =
            std::make_shared<index::TantivyIndexWrapper>(field_name.c_str(),
                                                         this->d_type_,
                                                         this->path_.c_str(),
                                                         tantivy_index_version);
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader(SetBitsetFn set_bitset) {
        this->wrapper_->create_reader(set_bitset);
    }

    JsonInvertedIndexParseErrorRecorder&
    GetErrorRecorder() {
        return error_recorder_;
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

    BinarySet
    Serialize(const Config& config) override {
        std::shared_lock<folly::SharedMutex> lock(this->mutex_);
        auto index_valid_data_length =
            this->null_offset_.size() * sizeof(size_t);
        std::shared_ptr<uint8_t[]> index_valid_data(
            new uint8_t[index_valid_data_length]);
        memcpy(index_valid_data.get(),
               this->null_offset_.data(),
               index_valid_data_length);

        auto non_exist_data_length =
            this->non_exist_offsets_.size() * sizeof(size_t);
        std::shared_ptr<uint8_t[]> non_exist_data(
            new uint8_t[non_exist_data_length]);
        memcpy(non_exist_data.get(),
               this->non_exist_offsets_.data(),
               non_exist_data_length);
        lock.unlock();
        BinarySet res_set;
        if (index_valid_data_length > 0) {
            res_set.Append(INDEX_NULL_OFFSET_FILE_NAME,
                           index_valid_data,
                           index_valid_data_length);
        }
        if (non_exist_data_length > 0) {
            res_set.Append(INDEX_NON_EXIST_OFFSET_FILE_NAME,
                           non_exist_data,
                           non_exist_data_length);
        }
        if (!row_to_element_start_.empty()) {
            ArrayOffsetsSealed tmp({}, row_to_element_start_);
            auto array_offsets_buf = tmp.Serialize();
            std::shared_ptr<uint8_t[]> array_offsets_data(
                new uint8_t[array_offsets_buf.size()]);
            memcpy(array_offsets_data.get(),
                   array_offsets_buf.data(),
                   array_offsets_buf.size());
            res_set.Append(INDEX_ARRAY_OFFSETS_FILE_NAME,
                           array_offsets_data,
                           array_offsets_buf.size());
        }
        milvus::Disassemble(res_set);
        return res_set;
    }

    // Returns a row-level bitmap indicating which rows contain the JSON path.
    TargetBitmap
    Exists();

    // Populated only when cast_type_ is ARRAY, either during build (sidecar
    // is produced on flush) or during load (deserialized from sidecar).
    // Null otherwise. Segment layer pulls this to register into
    // array_offsets_map_ for MATCH_* expressions.
    std::shared_ptr<ArrayOffsetsSealed>
    GetArrayOffsets() const override {
        return array_offsets_;
    }

    void
    WriteEntries(storage::IndexEntryWriter* writer) override;

    void
    LoadEntries(storage::IndexEntryReader& reader,
                const Config& config) override;

 protected:
    nlohmann::json
    BuildTantivyMeta(const std::vector<std::string>& file_names,
                     bool has_null) override;

    void
    LoadIndexMetas(const std::vector<std::string>& index_files,
                   const Config& config) override final;

    void
    RetainTantivyIndexFiles(
        std::vector<std::string>& index_files) override final;

 private:
    std::string nested_path_;
    JsonInvertedIndexParseErrorRecorder error_recorder_;
    JsonCastType cast_type_;
    JsonCastFunction cast_function_;

    // Stores the offsets of rows in which this JSON path does not exist.
    // This includes rows that are null, does not have this JSON path, or have
    // null values for this JSON path.
    //
    // For example, if the JSON path is "/a", this vector will store rows like
    // null, {}, {"a": null}, etc.
    std::vector<size_t> non_exist_offsets_;

    // Populated only when cast_type_ is ARRAY. Accumulated in
    // build_index_for_json: starts with sentinel 0, each row pushes
    // back() + array_size. Serialized as the ArrayOffsets sidecar.
    std::vector<int32_t> row_to_element_start_;

    // Materialized ArrayOffsets after loading the sidecar. Built lazily
    // on first Load* invocation.
    std::shared_ptr<ArrayOffsetsSealed> array_offsets_;
};

}  // namespace milvus::index
