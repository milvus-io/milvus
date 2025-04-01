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
#include "common/FieldDataInterface.h"
#include "index/InvertedIndexTantivy.h"
#include "index/ScalarIndex.h"
#include "storage/FileManager.h"
#include "boost/filesystem.hpp"
#include "tantivy-binding.h"

namespace milvus::index {
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
                     error_code,
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
    JsonInvertedIndex(const proto::schema::DataType cast_type,
                      const std::string& nested_path,
                      const storage::FileManagerContext& ctx)
        : nested_path_(nested_path), cast_type_(cast_type) {
        this->schema_ = ctx.fieldDataMeta.field_schema;
        this->mem_file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(ctx);
        this->disk_file_manager_ =
            std::make_shared<storage::DiskFileManagerImpl>(ctx);

        if (ctx.for_loading_index) {
            return;
        }
        auto prefix = this->disk_file_manager_->GetTextIndexIdentifier();
        constexpr const char* TMP_INVERTED_INDEX_PREFIX =
            "/tmp/milvus/inverted-index/";
        this->path_ = std::string(TMP_INVERTED_INDEX_PREFIX) + prefix;

        this->d_type_ = index::get_tantivy_data_type(cast_type);
        boost::filesystem::create_directories(this->path_);
        std::string field_name = std::to_string(
            this->disk_file_manager_->GetFieldDataMeta().field_id);
        this->wrapper_ = std::make_shared<index::TantivyIndexWrapper>(
            field_name.c_str(), this->d_type_, this->path_.c_str());
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader() {
        this->wrapper_->create_reader();
    }

    enum DataType
    JsonCastType() const override {
        return static_cast<enum DataType>(cast_type_);
    }

    JsonInvertedIndexParseErrorRecorder&
    GetErrorRecorder() {
        return error_recorder_;
    }

 private:
    std::string nested_path_;
    proto::schema::DataType cast_type_;
    JsonInvertedIndexParseErrorRecorder error_recorder_;
};

}  // namespace milvus::index
