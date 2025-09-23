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

#include "index/JsonInvertedIndex.h"
#include <string>
#include <shared_mutex>
#include <string_view>
#include <type_traits>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Types.h"
#include "folly/FBVector.h"
#include "log/Log.h"
#include "common/JsonUtils.h"
#include "simdjson/error.h"
#include "index/JsonIndexBuilder.h"

namespace milvus::index {

namespace json {

bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array) {
    bool cast_type_is_array =
        cast_type.data_type() == JsonCastType::DataType::ARRAY;
    auto type = cast_type.ToMilvusDataType();
    return is_array == cast_type_is_array &&
           (type == data_type ||
            (data_type == DataType::INT64 && type == DataType::DOUBLE));
}

}  // namespace json

template <typename T>
void
JsonInvertedIndex<T>::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    LOG_INFO("Start to build json inverted index for field: {}", nested_path_);

    ProcessJsonFieldData<T>(
        field_datas,
        this->schema_,
        nested_path_,
        cast_type_,
        cast_function_,
        // add data
        [this](const T* data, int64_t size, int64_t offset) {
            this->wrapper_->template add_array_data<T>(data, size, offset);
        },
        // handle null
        [this](int64_t offset) { this->null_offset_.push_back(offset); },
        // handle non exist
        [this](int64_t offset) { this->non_exist_offsets_.push_back(offset); },
        // handle error
        [this](const Json& json,
               const std::string& nested_path,
               simdjson::error_code error) {
            this->error_recorder_.Record(json, nested_path, error);
        });

    error_recorder_.PrintErrStats();
}

template <typename T>
TargetBitmap
JsonInvertedIndex<T>::Exists() {
    int64_t count = this->Count();
    TargetBitmap bitset(count, true);

    auto fill_bitset = [this, count, &bitset]() {
        auto end = std::lower_bound(
            non_exist_offsets_.begin(), non_exist_offsets_.end(), count);
        for (auto iter = non_exist_offsets_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
    };

    if (this->is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(this->mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
void
JsonInvertedIndex<T>::LoadIndexMetas(
    const std::vector<std::string>& index_files, const Config& config) {
    InvertedIndexTantivy<T>::LoadIndexMetas(index_files, config);
    auto fill_non_exist_offset = [&](const uint8_t* data, int64_t size) {
        non_exist_offsets_.resize((size_t)size / sizeof(size_t));
        memcpy(non_exist_offsets_.data(), data, (size_t)size);
    };
    auto non_exist_offset_file_itr = std::find_if(
        index_files.begin(), index_files.end(), [&](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   INDEX_NON_EXIST_OFFSET_FILE_NAME;
        });
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    if (non_exist_offset_file_itr != index_files.end()) {
        // null offset file is not sliced
        auto index_datas = this->mem_file_manager_->LoadIndexToMemory(
            {*non_exist_offset_file_itr}, load_priority);
        auto non_exist_offset_data =
            std::move(index_datas.at(INDEX_NON_EXIST_OFFSET_FILE_NAME));
        fill_non_exist_offset(non_exist_offset_data->PayloadData(),
                              non_exist_offset_data->PayloadSize());
        return;
    }
    std::vector<std::string> non_exist_offset_files;
    for (auto& file : index_files) {
        auto file_name = boost::filesystem::path(file).filename().string();
        if (file_name.find(INDEX_NON_EXIST_OFFSET_FILE_NAME) !=
            std::string::npos) {
            non_exist_offset_files.push_back(file);
        }
    }
    if (non_exist_offset_files.size() > 0) {
        // null offset file is sliced
        auto index_datas = this->mem_file_manager_->LoadIndexToMemory(
            non_exist_offset_files, load_priority);

        auto non_exist_offset_data = CompactIndexDatas(index_datas);
        auto non_exist_offset_data_codecs = std::move(
            non_exist_offset_data.at(INDEX_NON_EXIST_OFFSET_FILE_NAME));
        for (auto&& non_exist_offset_codec :
             non_exist_offset_data_codecs.codecs_) {
            fill_non_exist_offset(non_exist_offset_codec->PayloadData(),
                                  non_exist_offset_codec->PayloadSize());
        }
        return;
    }

    // Fallback: no non_exist_offset_file found. This occurs in two scenarios:
    // 1. Legacy v2.5.x data where non_exist_offset_file doesn't exist
    // 2. All records are valid (no invalid offsets to track)
    //
    // Use null_offset_ as the source for non_exist_offsets_ to maintain
    // backward compatibility. This ensures Exists() behaves like v2.5.x IsNotNull().
    non_exist_offsets_ = this->null_offset_;
}

template <typename T>
void
JsonInvertedIndex<T>::RetainTantivyIndexFiles(
    std::vector<std::string>& index_files) {
    index_files.erase(
        std::remove_if(
            index_files.begin(),
            index_files.end(),
            [&](const std::string& file) {
                auto file_name =
                    boost::filesystem::path(file).filename().string();
                return file_name.find(INDEX_NON_EXIST_OFFSET_FILE_NAME) !=
                       std::string::npos;
            }),
        index_files.end());
    InvertedIndexTantivy<T>::RetainTantivyIndexFiles(index_files);
}

template class JsonInvertedIndex<bool>;
template class JsonInvertedIndex<int64_t>;
template class JsonInvertedIndex<double>;
template class JsonInvertedIndex<std::string>;

}  // namespace milvus::index
