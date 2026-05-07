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

#include <algorithm>
#include <exception>
#include <fcntl.h>
#include <list>
#include <optional>
#include <string>
#include <unistd.h>
#include <utility>

#include "boost/filesystem/directory.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "folly/SharedMutex.h"
#include "common/Json.h"
#include "common/Types.h"
#include "index/JsonIndexBuilder.h"
#include "index/Utils.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "simdjson/error.h"
#include "storage/FileWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/ThreadPools.h"

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

    auto null_adder = [this](int64_t offset) {
        this->null_offset_.push_back(offset);
    };
    auto non_exist_adder = [this](int64_t offset) {
        this->non_exist_offsets_.push_back(offset);
    };
    auto error_adder = [this](const Json& json,
                              const std::string& nested_path,
                              simdjson::error_code error) {
        this->error_recorder_.Record(json, nested_path, error);
    };

    if (cast_type_.data_type() == JsonCastType::DataType::ARRAY) {
        row_to_element_start_.push_back(0);  // sentinel
        ProcessJsonFieldArrayData<T>(
            field_datas,
            this->schema_,
            nested_path_,
            [this](const T* data, int64_t size, int64_t offset) {
                // Nested mode: emit one tantivy doc per element, with
                // consecutive doc_ids [offset, offset+size). offset is the
                // running element count (elem_start) from the Process fn.
                if (size > 0) {
                    this->wrapper_->template add_data<T>(data, size, offset);
                }
                this->row_to_element_start_.push_back(
                    this->row_to_element_start_.back() +
                    static_cast<int32_t>(size));
            },
            null_adder,
            non_exist_adder,
            error_adder);
        std::vector<int32_t> element_row_ids(row_to_element_start_.back());
        for (int32_t row = 0;
             row < static_cast<int32_t>(row_to_element_start_.size()) - 1;
             ++row) {
            std::fill(element_row_ids.begin() + row_to_element_start_[row],
                      element_row_ids.begin() + row_to_element_start_[row + 1],
                      row);
        }
        array_offsets_ = std::make_shared<ArrayOffsetsSealed>(
            std::move(element_row_ids), row_to_element_start_);
    } else {
        ProcessJsonFieldData<T>(
            field_datas,
            this->schema_,
            nested_path_,
            cast_function_,
            [this](const T* data, int64_t size, int64_t offset) {
                this->wrapper_->template add_array_data<T>(data, size, offset);
            },
            null_adder,
            non_exist_adder,
            error_adder);
    }

    error_recorder_.PrintErrStats();
}

template <typename T>
TargetBitmap
JsonInvertedIndex<T>::Exists() {
    int64_t count = this->Count();
    if (this->is_nested_index_) {
        AssertInfo(array_offsets_ != nullptr,
                   "JSON ARRAY index requires ArrayOffsets for Exists()");
        count = array_offsets_->GetRowCount();
    }
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
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    // Load ArrayOffsets sidecar (only present for ARRAY cast_type).
    auto array_offsets_file_itr = std::find_if(
        index_files.begin(), index_files.end(), [&](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   INDEX_ARRAY_OFFSETS_FILE_NAME;
        });
    if (array_offsets_file_itr != index_files.end()) {
        auto index_datas = this->file_manager_->LoadIndexToMemory(
            {*array_offsets_file_itr}, load_priority);
        auto data = std::move(index_datas.at(INDEX_ARRAY_OFFSETS_FILE_NAME));
        array_offsets_ = ArrayOffsetsSealed::Deserialize(
            reinterpret_cast<const uint8_t*>(data->PayloadData()),
            static_cast<size_t>(data->PayloadSize()));
        this->is_nested_index_ = true;
    } else if (cast_type_.data_type() == JsonCastType::DataType::ARRAY) {
        // Legacy JSON ARRAY indexes were row-level and have no ArrayOffsets
        // sidecar. Keep them row-level so existing json_contains/exists
        // behavior is preserved after upgrade.
        this->is_nested_index_ = false;
    }

    auto fill_non_exist_offset = [&](const uint8_t* data, int64_t size) {
        non_exist_offsets_.resize((size_t)size / sizeof(size_t));
        memcpy(non_exist_offsets_.data(), data, (size_t)size);
    };
    auto non_exist_offset_file_itr = std::find_if(
        index_files.begin(), index_files.end(), [&](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   INDEX_NON_EXIST_OFFSET_FILE_NAME;
        });

    if (non_exist_offset_file_itr != index_files.end()) {
        // null offset file is not sliced
        auto index_datas = this->file_manager_->LoadIndexToMemory(
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
        // add slice meta file for null offset file compact
        if (file_name == INDEX_FILE_SLICE_META) {
            non_exist_offset_files.push_back(file);
        }
    }
    if (non_exist_offset_files.size() > 0) {
        // null offset file is sliced
        auto index_datas = this->file_manager_->LoadIndexToMemory(
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
                           std::string::npos ||
                       file_name == INDEX_ARRAY_OFFSETS_FILE_NAME;
            }),
        index_files.end());
    InvertedIndexTantivy<T>::RetainTantivyIndexFiles(index_files);
}

template <typename T>
nlohmann::json
JsonInvertedIndex<T>::BuildTantivyMeta(
    const std::vector<std::string>& file_names, bool has_null) {
    auto meta = InvertedIndexTantivy<T>::BuildTantivyMeta(file_names, has_null);
    std::shared_lock<folly::SharedMutex> lock(this->mutex_);
    meta["has_non_exist"] = !this->non_exist_offsets_.empty();
    meta["has_array_offsets"] = !this->row_to_element_start_.empty();
    return meta;
}

template <typename T>
void
JsonInvertedIndex<T>::WriteEntries(storage::IndexEntryWriter* writer) {
    // Call parent to write tantivy index files and null_offset
    InvertedIndexTantivy<T>::WriteEntries(writer);

    // Write json-specific data (non_exist_offsets)
    std::shared_lock<folly::SharedMutex> lock(this->mutex_);
    bool has_non_exist = !this->non_exist_offsets_.empty();
    writer->PutMeta("has_non_exist", has_non_exist);
    if (has_non_exist) {
        writer->WriteEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME,
                           this->non_exist_offsets_.data(),
                           this->non_exist_offsets_.size() * sizeof(size_t));
    }

    // Write ArrayOffsets sidecar (only present for ARRAY cast_type).
    bool has_array_offsets = !this->row_to_element_start_.empty();
    writer->PutMeta("has_array_offsets", has_array_offsets);
    if (has_array_offsets) {
        ArrayOffsetsSealed tmp({}, this->row_to_element_start_);
        auto buf = tmp.Serialize();
        writer->WriteEntry(
            INDEX_ARRAY_OFFSETS_FILE_NAME, buf.data(), buf.size());
    }
}

template <typename T>
void
JsonInvertedIndex<T>::LoadEntries(storage::IndexEntryReader& reader,
                                  const Config& config) {
    // Call parent to load tantivy index files and null_offset
    InvertedIndexTantivy<T>::LoadEntries(reader, config);

    // Load json-specific data (non_exist_offsets)
    bool has_non_exist = reader.GetMeta<bool>("has_non_exist", false);
    if (has_non_exist) {
        auto e = reader.ReadEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME);
        this->non_exist_offsets_.resize(e.data.size() / sizeof(size_t));
        std::memcpy(
            this->non_exist_offsets_.data(), e.data.data(), e.data.size());
    }

    // Load ArrayOffsets sidecar (only present for ARRAY cast_type).
    bool has_array_offsets = reader.GetMeta<bool>("has_array_offsets", false);
    if (has_array_offsets) {
        auto e = reader.ReadEntry(INDEX_ARRAY_OFFSETS_FILE_NAME);
        array_offsets_ = ArrayOffsetsSealed::Deserialize(
            reinterpret_cast<const uint8_t*>(e.data.data()), e.data.size());
        this->is_nested_index_ = true;
    } else if (cast_type_.data_type() == JsonCastType::DataType::ARRAY) {
        this->is_nested_index_ = false;
    }
    LOG_INFO(
        "LoadEntries JsonInvertedIndex done, has_non_exist: {}, "
        "has_array_offsets: {}",
        has_non_exist,
        has_array_offsets);
}

template class JsonInvertedIndex<bool>;
template class JsonInvertedIndex<int64_t>;
template class JsonInvertedIndex<double>;
template class JsonInvertedIndex<std::string>;

}  // namespace milvus::index
