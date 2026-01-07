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
#include <string_view>
#include <type_traits>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Types.h"
#include "folly/FBVector.h"
#include "log/Log.h"
#include "simdjson/error.h"

namespace milvus::index {

// Parse a JSON Pointer into unescaped path segments
std::vector<std::string>
parse_json_pointer(const std::string& pointer) {
    std::vector<std::string> tokens;
    if (pointer.empty())
        return tokens;  // Root path (entire document)
    if (pointer[0] != '/') {
        throw std::invalid_argument(
            "Invalid JSON Pointer: must start with '/'");
    }
    size_t start = 1;
    while (start < pointer.size()) {
        size_t end = pointer.find('/', start);
        if (end == std::string::npos)
            end = pointer.size();
        std::string token = pointer.substr(start, end - start);
        // Replace ~1 with / and ~0 with ~
        size_t pos = 0;
        while ((pos = token.find("~1", pos)) != std::string::npos) {
            token.replace(pos, 2, "/");
            pos += 1;  // Avoid infinite loops on overlapping replacements
        }
        pos = 0;
        while ((pos = token.find("~0", pos)) != std::string::npos) {
            token.replace(pos, 2, "~");
            pos += 1;
        }
        tokens.push_back(token);
        start = end + 1;
    }
    return tokens;
}

// Check if a JSON Pointer path exists
bool
path_exists(const simdjson::dom::element& root,
            const std::vector<std::string>& tokens) {
    simdjson::dom::element current = root;
    for (const auto& token : tokens) {
        if (current.type() == simdjson::dom::element_type::OBJECT) {
            auto obj = current.get_object();
            if (obj.error())
                return false;
            auto next = obj.value().at_key(token);
            if (next.error())
                return false;
            current = next.value();
        } else if (current.type() == simdjson::dom::element_type::ARRAY) {
            if (token == "-")
                return false;  // "-" is invalid for existence checks
            char* endptr;
            long index = strtol(token.c_str(), &endptr, 10);
            if (*endptr != '\0' || index < 0)
                return false;  // Not a valid index
            auto arr = current.get_array();
            if (arr.error())
                return false;
            if (static_cast<size_t>(index) >= arr.value().size())
                return false;
            auto next = arr.value().at(index);
            if (next.error())
                return false;
            current = next.value();
        } else {
            return false;  // Path cannot be resolved
        }
    }
    return true;
}
template <typename T>
void
JsonInvertedIndex<T>::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    LOG_INFO("Start to build json inverted index for field: {}", nested_path_);
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    auto tokens = parse_json_pointer(nested_path_);

    bool is_array = cast_type_.data_type() == JsonCastType::DataType::ARRAY;

    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (this->schema_.nullable() && !data->is_valid(i)) {
                this->non_exist_offsets_.push_back(offset);
                this->null_offset_.push_back(offset);
                this->wrapper_->template add_multi_data<T>(
                    nullptr, 0, offset++);
                continue;
            }

            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists ||
                nested_path_ != "" &&
                    json_column->doc().at_pointer(nested_path_).is_null()) {
                error_recorder_.Record(
                    *json_column, nested_path_, simdjson::NO_SUCH_FIELD);
                this->non_exist_offsets_.push_back(offset);
                this->wrapper_->template add_multi_data<T>(
                    nullptr, 0, offset++);
                continue;
            }
            folly::fbvector<T> values;
            if (is_array) {
                auto doc = json_column->dom_doc();
                auto array_res = doc.at_pointer(nested_path_).get_array();
                if (array_res.error() != simdjson::SUCCESS) {
                    error_recorder_.Record(
                        *json_column, nested_path_, array_res.error());
                } else {
                    auto array_values = array_res.value();
                    for (auto value : array_values) {
                        auto val = value.get<SIMDJSON_T>();

                        if (val.error() == simdjson::SUCCESS) {
                            values.push_back(static_cast<T>(val.value()));
                        }
                    }
                }
            } else {
                if (cast_function_.match<T>()) {
                    auto res = JsonCastFunction::CastJsonValue<T>(
                        cast_function_, *json_column, nested_path_);
                    if (res.has_value()) {
                        values.push_back(res.value());
                    }
                } else {
                    value_result<SIMDJSON_T> res =
                        json_column->at<SIMDJSON_T>(nested_path_);
                    if (res.error() != simdjson::SUCCESS) {
                        error_recorder_.Record(
                            *json_column, nested_path_, res.error());
                    } else {
                        values.push_back(static_cast<T>(res.value()));
                    }
                }
            }
            this->wrapper_->template add_multi_data<T>(
                values.data(), values.size(), offset++);
        }
    }

    error_recorder_.PrintErrStats();
}

template <typename T>
bool
JsonInvertedIndex<T>::IsDataTypeSupported(DataType data_type,
                                          bool is_array) const {
    bool cast_type_is_array =
        cast_type_.data_type() == JsonCastType::DataType::ARRAY;
    auto type = cast_type_.ToMilvusDataType();
    return is_array == cast_type_is_array &&
           (type == data_type ||
            (data_type == DataType::INT64 && type == DataType::DOUBLE));
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

    fill_bitset();

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

    if (non_exist_offset_file_itr != index_files.end()) {
        // null offset file is not sliced
        auto index_datas = this->mem_file_manager_->LoadIndexToMemory(
            {*non_exist_offset_file_itr});
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
        auto index_datas =
            this->mem_file_manager_->LoadIndexToMemory(non_exist_offset_files);

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