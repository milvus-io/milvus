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
        [this](const folly::fbvector<T>& values, int64_t offset) {
            this->wrapper_->template add_array_data<T>(
                values.data(), values.size(), offset);
        },
        [this](int64_t offset) {
            this->null_offset_.push_back(offset);
            this->wrapper_->template add_array_data<T>(nullptr, 0, offset);
        },
        [this](const Json& json,
               const std::string& nested_path,
               simdjson::error_code error) {
            this->error_recorder_.Record(json, nested_path, error);
        });

    error_recorder_.PrintErrStats();
}

template class JsonInvertedIndex<bool>;
template class JsonInvertedIndex<int64_t>;
template class JsonInvertedIndex<double>;
template class JsonInvertedIndex<std::string>;

}  // namespace milvus::index