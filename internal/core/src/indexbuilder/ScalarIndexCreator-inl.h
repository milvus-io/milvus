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

#include <knowhere/index/structured_index_simple/StructuredIndexSort.h>
#include <pb/schema.pb.h>
#include "indexbuilder/helper.h"
#include "indexbuilder/StringIndexImpl.h"

#include <string>
#include <memory>
#include <vector>

namespace milvus::indexbuilder {

template <typename T>
inline ScalarIndexCreator<T>::ScalarIndexCreator(const char* type_params, const char* index_params) {
    // TODO: move parse-related logic to a common interface.
    Helper::ParseFromString(type_params_, std::string(type_params));
    Helper::ParseFromString(index_params_, std::string(index_params));
    // TODO: create index according to the params.
    index_ = std::make_unique<knowhere::scalar::StructuredIndexSort<T>>();
}

template <typename T>
inline void
ScalarIndexCreator<T>::Build(const knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
    index_->Build(size, reinterpret_cast<const T*>(data));
}

template <typename T>
inline knowhere::BinarySet
ScalarIndexCreator<T>::Serialize() {
    return index_->Serialize(config_);
}

template <typename T>
inline void
ScalarIndexCreator<T>::Load(const knowhere::BinarySet& binary_set) {
    index_->Load(binary_set);
}

// not sure that the pointer of a golang bool array acts like other types.
template <>
inline void
ScalarIndexCreator<bool>::Build(const milvus::knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
    proto::schema::BoolArray arr;
    Helper::ParseParams(arr, data, size);
    index_->Build(arr.data().size(), arr.data().data());
}

template <>
inline ScalarIndexCreator<std::string>::ScalarIndexCreator(const char* type_params, const char* index_params) {
    // TODO: move parse-related logic to a common interface.
    Helper::ParseFromString(type_params_, std::string(type_params));
    Helper::ParseFromString(index_params_, std::string(index_params));
    // TODO: create index according to the params.
    index_ = std::make_unique<StringIndexImpl>();
}

template <>
inline void
ScalarIndexCreator<std::string>::Build(const milvus::knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
    proto::schema::StringArray arr;
    Helper::ParseParams(arr, data, size);
    // TODO: optimize here. avoid memory copy.
    std::vector<std::string> vecs{arr.data().begin(), arr.data().end()};
    index_->Build(arr.data().size(), vecs.data());
}

}  // namespace milvus::indexbuilder
