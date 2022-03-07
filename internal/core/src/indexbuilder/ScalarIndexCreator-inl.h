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
#include "indexbuilder/common.h"
#include <string>
#include <memory>

namespace milvus::indexbuilder {

template <typename T>
ScalarIndexCreator<T>::ScalarIndexCreator(const char* type_params, const char* index_params) {
    Helper::ParseFromString(type_params_, std::string(type_params));
    Helper::ParseFromString(index_params_, std::string(index_params));
    // TODO: create index according to the params.
    index_ = std::make_shared<knowhere::scalar::StructuredIndexSort<T>>();
}

template <typename T>
void
ScalarIndexCreator<T>::Build(const knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
    index_->Build(size, static_cast<const T*>(data));
}

template <typename T>
std::unique_ptr<knowhere::BinarySet>
ScalarIndexCreator<T>::Serialize() {
    return std::make_unique<knowhere::BinarySet>(index_->Serialize(config_));
}

// not sure that the pointer of a golang bool array acts like other types.
template <>
void
ScalarIndexCreator<bool>::Build(const milvus::knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const char*>(knowhere::meta::TENSOR);
    proto::schema::BoolArray arr;
    arr.ParseFromArray(data, size);
    index_->Build(arr.data().size(), arr.data().data());
}

template <>
void
ScalarIndexCreator<std::string>::Build(const milvus::knowhere::DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const char*>(knowhere::meta::TENSOR);
    proto::schema::StringArray arr;
    arr.ParseFromArray(data, size);
    auto p = (std::string*)(arr.data().data());
    index_->Build(arr.data().size(), p);
}

}  // namespace milvus::indexbuilder
