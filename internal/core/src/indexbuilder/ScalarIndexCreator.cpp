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

#include "indexbuilder/ScalarIndexCreator.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/Utils.h"

#include <string>

namespace milvus::indexbuilder {

ScalarIndexCreator::ScalarIndexCreator(DataType dtype, const char* type_params, const char* index_params)
    : dtype_(dtype) {
    // TODO: move parse-related logic to a common interface.
    milvus::index::ParseFromString(type_params_, std::string(type_params));
    milvus::index::ParseFromString(index_params_, std::string(index_params));

    for (auto i = 0; i < type_params_.params_size(); ++i) {
        const auto& param = type_params_.params(i);
        config_[param.key()] = param.value();
    }

    for (auto i = 0; i < index_params_.params_size(); ++i) {
        const auto& param = index_params_.params(i);
        config_[param.key()] = param.value();
    }

    milvus::index::CreateIndexInfo index_info;
    index_info.field_type = dtype_;
    index_info.index_type = index_type();
    index_info.index_mode = IndexMode::MODE_CPU;
    index_ = index::IndexFactory::GetInstance().CreateIndex(index_info, nullptr);
}

void
ScalarIndexCreator::Build(const milvus::DatasetPtr& dataset) {
    auto size = knowhere::GetDatasetRows(dataset);
    auto data = knowhere::GetDatasetTensor(dataset);
    index_->BuildWithRawData(size, data);
}

milvus::BinarySet
ScalarIndexCreator::Serialize() {
    return index_->Serialize(config_);
}

void
ScalarIndexCreator::Load(const milvus::BinarySet& binary_set) {
    index_->Load(binary_set);
}

std::string
ScalarIndexCreator::index_type() {
    // TODO
    return "sort";
}

}  // namespace milvus::indexbuilder
