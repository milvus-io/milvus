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

#include "indexbuilder/helper.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "index/IndexFactory.h"
#include "indexbuilder/parse.h"

#include <string>

namespace milvus::indexbuilder {

ScalarIndexCreator::ScalarIndexCreator(CDataType dtype, const char* type_params, const char* index_params) {
    dtype_ = dtype;
    // TODO: move parse-related logic to a common interface.
    Helper::ParseFromString(type_params_, std::string(type_params));
    Helper::ParseFromString(index_params_, std::string(index_params));
    // TODO: create index according to the params.
    index_ = scalar::IndexFactory::GetInstance().CreateIndex(dtype_, index_type());
    parseConfig();
}

void
ScalarIndexCreator::parseConfig() {
    AddToConfig(config_, type_params_);
    AddToConfig(config_, index_params_);

    auto stoi_closure = [](const std::string& s) -> int { return std::stoi(s); };
    ParseConfig<int>(config_, knowhere::meta::SLICE_SIZE, stoi_closure, std::optional{DEFAULT_INDEX_SLICE_SIZE});
}

void
ScalarIndexCreator::Build(const knowhere::DatasetPtr& dataset) {
    index_->BuildWithDataset(dataset);
}

knowhere::BinarySet
ScalarIndexCreator::Serialize() {
    return index_->Serialize(config_);
}

void
ScalarIndexCreator::Load(const knowhere::BinarySet& binary_set) {
    index_->Load(binary_set);
}

std::string
ScalarIndexCreator::index_type() {
    // TODO
    return "sort";
}

}  // namespace milvus::indexbuilder
