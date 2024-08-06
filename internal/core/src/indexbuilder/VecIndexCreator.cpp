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

#include <map>
#include <utility>

#include "common/EasyAssert.h"
#include "indexbuilder/VecIndexCreator.h"
#include "index/Utils.h"
#include "index/IndexFactory.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::indexbuilder {

VecIndexCreator::VecIndexCreator(
    DataType data_type,
    Config& config,
    const storage::FileManagerContext& file_manager_context)
    : VecIndexCreator(data_type, "", 0, config, file_manager_context) {
}

VecIndexCreator::VecIndexCreator(
    DataType data_type,
    const std::string& field_name,
    const int64_t dim,
    Config& config,
    const storage::FileManagerContext& file_manager_context)
    : config_(config), data_type_(data_type) {
    index::CreateIndexInfo index_info;
    index_info.field_type = data_type_;
    index_info.index_type = index::GetIndexTypeFromConfig(config_);
    index_info.metric_type = index::GetMetricTypeFromConfig(config_);
    index_info.field_name = field_name;
    index_info.index_engine_version =
        index::GetIndexEngineVersionFromConfig(config_);
    index_info.dim = dim;

    index_ = index::IndexFactory::GetInstance().CreateIndex(
        index_info, file_manager_context);
    AssertInfo(index_ != nullptr,
               "[VecIndexCreator]Index is null after create index");
}

int64_t
VecIndexCreator::dim() {
    return index::GetDimFromConfig(config_);
}

void
VecIndexCreator::Build(const milvus::DatasetPtr& dataset) {
    index_->BuildWithDataset(dataset, config_);
}

void
VecIndexCreator::Build() {
    index_->Build(config_);
}

milvus::BinarySet
VecIndexCreator::Serialize() {
    return index_->Serialize(config_);
}

void
VecIndexCreator::Load(const milvus::BinarySet& binary_set) {
    index_->Load(binary_set, config_);
}

std::unique_ptr<SearchResult>
VecIndexCreator::Query(const milvus::DatasetPtr& dataset,
                       const SearchInfo& search_info,
                       const BitsetView& bitset) {
    auto vector_index = dynamic_cast<index::VectorIndex*>(index_.get());
    auto search_result = std::make_unique<SearchResult>();
    vector_index->Query(dataset, search_info, bitset, *search_result);
    return search_result;
}

BinarySet
VecIndexCreator::Upload() {
    return index_->Upload();
}

void
VecIndexCreator::CleanLocalData() {
    auto vector_index = dynamic_cast<index::VectorIndex*>(index_.get());
    vector_index->CleanLocalData();
}

}  // namespace milvus::indexbuilder
