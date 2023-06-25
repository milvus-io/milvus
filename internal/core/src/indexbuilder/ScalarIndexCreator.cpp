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
#include "pb/index_cgo_msg.pb.h"

#include <string>

namespace milvus::indexbuilder {

ScalarIndexCreator::ScalarIndexCreator(DataType dtype,
                                       Config& config,
                                       storage::FileManagerImplPtr file_manager)
    : dtype_(dtype), config_(config) {
    milvus::index::CreateIndexInfo index_info;
    index_info.field_type = dtype_;
    index_info.index_type = index_type();
    index_ = index::IndexFactory::GetInstance().CreateIndex(index_info,
                                                            file_manager);
}

void
ScalarIndexCreator::Build(const milvus::DatasetPtr& dataset) {
    auto size = dataset->GetRows();
    auto data = dataset->GetTensor();
    index_->BuildWithRawData(size, data);
}

void
ScalarIndexCreator::Build() {
    index_->Build(config_);
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

BinarySet
ScalarIndexCreator::Upload() {
    return index_->Upload();
}

}  // namespace milvus::indexbuilder
