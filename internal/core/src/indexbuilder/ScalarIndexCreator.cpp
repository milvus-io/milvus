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
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "pb/index_cgo_msg.pb.h"

#include <string>
#include <utility>

namespace milvus::indexbuilder {

ScalarIndexCreator::ScalarIndexCreator(
    DataType dtype,
    Config& config,
    const storage::FileManagerContext& file_manager_context)
    : config_(config), dtype_(dtype) {
    milvus::index::CreateIndexInfo index_info;
    if (config.contains("index_type")) {
        index_type_ = config.at("index_type").get<std::string>();
    }
    // Config should have value for milvus::index::SCALAR_INDEX_ENGINE_VERSION for production calling chain.
    // Use value_or(1) for unit test without setting this value
    index_info.scalar_index_engine_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);

    index_info.field_type = dtype_;
    index_info.index_type = index_type();
    if (dtype == DataType::JSON) {
        index_info.json_cast_type = static_cast<DataType>(
            std::stoi(config.at(JSON_CAST_TYPE).get<std::string>()));
        index_info.json_path = config.at(JSON_PATH).get<std::string>();
    }
    index_ = index::IndexFactory::GetInstance().CreateIndex(
        index_info, file_manager_context);
}

void
ScalarIndexCreator::Build(const milvus::DatasetPtr& dataset) {
    auto size = dataset->GetRows();
    auto data = dataset->GetTensor();
    index_->BuildWithRawDataForUT(size, data);
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
    return index_type_;
}

index::IndexStatsPtr
ScalarIndexCreator::Upload() {
    return index_->Upload();
}
}  // namespace milvus::indexbuilder
