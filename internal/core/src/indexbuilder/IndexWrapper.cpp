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

#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "IndexWrapper.h"

namespace milvus {
namespace indexbuilder {

IndexWrapper::IndexWrapper(const char* type_params_str, const char* index_params_str) {
    type_params_ = std::string(type_params_str);
    index_params_ = std::string(index_params_str);
    parse();

    auto index_type = index_config_["index_type"].get<std::string>();
    auto index_mode = index_config_["index_mode"].get<std::string>();
    auto mode = index_mode == "CPU" ? knowhere::IndexMode::MODE_CPU : knowhere::IndexMode::MODE_GPU;

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type, mode);
}

void
IndexWrapper::parse() {
    type_config_ = milvus::json::parse(type_params_);
    index_config_ = milvus::json::parse(index_params_);

    // TODO: parse from type_params & index_params
    auto dim = 128;
    config_ = knowhere::Config{
        {knowhere::meta::DIM, dim},         {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::nprobe, 4}, {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},  {knowhere::Metric::TYPE, knowhere::Metric::L2},
        {knowhere::meta::DEVICEID, 0},
    };
}

int64_t
IndexWrapper::dim() {
    // TODO: get from config
    return 128;
}

void
IndexWrapper::BuildWithoutIds(const knowhere::DatasetPtr& dataset) {
    index_->Train(dataset, config_);
    index_->AddWithoutIds(dataset, config_);
}

char*
IndexWrapper::Serialize() {
    return (char*)malloc(1);
}

void
IndexWrapper::Load(const char* dumped_blob_buffer) {
    return;
}

}  // namespace indexbuilder
}  // namespace milvus
