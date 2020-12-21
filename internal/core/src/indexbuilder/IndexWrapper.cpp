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

#include "pb/index_cgo_msg.pb.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/EasyAssert.h"
#include "IndexWrapper.h"

namespace milvus {
namespace indexbuilder {

IndexWrapper::IndexWrapper(const char* serialized_type_params, const char* serialized_index_params) {
    type_params_ = std::string(serialized_type_params);
    index_params_ = std::string(serialized_index_params);

    parse();

    auto index_type = index_config_["index_type"].get<std::string>();
    auto index_mode = index_config_["index_mode"].get<std::string>();
    auto mode = index_mode == "CPU" ? knowhere::IndexMode::MODE_CPU : knowhere::IndexMode::MODE_GPU;

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type, mode);
}

void
IndexWrapper::parse() {
    namespace indexcgo = milvus::proto::indexcgo;
    bool serialized_success;

    indexcgo::BinarySet type_config;
    serialized_success = type_config.ParseFromString(type_params_);
    Assert(serialized_success);

    indexcgo::BinarySet index_config;
    serialized_success = index_config.ParseFromString(index_params_);
    Assert(serialized_success);

    for (auto i = 0; i < type_config.datas_size(); ++i) {
        auto binary = type_config.datas(i);
        type_config_[binary.key()] = binary.value();
    }

    for (auto i = 0; i < index_config.datas_size(); ++i) {
        auto binary = index_config.datas(i);
        index_config_[binary.key()] = binary.value();
    }

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

/*
 * brief Return serialized binary set
 */
milvus::indexbuilder::IndexWrapper::Binary
IndexWrapper::Serialize() {
    namespace indexcgo = milvus::proto::indexcgo;
    auto binarySet = index_->Serialize(config_);
    indexcgo::BinarySet ret;

    for (auto [key, value] : binarySet.binary_map_) {
        auto binary = ret.add_datas();
        binary->set_key(key);
        binary->set_value(reinterpret_cast<char*>(value->data.get()));
    }

    std::string serialized_data;
    auto ok = ret.SerializeToString(&serialized_data);
    Assert(ok);

    auto data = new char[serialized_data.length() + 1];
    memcpy(data, serialized_data.c_str(), serialized_data.length());
    data[serialized_data.length()] = 0;

    return {data, static_cast<int32_t>(serialized_data.length() + 1)};
}

void
IndexWrapper::Load(const char* serialized_sliced_blob_buffer) {
    namespace indexcgo = milvus::proto::indexcgo;
    auto data = std::string(serialized_sliced_blob_buffer);
    indexcgo::BinarySet blob_buffer;

    auto ok = blob_buffer.ParseFromString(data);
    Assert(ok);

    milvus::knowhere::BinarySet binarySet;
    for (auto i = 0; i < blob_buffer.datas_size(); i++) {
        auto binary = blob_buffer.datas(i);
        std::shared_ptr<uint8_t[]> binary_data(new uint8_t[binary.value().length() + 1],
                                               std::default_delete<uint8_t[]>());
        memcpy(binary_data.get(), binary.value().c_str(), binary.value().length());
        binary_data[binary.value().length()] = 0;
        binarySet.Append(binary.key(), binary_data, binary.value().length() + 1);
    }

    index_->Load(binarySet);
}

}  // namespace indexbuilder
}  // namespace milvus
