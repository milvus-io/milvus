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
#include <exception>

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

    std::map<std::string, knowhere::IndexMode> mode_map = {{"CPU", knowhere::IndexMode::MODE_CPU},
                                                           {"GPU", knowhere::IndexMode::MODE_GPU}};
    auto type = get_config_by_name<std::string>("index_type");
    auto mode = get_config_by_name<std::string>("index_mode");
    auto index_type = type.has_value() ? type.value() : knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto index_mode = mode.has_value() ? mode_map[mode.value()] : knowhere::IndexMode::MODE_CPU;

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type, index_mode);
}

void
IndexWrapper::parse() {
    namespace indexcgo = milvus::proto::indexcgo;
    bool serialized_success;

    indexcgo::TypeParams type_config;
    serialized_success = type_config.ParseFromString(type_params_);
    Assert(serialized_success);

    indexcgo::IndexParams index_config;
    serialized_success = index_config.ParseFromString(index_params_);
    Assert(serialized_success);

    for (auto i = 0; i < type_config.params_size(); ++i) {
        auto type_param = type_config.params(i);
        auto key = type_param.key();
        auto value = type_param.value();
        type_config_[key] = value;
        config_[key] = value;
    }

    for (auto i = 0; i < index_config.params_size(); ++i) {
        auto index_param = index_config.params(i);
        auto key = index_param.key();
        auto value = index_param.value();
        index_config_[key] = value;
        config_[key] = value;
    }

    if (!config_.contains(milvus::knowhere::meta::DIM)) {
        // should raise exception here?
        PanicInfo("dim must be specific in type params or index params!");
    } else {
        auto dim = config_[milvus::knowhere::meta::DIM].get<std::string>();
        config_[milvus::knowhere::meta::DIM] = std::stoi(dim);
    }

    if (!config_.contains(milvus::knowhere::meta::TOPK)) {
    } else {
        auto topk = config_[milvus::knowhere::meta::TOPK].get<std::string>();
        config_[milvus::knowhere::meta::TOPK] = std::stoi(topk);
    }

    if (!config_.contains(milvus::knowhere::IndexParams::nlist)) {
    } else {
        auto nlist = config_[milvus::knowhere::IndexParams::nlist].get<std::string>();
        config_[milvus::knowhere::IndexParams::nlist] = std::stoi(nlist);
    }

    if (!config_.contains(milvus::knowhere::IndexParams::nprobe)) {
    } else {
        auto nprobe = config_[milvus::knowhere::IndexParams::nprobe].get<std::string>();
        config_[milvus::knowhere::IndexParams::nprobe] = std::stoi(nprobe);
    }

    if (!config_.contains(milvus::knowhere::IndexParams::nbits)) {
    } else {
        auto nbits = config_[milvus::knowhere::IndexParams::nbits].get<std::string>();
        config_[milvus::knowhere::IndexParams::nbits] = std::stoi(nbits);
    }

    if (!config_.contains(milvus::knowhere::IndexParams::m)) {
    } else {
        auto m = config_[milvus::knowhere::IndexParams::m].get<std::string>();
        config_[milvus::knowhere::IndexParams::m] = std::stoi(m);
    }

    if (!config_.contains(milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        config_[milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE] = 4;
    } else {
        auto slice_size = config_[milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<std::string>();
        config_[milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE] = std::stoi(slice_size);
    }
}

template <typename T>
std::optional<T>
IndexWrapper::get_config_by_name(std::string name) {
    if (config_.contains(name)) {
        return {config_[name].get<T>()};
    }
    return std::nullopt;
}

int64_t
IndexWrapper::dim() {
    auto dimension = get_config_by_name<int64_t>(milvus::knowhere::meta::DIM);
    Assert(dimension.has_value());
    return (dimension.value());
}

void
IndexWrapper::BuildWithoutIds(const knowhere::DatasetPtr& dataset) {
    auto index_type = index_->index_type();
    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
        PanicInfo(std::string(index_type) + " doesn't support build without ids yet!");
    }
    index_->Train(dataset, config_);
    index_->AddWithoutIds(dataset, config_);
}

void
IndexWrapper::BuildWithIds(const knowhere::DatasetPtr& dataset) {
    Assert(dataset->data().find(milvus::knowhere::meta::IDS) != dataset->data().end());
    //    index_->Train(dataset, config_);
    //    index_->Add(dataset, config_);
    index_->BuildAll(dataset, config_);
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
        binary->set_value(value->data.get(), value->size);
    }

    std::string serialized_data;
    auto ok = ret.SerializeToString(&serialized_data);
    Assert(ok);

    auto data = new char[serialized_data.length()];
    memcpy(data, serialized_data.c_str(), serialized_data.length());

    return {data, static_cast<int32_t>(serialized_data.length())};
}

void
IndexWrapper::Load(const char* serialized_sliced_blob_buffer, int32_t size) {
    namespace indexcgo = milvus::proto::indexcgo;
    auto data = std::string(serialized_sliced_blob_buffer, size);
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
