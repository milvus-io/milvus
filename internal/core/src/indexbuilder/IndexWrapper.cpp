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
#include <google/protobuf/text_format.h>

#include "pb/index_cgo_msg.pb.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/EasyAssert.h"
#include "IndexWrapper.h"
#include "indexbuilder/utils.h"

namespace milvus {
namespace indexbuilder {

IndexWrapper::IndexWrapper(const char* serialized_type_params, const char* serialized_index_params) {
    type_params_ = std::string(serialized_type_params);
    index_params_ = std::string(serialized_index_params);

    parse();

    std::map<std::string, knowhere::IndexMode> mode_map = {{"CPU", knowhere::IndexMode::MODE_CPU},
                                                           {"GPU", knowhere::IndexMode::MODE_GPU}};
    auto mode = get_config_by_name<std::string>("index_mode");
    auto index_mode = mode.has_value() ? mode_map[mode.value()] : knowhere::IndexMode::MODE_CPU;

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(get_index_type(), index_mode);
    Assert(index_ != nullptr);
}

void
IndexWrapper::parse() {
    namespace indexcgo = milvus::proto::indexcgo;
    bool deserialized_success;

    indexcgo::TypeParams type_config;
    deserialized_success = google::protobuf::TextFormat::ParseFromString(type_params_, &type_config);
    Assert(deserialized_success);

    indexcgo::IndexParams index_config;
    deserialized_success = google::protobuf::TextFormat::ParseFromString(index_params_, &index_config);
    Assert(deserialized_success);

    for (auto i = 0; i < type_config.params_size(); ++i) {
        const auto& type_param = type_config.params(i);
        const auto& key = type_param.key();
        const auto& value = type_param.value();
        type_config_[key] = value;
        config_[key] = value;
    }

    for (auto i = 0; i < index_config.params_size(); ++i) {
        const auto& index_param = index_config.params(i);
        const auto& key = index_param.key();
        const auto& value = index_param.value();
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
    auto index_type = get_index_type();
    // if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
    //     PanicInfo(std::string(index_type) + " doesn't support build without ids yet!");
    // }
    index_->Train(dataset, config_);
    index_->AddWithoutIds(dataset, config_);

    if (is_in_nm_list(index_type)) {
        auto tensor = dataset->Get<const void*>(milvus::knowhere::meta::TENSOR);
        auto row_num = dataset->Get<int64_t>(milvus::knowhere::meta::ROWS);
        auto dim = dataset->Get<int64_t>(milvus::knowhere::meta::DIM);
        int64_t data_size;
        if (is_in_bin_list(index_type)) {
            data_size = dim / 8 * row_num;
        } else {
            data_size = dim * row_num * sizeof(float);
        }
        raw_data_.resize(data_size);
        memcpy(raw_data_.data(), tensor, data_size);
    }
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
    auto binarySet = index_->Serialize(config_);
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        std::shared_ptr<uint8_t[]> raw_data(new uint8_t[raw_data_.size()], std::default_delete<uint8_t[]>());
        memcpy(raw_data.get(), raw_data_.data(), raw_data_.size());
        binarySet.Append(RAW_DATA, raw_data, raw_data_.size());
    }

    namespace indexcgo = milvus::proto::indexcgo;
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
        const auto& binary = blob_buffer.datas(i);
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto bptr = std::make_shared<milvus::knowhere::Binary>();
        bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)binary.value().c_str(), deleter);
        bptr->size = binary.value().length();
        binarySet.Append(binary.key(), bptr);
    }

    index_->Load(binarySet);
}

std::string
IndexWrapper::get_index_type() {
    // return index_->index_type();
    // knowhere bug here
    // the index_type of all ivf-based index will change to ivf flat after loaded
    auto type = get_config_by_name<std::string>("index_type");
    return type.has_value() ? type.value() : knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
}

}  // namespace indexbuilder
}  // namespace milvus
