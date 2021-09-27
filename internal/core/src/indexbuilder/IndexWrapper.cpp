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
#include "exceptions/EasyAssert.h"
#include "IndexWrapper.h"
#include "indexbuilder/utils.h"
#include "index/knowhere/knowhere/index/vector_index/ConfAdapterMgr.h"
#include "index/knowhere/knowhere/common/Timer.h"
#include "index/knowhere/knowhere/common/Utils.h"

namespace milvus {
namespace indexbuilder {

IndexWrapper::IndexWrapper(const char* serialized_type_params, const char* serialized_index_params) {
    type_params_ = std::string(serialized_type_params);
    index_params_ = std::string(serialized_index_params);

    parse();

    auto index_mode = get_index_mode();
    auto index_type = get_index_type();
    auto metric_type = get_metric_type();
    AssertInfo(!is_unsupported(index_type, metric_type), index_type + " doesn't support metric: " + metric_type);

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(get_index_type(), index_mode);
    AssertInfo(index_ != nullptr, "[IndexWrapper]Index is null after create index");
}

template <typename ParamsT>  // ugly here, ParamsT will just be MapParams later
void
IndexWrapper::parse_impl(const std::string& serialized_params_str, knowhere::Config& conf) {
    bool deserialized_success;

    ParamsT params;
    deserialized_success = google::protobuf::TextFormat::ParseFromString(serialized_params_str, &params);
    AssertInfo(deserialized_success, "[IndexWrapper]Deserialize params failed");

    for (auto i = 0; i < params.params_size(); ++i) {
        const auto& param = params.params(i);
        const auto& key = param.key();
        const auto& value = param.value();
        conf[key] = value;
    }

    auto stoi_closure = [](const std::string& s) -> auto {
        return std::stoi(s);
    };
    auto stof_closure = [](const std::string& s) -> auto {
        return std::stof(s);
    };

    /***************************** meta *******************************/
    check_parameter<int>(conf, milvus::knowhere::meta::DIM, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::meta::TOPK, stoi_closure, std::nullopt);

    /***************************** IVF Params *******************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::nprobe, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::nlist, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::m, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::nbits, stoi_closure, std::nullopt);

    /************************** NSG Parameter **************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::knng, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::search_length, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::out_degree, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::candidate, stoi_closure, std::nullopt);

    /************************** HNSW Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::efConstruction, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::M, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::ef, stoi_closure, std::nullopt);

    /************************** Annoy Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::n_trees, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::search_k, stoi_closure, std::nullopt);

    /************************** PQ Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::PQM, stoi_closure, std::nullopt);

    /************************** NGT Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::edge_size, stoi_closure, std::nullopt);

    /************************** NGT Search Params *****************************/
    check_parameter<float>(conf, milvus::knowhere::IndexParams::epsilon, stof_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::max_search_edges, stoi_closure, std::nullopt);

    /************************** NGT_PANNG Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::forcedly_pruned_edge_size, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::selectively_pruned_edge_size, stoi_closure, std::nullopt);

    /************************** NGT_ONNG Params *****************************/
    check_parameter<int>(conf, milvus::knowhere::IndexParams::outgoing_edge_size, stoi_closure, std::nullopt);
    check_parameter<int>(conf, milvus::knowhere::IndexParams::incoming_edge_size, stoi_closure, std::nullopt);

    /************************** Serialize Params *******************************/
    check_parameter<int>(conf, milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, stoi_closure, std::optional{4});
}

void
IndexWrapper::parse() {
    namespace indexcgo = milvus::proto::indexcgo;

    parse_impl<indexcgo::TypeParams>(type_params_, type_config_);
    parse_impl<indexcgo::IndexParams>(index_params_, index_config_);

    config_.update(type_config_);  // just like dict().update in Python, amazing
    config_.update(index_config_);
}

template <typename T>
void
IndexWrapper::check_parameter(knowhere::Config& conf,
                              const std::string& key,
                              std::function<T(std::string)> fn,
                              std::optional<T> default_v) {
    if (!conf.contains(key)) {
        if (default_v.has_value()) {
            conf[key] = default_v.value();
        }
    } else {
        auto value = conf[key];
        conf[key] = fn(value);
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
    AssertInfo(dimension.has_value(), "[IndexWrapper]Dimension doesn't have value");
    return (dimension.value());
}

void
IndexWrapper::BuildWithoutIds(const knowhere::DatasetPtr& dataset) {
    auto index_type = get_index_type();
    auto index_mode = get_index_mode();
    config_[knowhere::meta::ROWS] = dataset->Get<int64_t>(knowhere::meta::ROWS);
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        if (!config_.contains(knowhere::IndexParams::nbits)) {
            config_[knowhere::IndexParams::nbits] = 8;
        }
    }
    auto conf_adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_type);
    std::cout << "config_ when build index: " << config_ << std::endl;
    AssertInfo(conf_adapter->CheckTrain(config_, index_mode), "something wrong in index parameters!");

    if (is_in_need_id_list(index_type)) {
        PanicInfo(std::string(index_type) + " doesn't support build without ids yet!");
    }
    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    // if (is_in_need_build_all_list(index_type)) {
    //     index_->BuildAll(dataset, config_);
    // } else {
    //     index_->Train(dataset, config_);
    //     index_->AddWithoutIds(dataset, config_);
    // }
    index_->BuildAll(dataset, config_);
    rc.RecordSection("TrainAndAdd");

    if (is_in_nm_list(index_type)) {
        StoreRawData(dataset);
        rc.RecordSection("StoreRawData");
    }
    rc.ElapseFromBegin("Done");
}

void
IndexWrapper::BuildWithIds(const knowhere::DatasetPtr& dataset) {
    AssertInfo(dataset->data().find(milvus::knowhere::meta::IDS) != dataset->data().end(),
               "[IndexWrapper]Can't find ids field in dataset");
    auto index_type = get_index_type();
    auto index_mode = get_index_mode();
    config_[knowhere::meta::ROWS] = dataset->Get<int64_t>(knowhere::meta::ROWS);
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        if (!config_.contains(knowhere::IndexParams::nbits)) {
            config_[knowhere::IndexParams::nbits] = 8;
        }
    }
    auto conf_adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_type);
    AssertInfo(conf_adapter->CheckTrain(config_, index_mode), "something wrong in index parameters!");
    //    index_->Train(dataset, config_);
    //    index_->Add(dataset, config_);
    index_->BuildAll(dataset, config_);

    if (is_in_nm_list(get_index_type())) {
        StoreRawData(dataset);
    }
}

void
IndexWrapper::StoreRawData(const knowhere::DatasetPtr& dataset) {
    auto index_type = get_index_type();
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

/*
 * brief Return serialized binary set
 * TODO: use a more efficient method to manage memory, consider std::vector later
 */
std::unique_ptr<IndexWrapper::Binary>
IndexWrapper::Serialize() {
    auto binarySet = index_->Serialize(config_);
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        std::shared_ptr<uint8_t[]> raw_data(new uint8_t[raw_data_.size()], std::default_delete<uint8_t[]>());
        memcpy(raw_data.get(), raw_data_.data(), raw_data_.size());
        binarySet.Append(RAW_DATA, raw_data, raw_data_.size());
        auto slice_size = get_index_file_slice_size();
        // https://github.com/milvus-io/milvus/issues/6421
        // Disassemble will only divide the raw vectors, other keys was already divided
        knowhere::Disassemble(slice_size * 1024 * 1024, binarySet);
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
    AssertInfo(ok, "[IndexWrapper]Can't serialize data to string");

    auto binary = std::make_unique<IndexWrapper::Binary>();
    binary->data.resize(serialized_data.length());
    memcpy(binary->data.data(), serialized_data.c_str(), serialized_data.length());

    return binary;
}

void
IndexWrapper::Load(const char* serialized_sliced_blob_buffer, int32_t size) {
    namespace indexcgo = milvus::proto::indexcgo;
    auto data = std::string(serialized_sliced_blob_buffer, size);
    indexcgo::BinarySet blob_buffer;

    auto ok = blob_buffer.ParseFromString(data);
    AssertInfo(ok, "[IndexWrapper]Can't parse data from string to blob_buffer");

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

std::string
IndexWrapper::get_metric_type() {
    auto type = get_config_by_name<std::string>(knowhere::Metric::TYPE);
    if (type.has_value()) {
        return type.value();
    } else {
        auto index_type = get_index_type();
        if (is_in_bin_list(index_type)) {
            return knowhere::Metric::JACCARD;
        } else {
            return knowhere::Metric::L2;
        }
    }
}

knowhere::IndexMode
IndexWrapper::get_index_mode() {
    static std::map<std::string, knowhere::IndexMode> mode_map = {
        {"CPU", knowhere::IndexMode::MODE_CPU},
        {"GPU", knowhere::IndexMode::MODE_GPU},
    };
    auto mode = get_config_by_name<std::string>("index_mode");
    return mode.has_value() ? mode_map[mode.value()] : knowhere::IndexMode::MODE_CPU;
}

int64_t
IndexWrapper::get_index_file_slice_size() {
    if (config_.contains(knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        return config_[knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>();
    }
    return 4;  // by default
}

std::unique_ptr<IndexWrapper::QueryResult>
IndexWrapper::Query(const knowhere::DatasetPtr& dataset) {
    return std::move(QueryImpl(dataset, config_));
}

std::unique_ptr<IndexWrapper::QueryResult>
IndexWrapper::QueryWithParam(const knowhere::DatasetPtr& dataset, const char* serialized_search_params) {
    namespace indexcgo = milvus::proto::indexcgo;
    milvus::knowhere::Config search_conf;
    parse_impl<indexcgo::MapParams>(std::string(serialized_search_params), search_conf);

    return std::move(QueryImpl(dataset, search_conf));
}

std::unique_ptr<IndexWrapper::QueryResult>
IndexWrapper::QueryImpl(const knowhere::DatasetPtr& dataset, const knowhere::Config& conf) {
    auto load_raw_data_closure = [&]() { LoadRawData(); };  // hide this pointer
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        std::call_once(raw_data_loaded_, load_raw_data_closure);
    }

    auto res = index_->Query(dataset, conf, nullptr);
    auto ids = res->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto distances = res->Get<float*>(milvus::knowhere::meta::DISTANCE);
    auto nq = dataset->Get<int64_t>(milvus::knowhere::meta::ROWS);
    auto k = config_[milvus::knowhere::meta::TOPK].get<int64_t>();

    auto query_res = std::make_unique<IndexWrapper::QueryResult>();
    query_res->nq = nq;
    query_res->topk = k;
    query_res->ids.resize(nq * k);
    query_res->distances.resize(nq * k);
    memcpy(query_res->ids.data(), ids, sizeof(int64_t) * nq * k);
    memcpy(query_res->distances.data(), distances, sizeof(float) * nq * k);

    return std::move(query_res);
}

void
IndexWrapper::LoadRawData() {
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        auto bs = index_->Serialize(config_);
        auto bptr = std::make_shared<milvus::knowhere::Binary>();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        bptr->data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);
        bptr->size = raw_data_.size();
        bs.Append(RAW_DATA, bptr);
        index_->Load(bs);
    }
}

}  // namespace indexbuilder
}  // namespace milvus
