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

#include <exception>
#include <map>
#include <google/protobuf/text_format.h>

#include "exceptions/EasyAssert.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/utils.h"
#include "knowhere/common/Timer.h"
#include "knowhere/common/Utils.h"
#include "knowhere/index/VecIndexFactory.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::indexbuilder {

VecIndexCreator::VecIndexCreator(const char* serialized_type_params, const char* serialized_index_params) {
    type_params_ = std::string(serialized_type_params);
    index_params_ = std::string(serialized_index_params);

    parse();

    auto index_mode = get_index_mode();
    auto index_type = get_index_type();
    auto metric_type = get_metric_type();
    AssertInfo(!is_unsupported(index_type, metric_type), index_type + " doesn't support metric: " + metric_type);

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(get_index_type(), index_mode);
    AssertInfo(index_ != nullptr, "[VecIndexCreator]Index is null after create index");
}

template <typename ParamsT>
// ugly here, ParamsT will just be MapParams later
void
VecIndexCreator::parse_impl(const std::string& serialized_params_str, knowhere::Config& conf) {
    bool deserialized_success;

    ParamsT params;
    deserialized_success = google::protobuf::TextFormat::ParseFromString(serialized_params_str, &params);
    AssertInfo(deserialized_success, "[VecIndexCreator]Deserialize params failed");

    for (auto i = 0; i < params.params_size(); ++i) {
        const auto& param = params.params(i);
        const auto& key = param.key();
        const auto& value = param.value();
        conf[key] = value;
    }

    auto stoi_closure = [](const std::string_view& s) -> int { return std::stoi(std::string(s)); };
    auto stof_closure = [](const std::string_view& s) -> float { return std::stof(std::string(s)); };

    /***************************** meta *******************************/
    check_parameter<int>(conf, knowhere::meta::DIM, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::meta::TOPK, stoi_closure, std::nullopt);

    /***************************** IVF Params *******************************/
    check_parameter<int>(conf, knowhere::indexparam::NPROBE, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::NLIST, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::M, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::NBITS, stoi_closure, std::nullopt);

    /************************** PQ Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::PQ_M, stoi_closure, std::nullopt);

#ifdef MILVUS_SUPPORT_NSG
    /************************** NSG Parameter **************************/
    check_parameter<int>(conf, knowhere::indexparam::KNNG, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::SEARCH_LENGTH, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::OUT_DEGREE, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::CANDIDATE, stoi_closure, std::nullopt);
#endif

    /************************** HNSW Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::EFCONSTRUCTION, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::HNSW_M, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::EF, stoi_closure, std::nullopt);

    /************************** Annoy Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::N_TREES, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::SEARCH_K, stoi_closure, std::nullopt);

#ifdef MILVUS_SUPPORT_NGT
    /************************** NGT Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::EDGE_SIZE, stoi_closure, std::nullopt);

    /************************** NGT Search Params *****************************/
    check_parameter<float>(conf, knowhere::indexparam::EPSILON, stof_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::MAX_SEARCH_EDGES, stoi_closure, std::nullopt);

    /************************** NGT_PANNG Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::FORCEDLY_PRUNED_EDGE_SIZE, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::SELECTIVELY_PRUNED_EDGE_SIZE, stoi_closure, std::nullopt);

    /************************** NGT_ONNG Params *****************************/
    check_parameter<int>(conf, knowhere::indexparam::OUTGOING_EDGE_SIZE, stoi_closure, std::nullopt);
    check_parameter<int>(conf, knowhere::indexparam::INCOMING_EDGE_SIZE, stoi_closure, std::nullopt);
#endif

    /************************** Serialize Params *******************************/
    check_parameter<int>(conf, knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, stoi_closure, std::optional{4});
}

void
VecIndexCreator::parse() {
    namespace indexcgo = milvus::proto::indexcgo;

    parse_impl<indexcgo::TypeParams>(type_params_, type_config_);
    parse_impl<indexcgo::IndexParams>(index_params_, index_config_);

    config_.update(type_config_);  // just like dict().update in Python, amazing
    config_.update(index_config_);
}

template <typename T>
void
VecIndexCreator::check_parameter(knowhere::Config& conf,
                                 const std::string_view& key,
                                 std::function<T(std::string)> fn,
                                 std::optional<T> default_v) {
    if (!conf.contains(key)) {
        if (default_v.has_value()) {
            conf[std::string(key)] = default_v.value();
        }
    } else {
        auto value = conf[std::string(key)];
        conf[std::string(key)] = fn(value);
    }
}

template <typename T>
std::optional<T>
VecIndexCreator::get_config_by_name(std::string_view name) {
    if (config_.contains(name)) {
        return knowhere::GetValueFromConfig<T>(config_, std::string(name));
    }
    return std::nullopt;
}

int64_t
VecIndexCreator::dim() {
    auto dimension = get_config_by_name<int64_t>(knowhere::meta::DIM);
    AssertInfo(dimension.has_value(), "[VecIndexCreator]Dimension doesn't have value");
    return (dimension.value());
}

void
VecIndexCreator::BuildWithoutIds(const knowhere::DatasetPtr& dataset) {
    auto index_type = get_index_type();
    auto index_mode = get_index_mode();
    knowhere::SetMetaRows(config_, dataset->Get<int64_t>(knowhere::meta::ROWS));
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        if (!config_.contains(knowhere::indexparam::NBITS)) {
            knowhere::SetIndexParamNbits(config_, 8);
        }
    }
    auto conf_adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_type);
    // TODO: Use easylogging instead, if you really need to keep this log.
    // std::cout << "Konwhere BuildWithoutIds config_ is " << config_ << std::endl;
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
VecIndexCreator::BuildWithIds(const knowhere::DatasetPtr& dataset) {
    AssertInfo(dataset->data().find(std::string(knowhere::meta::IDS)) != dataset->data().end(),
               "[VecIndexCreator]Can't find ids field in dataset");
    auto index_type = get_index_type();
    auto index_mode = get_index_mode();
    knowhere::SetMetaRows(config_, dataset->Get<int64_t>(knowhere::meta::ROWS));
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        if (!config_.contains(knowhere::indexparam::NBITS)) {
            knowhere::SetIndexParamNbits(config_, 8);
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
VecIndexCreator::StoreRawData(const knowhere::DatasetPtr& dataset) {
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        auto tensor = dataset->Get<const void*>(knowhere::meta::TENSOR);
        auto row_num = dataset->Get<int64_t>(knowhere::meta::ROWS);
        auto dim = dataset->Get<int64_t>(knowhere::meta::DIM);
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

knowhere::BinarySet
VecIndexCreator::Serialize() {
    auto ret = index_->Serialize(config_);
    auto index_type = get_index_type();

    if (is_in_nm_list(index_type)) {
        std::shared_ptr<uint8_t[]> raw_data(new uint8_t[raw_data_.size()], std::default_delete<uint8_t[]>());
        memcpy(raw_data.get(), raw_data_.data(), raw_data_.size());
        ret.Append(RAW_DATA, raw_data, raw_data_.size());
        // https://github.com/milvus-io/milvus/issues/6421
        // Disassemble will only divide the raw vectors, other keys were already divided
        knowhere::Disassemble(ret, config_);
    }
    return ret;
}

void
VecIndexCreator::Load(const knowhere::BinarySet& binary_set) {
    auto& map_ = binary_set.binary_map_;
    for (auto it = map_.begin(); it != map_.end(); ++it) {
        if (it->first == RAW_DATA) {
            raw_data_.clear();
            auto data_size = it->second->size;
            raw_data_.resize(data_size);
            memcpy(raw_data_.data(), it->second->data.get(), data_size);
            break;
        }
    }
    index_->Load(binary_set);
}

std::string
VecIndexCreator::get_index_type() {
    // return index_->index_type();
    // knowhere bug here
    // the index_type of all ivf-based index will change to ivf flat after loaded
    auto type = get_config_by_name<std::string>("index_type");
    return type.has_value() ? type.value() : std::string(knowhere::IndexEnum::INDEX_FAISS_IVFPQ);
}

std::string
VecIndexCreator::get_metric_type() {
    auto type = get_config_by_name<std::string>(knowhere::meta::METRIC_TYPE);
    if (type.has_value()) {
        return type.value();
    } else {
        auto index_type = get_index_type();
        if (is_in_bin_list(index_type)) {
            return std::string(knowhere::metric::JACCARD);
        } else {
            return std::string(knowhere::metric::L2);
        }
    }
}

knowhere::IndexMode
VecIndexCreator::get_index_mode() {
    static std::map<std::string, knowhere::IndexMode> mode_map = {
        {"CPU", knowhere::IndexMode::MODE_CPU},
        {"GPU", knowhere::IndexMode::MODE_GPU},
    };
    auto mode = get_config_by_name<std::string>("index_mode");
    return mode.has_value() ? mode_map[mode.value()] : knowhere::IndexMode::MODE_CPU;
}

int64_t
VecIndexCreator::get_index_file_slice_size() {
    if (config_.contains(knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        return config_[knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>();
    }
    return knowhere::index_file_slice_size;  // by default
}

std::unique_ptr<VecIndexCreator::QueryResult>
VecIndexCreator::Query(const knowhere::DatasetPtr& dataset) {
    return std::move(QueryImpl(dataset, config_));
}

std::unique_ptr<VecIndexCreator::QueryResult>
VecIndexCreator::QueryWithParam(const knowhere::DatasetPtr& dataset, const char* serialized_search_params) {
    namespace indexcgo = milvus::proto::indexcgo;
    knowhere::Config search_conf;
    parse_impl<indexcgo::MapParams>(std::string(serialized_search_params), search_conf);

    return std::move(QueryImpl(dataset, search_conf));
}

std::unique_ptr<VecIndexCreator::QueryResult>
VecIndexCreator::QueryImpl(const knowhere::DatasetPtr& dataset, const knowhere::Config& conf) {
    auto load_raw_data_closure = [&]() { LoadRawData(); };  // hide this pointer
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        std::call_once(raw_data_loaded_, load_raw_data_closure);
    }

    auto res = index_->Query(dataset, conf, nullptr);
    auto ids = res->Get<int64_t*>(knowhere::meta::IDS);
    auto distances = res->Get<float*>(knowhere::meta::DISTANCE);
    auto nq = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto k = knowhere::GetMetaTopk(config_);

    auto query_res = std::make_unique<VecIndexCreator::QueryResult>();
    query_res->nq = nq;
    query_res->topk = k;
    query_res->ids.resize(nq * k);
    query_res->distances.resize(nq * k);
    memcpy(query_res->ids.data(), ids, sizeof(int64_t) * nq * k);
    memcpy(query_res->distances.data(), distances, sizeof(float) * nq * k);

    return std::move(query_res);
}

void
VecIndexCreator::LoadRawData() {
    auto index_type = get_index_type();
    if (is_in_nm_list(index_type)) {
        auto bs = index_->Serialize(config_);
        auto bptr = std::make_shared<knowhere::Binary>();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        bptr->data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);
        bptr->size = raw_data_.size();
        bs.Append(RAW_DATA, bptr);
        index_->Load(bs);
    }
}

}  // namespace milvus::indexbuilder
