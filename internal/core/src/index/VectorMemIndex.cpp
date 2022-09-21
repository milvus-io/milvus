// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/VectorMemIndex.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "exceptions/EasyAssert.h"
#include "config/ConfigKnowhere.h"

#include "knowhere/index/VecIndexFactory.h"
#include "knowhere/common/Timer.h"
#include "common/BitsetView.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::index {

VectorMemIndex::VectorMemIndex(const IndexType& index_type, const MetricType& metric_type, const IndexMode& index_mode)
    : VectorIndex(index_type, index_mode, metric_type) {
    AssertInfo(!is_unsupported(index_type, metric_type), index_type + " doesn't support metric: " + metric_type);

    index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(GetIndexType(), index_mode);
    AssertInfo(index_ != nullptr, "[VecIndexCreator]Index is null after create index");
}

BinarySet
VectorMemIndex::Serialize(const Config& config) {
    knowhere::Config serialize_config = config;
    parse_config(serialize_config);

    auto ret = index_->Serialize(serialize_config);
    auto index_type = GetIndexType();

    if (is_in_nm_list(index_type)) {
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto raw_data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);

        //        std::shared_ptr<uint8_t[]> raw_data(new uint8_t[raw_data_.size()], std::default_delete<uint8_t[]>());
        //        memcpy(raw_data.get(), raw_data_.data(), raw_data_.size());
        ret.Append(RAW_DATA, raw_data, raw_data_.size());
        // Disassemble will only divide the raw vectors, other keys were already divided
        knowhere::Disassemble(ret, serialize_config);
    }
    return ret;
}

void
VectorMemIndex::Load(const BinarySet& binary_set, const Config& config) {
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
    SetDim(index_->Dim());
}

void
VectorMemIndex::BuildWithDataset(const DatasetPtr& dataset, const Config& config) {
    knowhere::Config index_config;
    index_config.update(config);
    parse_config(index_config);

    SetDim(knowhere::GetDatasetDim(dataset));
    knowhere::SetMetaRows(index_config, knowhere::GetDatasetRows(dataset));
    if (GetIndexType() == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        if (!config.contains(knowhere::indexparam::NBITS)) {
            knowhere::SetIndexParamNbits(index_config, 8);
        }
    }
    auto conf_adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(GetIndexType());
    AssertInfo(conf_adapter->CheckTrain(index_config, GetIndexMode()), "something wrong in index parameters!");

    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    index_->BuildAll(dataset, index_config);
    rc.RecordSection("TrainAndAdd");

    if (is_in_nm_list(GetIndexType())) {
        store_raw_data(dataset);
        rc.RecordSection("store_raw_data");
    }
    rc.ElapseFromBegin("Done");
    SetDim(index_->Dim());
}

std::unique_ptr<SearchResult>
VectorMemIndex::Query(const DatasetPtr dataset, const SearchInfo& search_info, const BitsetView& bitset) {
    //    AssertInfo(GetMetricType() == search_info.metric_type_,
    //               "Metric type of field index isn't the same with search info");

    auto load_raw_data_closure = [&]() { LoadRawData(); };  // hide this pointer
    auto index_type = GetIndexType();
    if (is_in_nm_list(index_type)) {
        std::call_once(raw_data_loaded_, load_raw_data_closure);
    }

    auto num_queries = knowhere::GetDatasetRows(dataset);
    Config search_conf = search_info.search_params_;
    auto topk = search_info.topk_;
    // TODO :: check dim of search data
    auto final = [&] {
        knowhere::SetMetaTopk(search_conf, topk);
        knowhere::SetMetaMetricType(search_conf, GetMetricType());
        auto index_type = GetIndexType();
        auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_type);
        try {
            adapter->CheckSearch(search_conf, index_type, GetIndexMode());
        } catch (std::exception& e) {
            AssertInfo(false, e.what());
        }
        return index_->Query(dataset, search_conf, bitset);
    }();

    auto ids = knowhere::GetDatasetIDs(final);
    float* distances = (float*)knowhere::GetDatasetDistance(final);

    auto round_decimal = search_info.round_decimal_;
    auto total_num = num_queries * topk;

    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = round(distances[i] * multiplier) / multiplier;
        }
    }
    auto result = std::make_unique<SearchResult>();
    result->seg_offsets_.resize(total_num);
    result->distances_.resize(total_num);
    result->total_nq_ = num_queries;
    result->unity_topK_ = topk;

    std::copy_n(ids, total_num, result->seg_offsets_.data());
    std::copy_n(distances, total_num, result->distances_.data());

    return result;
}

void
VectorMemIndex::store_raw_data(const knowhere::DatasetPtr& dataset) {
    auto index_type = GetIndexType();
    if (is_in_nm_list(index_type)) {
        auto tensor = knowhere::GetDatasetTensor(dataset);
        auto row_num = knowhere::GetDatasetRows(dataset);
        auto dim = knowhere::GetDatasetDim(dataset);
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
VectorMemIndex::LoadRawData() {
    auto index_type = GetIndexType();
    if (is_in_nm_list(index_type)) {
        auto bs = index_->Serialize(Config{knowhere::meta::SLICE_SIZE, config::KnowhereGetIndexSliceSize()});
        auto bptr = std::make_shared<knowhere::Binary>();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        bptr->data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);
        bptr->size = raw_data_.size();
        bs.Append(RAW_DATA, bptr);
        index_->Load(bs);
    }
}

void
VectorMemIndex::parse_config(Config& config) {
    auto stoi_closure = [](const std::string& s) -> int { return std::stoi(s); };

    /***************************** meta *******************************/
    CheckParameter<int>(config, knowhere::meta::SLICE_SIZE, stoi_closure,
                        std::optional{config::KnowhereGetIndexSliceSize()});
    CheckParameter<int>(config, knowhere::meta::DIM, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::meta::TOPK, stoi_closure, std::nullopt);

    /***************************** IVF Params *******************************/
    CheckParameter<int>(config, knowhere::indexparam::NPROBE, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::NLIST, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::M, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::NBITS, stoi_closure, std::nullopt);

    /************************** PQ Params *****************************/
    CheckParameter<int>(config, knowhere::indexparam::PQ_M, stoi_closure, std::nullopt);

    /************************** HNSW Params *****************************/
    CheckParameter<int>(config, knowhere::indexparam::EFCONSTRUCTION, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::HNSW_M, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::EF, stoi_closure, std::nullopt);

    /************************** Annoy Params *****************************/
    CheckParameter<int>(config, knowhere::indexparam::N_TREES, stoi_closure, std::nullopt);
    CheckParameter<int>(config, knowhere::indexparam::SEARCH_K, stoi_closure, std::nullopt);
}

}  // namespace milvus::index
