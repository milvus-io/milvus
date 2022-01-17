// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "knowhere/index/vector_index/IndexBinaryIVF.h"

#include <faiss/IndexBinaryFlat.h>
#include <faiss/IndexBinaryIVF.h>

#include <chrono>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus {
namespace knowhere {

using stdclock = std::chrono::high_resolution_clock;

BinarySet
BinaryIVF::Serialize(const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    auto ret = SerializeImpl(index_type_);
    if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, ret);
    }
    return ret;
}

void
BinaryIVF::Load(const BinarySet& index_binary) {
    Assemble(const_cast<BinarySet&>(index_binary));
    LoadImpl(index_binary, index_type_);

    if (STATISTICS_LEVEL >= 3) {
        auto ivf_index = static_cast<faiss::IndexBinaryIVF*>(index_.get());
        ivf_index->nprobe_statistics.resize(ivf_index->nlist, 0);
    }
}

DatasetPtr
BinaryIVF::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GET_TENSOR_DATA(dataset_ptr)

    int64_t* p_id = nullptr;
    float* p_dist = nullptr;
    auto release_when_exception = [&]() {
        if (p_id != nullptr) {
            free(p_id);
        }
        if (p_dist != nullptr) {
            free(p_dist);
        }
    };

    try {
        auto k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        p_id = static_cast<int64_t*>(malloc(p_id_size));
        p_dist = static_cast<float*>(malloc(p_dist_size));

        QueryImpl(rows, reinterpret_cast<const uint8_t*>(p_data), k, p_dist, p_id, config, bitset);
        MapOffsetToUid(p_id, static_cast<size_t>(elems));

        auto ret_ds = std::make_shared<Dataset>();

        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, p_dist);

        return ret_ds;
    } catch (faiss::FaissException& e) {
        release_when_exception();
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        release_when_exception();
        KNOWHERE_THROW_MSG(e.what());
    }
}

int64_t
BinaryIVF::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
BinaryIVF::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

void
BinaryIVF::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto bin_ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    auto nb = bin_ivf_index->invlists->compute_ntotal();
    auto nlist = bin_ivf_index->nlist;
    auto code_size = bin_ivf_index->code_size;

    // binary ivf codes, ids and quantizer
    index_size_ = nb * code_size + nb * sizeof(int64_t) + nlist * code_size;
}

StatisticsPtr
BinaryIVF::GetStatistics() {
    if (!STATISTICS_LEVEL) {
        return stats;
    }
    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    auto ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    auto lock = ivf_stats->Lock();
    ivf_stats->update_ivf_access_stats(ivf_index->nprobe_statistics);
    return ivf_stats;
}

void
BinaryIVF::ClearStatistics() {
    if (!STATISTICS_LEVEL) {
        return;
    }
    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    auto ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    ivf_index->clear_nprobe_statistics();
    ivf_index->index_ivf_stats.reset();
    auto lock = ivf_stats->Lock();
    ivf_stats->clear();
}

void
BinaryIVF::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)

    int64_t nlist = config[IndexParams::nlist];
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::IndexBinary* coarse_quantizer = new faiss::IndexBinaryFlat(dim, metric_type);
    auto index = std::make_shared<faiss::IndexBinaryIVF>(coarse_quantizer, dim, nlist, metric_type);
    index->own_fields = true;
    index->train(rows, static_cast<const uint8_t*>(p_data));
    index_ = index;
}

void
BinaryIVF::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    GET_TENSOR_DATA(dataset_ptr)
    index_->add(rows, reinterpret_cast<const uint8_t*>(p_data));
}

std::shared_ptr<faiss::IVFSearchParameters>
BinaryIVF::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->max_codes = config["max_code"];
    return params;
}

void
BinaryIVF::QueryImpl(int64_t n,
                     const uint8_t* data,
                     int64_t k,
                     float* distances,
                     int64_t* labels,
                     const Config& config,
                     const faiss::BitsetView bitset) {
    auto params = GenParams(config);
    auto ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    ivf_index->nprobe = params->nprobe;

    stdclock::time_point before = stdclock::now();
    auto i_distances = reinterpret_cast<int32_t*>(distances);

    index_->search(n, data, k, i_distances, labels, bitset);

    stdclock::time_point after = stdclock::now();
    double search_cost = (std::chrono::duration<double, std::micro>(after - before)).count();
    LOG_KNOWHERE_DEBUG_ << "IVF_NM search cost: " << search_cost
                        << ", quantization cost: " << ivf_index->index_ivf_stats.quantization_time
                        << ", data search cost: " << ivf_index->index_ivf_stats.search_time;

    if (STATISTICS_LEVEL) {
        auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
        auto lock = ivf_stats->Lock();
        if (STATISTICS_LEVEL >= 1) {
            ivf_stats->update_nq(n);
            ivf_stats->count_nprobe(ivf_index->nprobe);
            ivf_stats->update_total_query_time(ivf_index->index_ivf_stats.quantization_time +
                                               ivf_index->index_ivf_stats.search_time);
            ivf_index->index_ivf_stats.quantization_time = 0;
            ivf_index->index_ivf_stats.search_time = 0;
        }
        if (STATISTICS_LEVEL >= 2) {
            ivf_stats->update_filter_percentage(bitset);
        }
    }

    // if hamming, it need transform int32 to float
    if (ivf_index->metric_type == faiss::METRIC_Hamming) {
        int64_t num = n * k;
        for (int64_t i = 0; i < num; i++) {
            distances[i] = static_cast<float>(i_distances[i]);
        }
    }
}

}  // namespace knowhere
}  // namespace milvus
