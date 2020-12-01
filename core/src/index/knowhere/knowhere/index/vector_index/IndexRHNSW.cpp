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

#include "knowhere/index/vector_index/IndexRHNSW.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iterator>
#include <utility>
#include <vector>

#include "faiss/BuilderSuspend.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexRHNSW::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        writer.name = this->index_type() + "_Index";
        faiss::write_index(index_.get(), &writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append(writer.name, data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSW::Load(const BinarySet& index_binary) {
    try {
        MemoryIOReader reader;
        reader.name = this->index_type() + "_Index";
        auto binary = index_binary.GetByName(reader.name);

        reader.total = static_cast<size_t>(binary->size);
        reader.data_ = binary->data.get();

        auto idx = faiss::read_index(&reader);
        auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
        if (STATISTICS_ENABLE) {
            auto real_idx = dynamic_cast<faiss::IndexRHNSW*>(idx);
            if (STATISTICS_ENABLE >= 3) {
                hnsw_stats->max_level = real_idx->hnsw.max_level;
                hnsw_stats->distribution.resize(real_idx->hnsw.max_level + 1);
                for (auto i = 0; i <= real_idx->hnsw.max_level; ++i) {
                    hnsw_stats->distribution[i] = real_idx->hnsw.level_stats[i];
                    if (hnsw_stats->distribution[i] >= 1000 && hnsw_stats->distribution[i] < 10000)
                        hnsw_stats->target_level = i;
                }
                real_idx->set_target_level(hnsw_stats->target_level);
            }
        }
        LOG_KNOWHERE_DEBUG_ << "IndexRHNSW::Load finished, show statistics:";
        LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);
        index_.reset(idx);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSW::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    KNOWHERE_THROW_MSG("IndexRHNSW has no implementation of Train, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}

void
IndexRHNSW::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr)

    index_->add(rows, reinterpret_cast<const float*>(p_data));
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    if (STATISTICS_ENABLE) {
        auto real_idx = dynamic_cast<faiss::IndexRHNSW*>(index_.get());
        if (STATISTICS_ENABLE >= 3) {
            hnsw_stats->max_level = real_idx->hnsw.max_level;
            hnsw_stats->distribution.resize(real_idx->hnsw.max_level + 1);
            for (auto i = 0; i <= real_idx->hnsw.max_level; ++i) {
                hnsw_stats->distribution[i] = real_idx->hnsw.level_stats[i];
                if (hnsw_stats->distribution[i] >= 1000 && hnsw_stats->distribution[i] < 10000)
                    hnsw_stats->target_level = i;
            }
            real_idx->set_target_level(hnsw_stats->target_level);
        }
    }
    LOG_KNOWHERE_DEBUG_ << "IndexRHNSW::Build finished, show statistics:";
    LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);
}

DatasetPtr
IndexRHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::ConcurrentBitsetPtr& bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA(dataset_ptr)

    auto k = config[meta::TOPK].get<int64_t>();
    int64_t id_size = sizeof(int64_t) * k;
    int64_t dist_size = sizeof(float) * k;
    auto p_id = static_cast<int64_t*>(malloc(id_size * rows));
    auto p_dist = static_cast<float*>(malloc(dist_size * rows));
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    if (STATISTICS_ENABLE) {
        if (STATISTICS_ENABLE >= 1) {
            hnsw_stats->nq_cnt += rows;
            hnsw_stats->batch_cnt += 1;
            hnsw_stats->ef_sum += config[IndexParams::ef].get<int64_t>();
            if (rows > 2048)
                hnsw_stats->nq_fd[12]++;
            else
                hnsw_stats->nq_fd[len_of_pow2(upper_bound_of_pow2((uint64_t)rows))]++;
        }
        if (STATISTICS_ENABLE >= 2) {
            double fps = bitset ? (double)bitset->count_1() / bitset->count() : 0.0;
            hnsw_stats->filter_percentage_sum += fps;
            if (fps > 1.0 || fps < 0.0)
                LOG_KNOWHERE_ERROR_ << "in IndexRHNSW::Query, the percentage of 1 in bitset is " << fps
                                    << ", which is exceed 100% or negative!";
            else
                hnsw_stats->filter_cdf[(int)(fps * 100) / 5] += 1;
        }
    }
    for (auto i = 0; i < k * rows; ++i) {
        p_id[i] = -1;
        p_dist[i] = -1;
    }

    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());

    real_index->hnsw.efSearch = (config[IndexParams::ef].get<int64_t>());

    std::chrono::high_resolution_clock::time_point query_start, query_end;
    query_start = std::chrono::high_resolution_clock::now();
    real_index->search(rows, reinterpret_cast<const float*>(p_data), k, p_dist, p_id, bitset);
    query_end = std::chrono::high_resolution_clock::now();
    if (STATISTICS_ENABLE) {
        if (STATISTICS_ENABLE >= 1) {
            hnsw_stats->total_query_time +=
                std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start).count();
        }
        if (STATISTICS_ENABLE >= 3) {
            real_index->calculate_stats(hnsw_stats->access_lorenz_curve, hnsw_stats->access_total);
        }
    }
    LOG_KNOWHERE_DEBUG_ << "IndexRHNSW::Load finished, show statistics:";
    LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexRHNSW::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
IndexRHNSW::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

StatisticsPtr
IndexRHNSW::GetStatistics() {
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    if (!STATISTICS_ENABLE)
        return hnsw_stats;
    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());
    real_index->calculate_stats(hnsw_stats->access_lorenz_curve, hnsw_stats->access_total);
    return hnsw_stats;
}

void
IndexRHNSW::ClearStatistics() {
    if (!STATISTICS_ENABLE)
        return;
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());
    hnsw_stats->Clear();
    real_index->clear_stats();
}

void
IndexRHNSW::UpdateIndexSize() {
    KNOWHERE_THROW_MSG(
        "IndexRHNSW has no implementation of UpdateIndexSize, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}

/*
BinarySet
IndexRHNSW::SerializeImpl(const milvus::knowhere::IndexType &type) { return BinarySet(); }

void
IndexRHNSW::SealImpl() {}

void
IndexRHNSW::LoadImpl(const milvus::knowhere::BinarySet &, const milvus::knowhere::IndexType &type) {}
*/

void
IndexRHNSW::AddWithoutIds(const milvus::knowhere::DatasetPtr& dataset, const milvus::knowhere::Config& config) {
    KNOWHERE_THROW_MSG("IndexRHNSW has no implementation of AddWithoutIds, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}
}  // namespace knowhere
}  // namespace milvus
