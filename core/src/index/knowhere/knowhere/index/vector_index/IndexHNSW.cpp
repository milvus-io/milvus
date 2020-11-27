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

#include "knowhere/index/vector_index/IndexHNSW.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <utility>
#include <vector>
#include <chrono>

#include "faiss/BuilderSuspend.h"
#include "hnswlib/hnswalg.h"
#include "hnswlib/hnswlib.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

// void
// normalize_vector(float* data, float* norm_array, size_t dim) {
//     float norm = 0.0f;
//     for (int i = 0; i < dim; i++) norm += data[i] * data[i];
//     norm = 1.0f / (sqrtf(norm) + 1e-30f);
//     for (int i = 0; i < dim; i++) norm_array[i] = data[i] * norm;
// }

BinarySet
IndexHNSW::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        index_->saveIndex(writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("HNSW", data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::Load(const BinarySet& index_binary) {
    try {
        auto binary = index_binary.GetByName("HNSW");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        hnswlib::SpaceInterface<float>* space = nullptr;
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space);
        if (STATISTICS_ENABLE >= 2)
            index_->stats_enable = true;
        index_->loadIndex(reader);
        auto hnsw_stats = std::dynamic_pointer_cast<HNSWStatistics>(stats);
        if (STATISTICS_ENABLE) {
            if (STATISTICS_ENABLE >= 2) {
                hnsw_stats->max_level = index_->maxlevel_;
                hnsw_stats->distribution.resize(index_->maxlevel_ + 1);
                for (auto i = 0; i <= index_->maxlevel_; ++ i) {
                    hnsw_stats->distribution[i] = index_->level_stats_[i];
                    if (hnsw_stats->distribution[i] >= 1000 && hnsw_stats->distribution[i] < 10000)
                        hnsw_stats->target_level = i;
                }
            }
        }
        LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Load finished, show statistics:";
        LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);

        normalize = index_->metric_type_ == 1;  // 1 == InnerProduct
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        auto dim = dataset_ptr->Get<int64_t>(meta::DIM);
        auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);

        hnswlib::SpaceInterface<float>* space;
        std::string metric_type = config[Metric::TYPE];
        if (metric_type == Metric::L2) {
            space = new hnswlib::L2Space(dim);
        } else if (metric_type == Metric::IP) {
            space = new hnswlib::InnerProductSpace(dim);
            normalize = true;
        } else {
            KNOWHERE_THROW_MSG("Metric type not supported: " + metric_type);
        }
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space, rows, config[IndexParams::M].get<int64_t>(),
                                                                   config[IndexParams::efConstruction].get<int64_t>());
        if (STATISTICS_ENABLE >= 2)
            index_->stats_enable = true;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);

    GET_TENSOR_DATA_ID(dataset_ptr)

    //     if (normalize) {
    //         std::vector<float> ep_norm_vector(Dim());
    //         normalize_vector((float*)(p_data), ep_norm_vector.data(), Dim());
    //         index_->addPoint((void*)(ep_norm_vector.data()), p_ids[0]);
    // #pragma omp parallel for
    //         for (int i = 1; i < rows; ++i) {
    //             std::vector<float> norm_vector(Dim());
    //             normalize_vector((float*)(p_data + Dim() * i), norm_vector.data(), Dim());
    //             index_->addPoint((void*)(norm_vector.data()), p_ids[i]);
    //         }
    //     } else {
    //         index_->addPoint((void*)(p_data), p_ids[0]);
    // #pragma omp parallel for
    //         for (int i = 1; i < rows; ++i) {
    //             index_->addPoint((void*)(p_data + Dim() * i), p_ids[i]);
    //         }
    //     }

    index_->addPoint(p_data, p_ids[0]);
#pragma omp parallel for
    for (int i = 1; i < rows; ++i) {
        faiss::BuilderSuspend::check_wait();
        index_->addPoint((reinterpret_cast<const float*>(p_data) + Dim() * i), p_ids[i]);
    }
    if (STATISTICS_ENABLE) {
        auto hnsw_stats = std::dynamic_pointer_cast<HNSWStatistics>(stats);
        if (STATISTICS_ENABLE >= 2) {
            hnsw_stats->max_level = index_->maxlevel_;
            hnsw_stats->distribution.resize(index_->maxlevel_ + 1);
            for (auto i = 0; i <= index_->maxlevel_; ++ i) {
                hnsw_stats->distribution[i] = index_->level_stats_[i];
                if (hnsw_stats->distribution[i] >= 1000 && hnsw_stats->distribution[i] < 10000)
                    hnsw_stats->target_level = i;
            }
        }
        LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Train finished, show statistics:";
        LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);
    }
}

DatasetPtr
IndexHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::ConcurrentBitsetPtr& bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA(dataset_ptr)

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = static_cast<int64_t*>(malloc(id_size * rows));
    auto p_dist = static_cast<float*>(malloc(dist_size * rows));
    std::vector<hnswlib::StatisticsInfo> query_stats;
    auto hnsw_stats = std::dynamic_pointer_cast<HNSWStatistics>(stats);
    if (STATISTICS_ENABLE) {
        if (STATISTICS_ENABLE >= 1) {
            hnsw_stats->nq_cnt += rows;
            hnsw_stats->batch_cnt += 1;
            hnsw_stats->ef_sum += config[IndexParams::ef].get<int64_t>();
        }
        if (STATISTICS_ENABLE >= 2) {
            query_stats.resize(rows);
            for (auto i = 0; i < rows; ++ i)
                query_stats[i].target_level = hnsw_stats->target_level;
            double fps = bitset ? (double)bitset->count_1() / bitset->count() : 0.0;
            hnsw_stats->filter_percentage_sum += fps;
            if (fps > 1.0 || fps < 0.0)
                LOG_KNOWHERE_ERROR_ << "in IndexHNSW::Query, the percentage of 1 in bitset is " << fps
                                    << ", which is exceed 100% or negative!";
            else
                hnsw_stats->filter_cdf[(int)(fps * 100) / 5] += 1;
        }
    }

    index_->setEf(config[IndexParams::ef].get<int64_t>());

    using P = std::pair<float, int64_t>;
    auto compare = [](const P& v1, const P& v2) { return v1.first < v2.first; };

    std::chrono::high_resolution_clock::time_point query_start, query_end;
    query_start = std::chrono::high_resolution_clock::now();

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        std::vector<P> ret;
        const float* single_query = reinterpret_cast<const float*>(p_data) + i * Dim();

        // if (normalize) {
        //     std::vector<float> norm_vector(Dim());
        //     normalize_vector((float*)(single_query), norm_vector.data(), Dim());
        //     ret = index_->searchKnn((float*)(norm_vector.data()), config[meta::TOPK].get<int64_t>(), compare);
        // } else {
        //     ret = index_->searchKnn((float*)single_query, config[meta::TOPK].get<int64_t>(), compare);
        // }
        if (STATISTICS_ENABLE >= 2) {
            ret = index_->searchKnn(single_query, k, compare, bitset, query_stats[i]);
        }
        else {
            auto dummy_stat = hnswlib::StatisticsInfo();
            ret = index_->searchKnn(single_query, k, compare, bitset, dummy_stat);
        }

        while (ret.size() < k) {
            ret.emplace_back(std::make_pair(-1, -1));
        }
        std::vector<float> dist;
        std::vector<int64_t> ids;

        if (normalize) {
            std::transform(ret.begin(), ret.end(), std::back_inserter(dist),
                           [](const std::pair<float, int64_t>& e) { return float(1 - e.first); });
        } else {
            std::transform(ret.begin(), ret.end(), std::back_inserter(dist),
                           [](const std::pair<float, int64_t>& e) { return e.first; });
        }
        std::transform(ret.begin(), ret.end(), std::back_inserter(ids),
                       [](const std::pair<float, int64_t>& e) { return e.second; });

        memcpy(p_dist + i * k, dist.data(), dist_size);
        memcpy(p_id + i * k, ids.data(), id_size);
    }
    query_end = std::chrono::high_resolution_clock::now();

    if (STATISTICS_ENABLE) {
        if (STATISTICS_ENABLE >= 1) {
            hnsw_stats->total_query_time += std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start).count();
        }
        if (STATISTICS_ENABLE >= 2) {
            for (auto i = 0; i < rows; ++ i) {
                for (auto j = 0; j < query_stats[i].accessed_points.size(); ++ j) {
                    auto tgt = hnsw_stats->access_cnt.find(query_stats[i].accessed_points[j]);
                    if (tgt == hnsw_stats->access_cnt.end())
                        hnsw_stats->access_cnt[query_stats[i].accessed_points[j]] = 1;
                    else
                        tgt->second += 1;
                }
            }
        }
    }
    LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Query finished, show statistics:";
    LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString(index_type_);
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexHNSW::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->cur_element_count;
}

int64_t
IndexHNSW::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return (*static_cast<size_t*>(index_->dist_func_param_));
}

void
IndexHNSW::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

StatisticsPtr
IndexHNSW::GetStatistics() {
    auto hnsw_stats = std::dynamic_pointer_cast<HNSWStatistics>(stats);
    return hnsw_stats;
}

void
IndexHNSW::ClearStatistics() {
    if (!STATISTICS_ENABLE)
        return;
    auto hnsw_stats = std::dynamic_pointer_cast<HNSWStatistics>(stats);
    hnsw_stats->Clear();
}

}  // namespace knowhere
}  // namespace milvus
