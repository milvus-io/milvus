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
#include <chrono>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

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
        if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
            Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, res_set);
        }
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::Load(const BinarySet& index_binary) {
    try {
        Assemble(const_cast<BinarySet&>(index_binary));
        auto binary = index_binary.GetByName("HNSW");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        hnswlib::SpaceInterface<float>* space = nullptr;
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space);
        index_->stats_enable = (STATISTICS_LEVEL >= 3);
        index_->loadIndex(reader);
        auto hnsw_stats = std::static_pointer_cast<LibHNSWStatistics>(stats);
        if (STATISTICS_LEVEL >= 3) {
            auto lock = hnsw_stats->Lock();
            hnsw_stats->update_level_distribution(index_->maxlevel_, index_->level_stats_);
        }
        //         LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Load finished, show statistics:";
        //         LOG_KNOWHERE_DEBUG_ << hnsw_stats->ToString();
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
        } else {
            KNOWHERE_THROW_MSG("Metric type not supported: " + metric_type);
        }
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space, rows, config[IndexParams::M].get<int64_t>(),
                                                                   config[IndexParams::efConstruction].get<int64_t>());
        index_->stats_enable = (STATISTICS_LEVEL >= 3);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    GET_TENSOR_DATA(dataset_ptr)

    index_->addPoint(p_data, 0);
#pragma omp parallel for
    for (int i = 1; i < rows; ++i) {
        index_->addPoint((reinterpret_cast<const float*>(p_data) + Dim() * i), i);
    }
    if (STATISTICS_LEVEL >= 3) {
        auto hnsw_stats = std::static_pointer_cast<LibHNSWStatistics>(stats);
        auto lock = hnsw_stats->Lock();
        hnsw_stats->update_level_distribution(index_->maxlevel_, index_->level_stats_);
    }
    //     LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Train finished, show statistics:";
    //     LOG_KNOWHERE_DEBUG_ << GetStatistics()->ToString();
}

DatasetPtr
IndexHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA_DIM(dataset_ptr)

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = static_cast<int64_t*>(malloc(id_size * rows));
    auto p_dist = static_cast<float*>(malloc(dist_size * rows));
    std::vector<hnswlib::StatisticsInfo> query_stats;
    auto hnsw_stats = std::dynamic_pointer_cast<LibHNSWStatistics>(stats);
    if (STATISTICS_LEVEL >= 3) {
        query_stats.resize(rows);
        for (auto i = 0; i < rows; ++i) {
            query_stats[i].target_level = hnsw_stats->target_level;
        }
    }

    index_->setEf(config[IndexParams::ef].get<int64_t>());
    bool transform = (index_->metric_type_ == 1);  // InnerProduct: 1

    std::chrono::high_resolution_clock::time_point query_start, query_end;
    query_start = std::chrono::high_resolution_clock::now();

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        auto single_query = (float*)p_data + i * dim;
        std::priority_queue<std::pair<float, hnswlib::labeltype>> rst;
        if (STATISTICS_LEVEL >= 3) {
            rst = index_->searchKnn(single_query, k, bitset, query_stats[i]);
        } else {
            auto dummy_stat = hnswlib::StatisticsInfo();
            rst = index_->searchKnn(single_query, k, bitset, dummy_stat);
        }
        size_t rst_size = rst.size();

        auto p_single_dis = p_dist + i * k;
        auto p_single_id = p_id + i * k;
        size_t idx = rst_size - 1;
        while (!rst.empty()) {
            auto& it = rst.top();
            p_single_dis[idx] = transform ? (1 - it.first) : it.first;
            p_single_id[idx] = it.second;
            rst.pop();
            idx--;
        }
        MapOffsetToUid(p_single_id, rst_size);

        for (idx = rst_size; idx < k; idx++) {
            p_single_dis[idx] = float(1.0 / 0.0);
            p_single_id[idx] = -1;
        }
    }
    query_end = std::chrono::high_resolution_clock::now();

    if (STATISTICS_LEVEL) {
        auto lock = hnsw_stats->Lock();
        if (STATISTICS_LEVEL >= 1) {
            hnsw_stats->update_nq(rows);
            hnsw_stats->update_ef_sum(index_->ef_ * rows);
            hnsw_stats->update_total_query_time(
                std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start).count());
        }
        if (STATISTICS_LEVEL >= 2) {
            hnsw_stats->update_filter_percentage(bitset);
        }
        if (STATISTICS_LEVEL >= 3) {
            for (auto i = 0; i < rows; ++i) {
                for (auto j = 0; j < query_stats[i].accessed_points.size(); ++j) {
                    auto tgt = hnsw_stats->access_cnt_map.find(query_stats[i].accessed_points[j]);
                    if (tgt == hnsw_stats->access_cnt_map.end())
                        hnsw_stats->access_cnt_map[query_stats[i].accessed_points[j]] = 1;
                    else
                        tgt->second += 1;
                }
            }
        }
    }
    //     LOG_KNOWHERE_DEBUG_ << "IndexHNSW::Query finished, show statistics:";
    //     LOG_KNOWHERE_DEBUG_ << GetStatistics()->ToString();

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

void
IndexHNSW::ClearStatistics() {
    if (!STATISTICS_LEVEL)
        return;
    auto hnsw_stats = std::static_pointer_cast<LibHNSWStatistics>(stats);
    auto lock = hnsw_stats->Lock();
    hnsw_stats->clear();
}

}  // namespace knowhere
}  // namespace milvus
