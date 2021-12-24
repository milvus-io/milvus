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

#include <faiss/AutoTune.h>
#include <faiss/IVFlib.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/clone_index.h>
#include <faiss/index_io.h>
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#endif

#include <fiu/fiu-local.h>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_offset_index/IndexIVF_NM.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

namespace milvus {
namespace knowhere {

using stdclock = std::chrono::high_resolution_clock;

BinarySet
IVF_NM::Serialize(const Config& config) {
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
IVF_NM::Load(const BinarySet& binary_set) {
    Assemble(const_cast<BinarySet&>(binary_set));
    LoadImpl(binary_set, index_type_);

    // Construct arranged data from original data
    auto binary = binary_set.GetByName(RAW_DATA);
    auto original_data = reinterpret_cast<const float*>(binary->data.get());
    auto ivf_index = static_cast<faiss::IndexIVF*>(index_.get());
    auto invlists = ivf_index->invlists;
    auto d = ivf_index->d;
    prefix_sum.resize(invlists->nlist);
    size_t curr_index = 0;

    if (STATISTICS_LEVEL >= 3) {
        ivf_index->nprobe_statistics.resize(invlists->nlist, 0);
    }

#ifndef MILVUS_GPU_VERSION
    auto ails = dynamic_cast<faiss::ArrayInvertedLists*>(invlists);
    size_t nb = binary->size / invlists->code_size;
    auto arranged_data = new float[d * nb];
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = ails->ids[i].size();
        for (size_t j = 0; j < list_size; j++) {
            memcpy(arranged_data + d * (curr_index + j), original_data + d * ails->ids[i][j], d * sizeof(float));
        }
        prefix_sum[i] = curr_index;
        curr_index += list_size;
    }
    data_ = std::shared_ptr<uint8_t[]>(reinterpret_cast<uint8_t*>(arranged_data));
#else
    auto rol = dynamic_cast<faiss::ReadOnlyArrayInvertedLists*>(invlists);
    auto arranged_data = reinterpret_cast<float*>(rol->pin_readonly_codes->data);
    auto lengths = rol->readonly_length;
    auto rol_ids = reinterpret_cast<const int64_t*>(rol->pin_readonly_ids->data);
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = lengths[i];
        for (size_t j = 0; j < list_size; j++) {
            memcpy(arranged_data + d * (curr_index + j), original_data + d * rol_ids[curr_index + j],
                   d * sizeof(float));
        }
        prefix_sum[i] = curr_index;
        curr_index += list_size;
    }

    /* hold codes shared pointer */
    ro_codes = rol->pin_readonly_codes;
    data_ = nullptr;
#endif
    //    LOG_KNOWHERE_DEBUG_ << "IndexIVF_FLAT::Load finished, show statistics:";
    //    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    //    LOG_KNOWHERE_DEBUG_ << ivf_stats->ToString();
}

void
IVF_NM::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)

    int64_t nlist = config[IndexParams::nlist].get<int64_t>();
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    auto coarse_quantizer = new faiss::IndexFlat(dim, metric_type);
    auto index = std::make_shared<faiss::IndexIVFFlat>(coarse_quantizer, dim, nlist, metric_type);
    index->own_fields = true;
    index->train(rows, reinterpret_cast<const float*>(p_data));
    index_ = index;
}

void
IVF_NM::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GET_TENSOR_DATA(dataset_ptr)
    index_->add_without_codes(rows, reinterpret_cast<const float*>(p_data));
}

DatasetPtr
IVF_NM::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
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
        fiu_do_on("IVF_NM.Search.throw_std_exception", throw std::exception());
        fiu_do_on("IVF_NM.Search.throw_faiss_exception", throw faiss::FaissException(""));
        auto k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = static_cast<int64_t*>(malloc(p_id_size));
        auto p_dist = static_cast<float*>(malloc(p_dist_size));

        QueryImpl(rows, reinterpret_cast<const float*>(p_data), k, p_dist, p_id, config, bitset);
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

#if 0
DatasetPtr
IVF_NM::QueryById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);

    try {
        int64_t k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        // todo: enable search by id (zhiru)
        //        auto blacklist = dataset_ptr->Get<faiss::ConcurrentBitsetPtr>("bitset");
        auto index_ivf = std::static_pointer_cast<faiss::IndexIVF>(index_);
        index_ivf->search_by_id(rows, p_data, k, p_dist, p_id, bitset_);

        auto ret_ds = std::make_shared<Dataset>();
        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, p_dist);
        return ret_ds;
    } catch (faiss::FaissException& e) {
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

DatasetPtr
IVF_NM::GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);
    auto elems = dataset_ptr->Get<int64_t>(meta::DIM);

    try {
        size_t p_x_size = sizeof(float) * elems;
        auto p_x = (float*)malloc(p_x_size);

        auto index_ivf = std::static_pointer_cast<faiss::IndexIVF>(index_);
        index_ivf->get_vector_by_id(1, p_data, p_x, bitset_);

        auto ret_ds = std::make_shared<Dataset>();
        ret_ds->Set(meta::TENSOR, p_x);
        return ret_ds;
    } catch (faiss::FaissException& e) {
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}
#endif

void
IVF_NM::Seal() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    SealImpl();
}

VecIndexPtr
IVF_NM::CopyCpuToGpu(const int64_t device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu_without_codes(res->faiss_res.get(), device_id, index_.get(),
                                                                    static_cast<const uint8_t*>(ro_codes->data));

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVF>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }

#else
    KNOWHERE_THROW_MSG("Calling IVF_NM::CopyCpuToGpu when we are using CPU version");
#endif
}

void
IVF_NM::GenGraph(const float* data, const int64_t k, GraphType& graph, const Config& config) {
    int64_t K = k + 1;
    auto ntotal = Count();

    size_t dim = config[meta::DIM];
    auto batch_size = 1000;
    auto tail_batch_size = ntotal % batch_size;
    auto batch_search_count = ntotal / batch_size;
    auto total_search_count = tail_batch_size == 0 ? batch_search_count : batch_search_count + 1;

    std::vector<float> res_dis(K * batch_size);
    graph.resize(ntotal);
    GraphType res_vec(total_search_count);
    for (int i = 0; i < total_search_count; ++i) {
        auto b_size = (i == (total_search_count - 1)) && tail_batch_size != 0 ? tail_batch_size : batch_size;

        auto& res = res_vec[i];
        res.resize(K * b_size);

        const float* xq = data + batch_size * dim * i;
        QueryImpl(b_size, xq, K, res_dis.data(), res.data(), config, nullptr);

        for (int j = 0; j < b_size; ++j) {
            auto& node = graph[batch_size * i + j];
            node.resize(k);
            auto start_pos = j * K + 1;
            for (int m = 0, cursor = start_pos; m < k && cursor < start_pos + k; ++m, ++cursor) {
                node[m] = res[cursor];
            }
        }
    }
}

std::shared_ptr<faiss::IVFSearchParameters>
IVF_NM::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->max_codes = config["max_codes"];
    return params;
}

void
IVF_NM::QueryImpl(int64_t n,
                  const float* query,
                  int64_t k,
                  float* distances,
                  int64_t* labels,
                  const Config& config,
                  const faiss::BitsetView bitset) {
    auto params = GenParams(config);
    auto ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->nprobe = params->nprobe;
    stdclock::time_point before = stdclock::now();
    if (params->nprobe > 1 && n <= 4) {
        ivf_index->parallel_mode = 1;
    } else {
        ivf_index->parallel_mode = 0;
    }
    bool is_sq8 = (index_type_ == IndexEnum::INDEX_FAISS_IVFSQ8) ? true : false;

#ifndef MILVUS_GPU_VERSION
    auto data = static_cast<const uint8_t*>(data_.get());
#else
    auto data = static_cast<const uint8_t*>(ro_codes->data);
#endif
    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    ivf_index->search_without_codes(n, reinterpret_cast<const float*>(query), data, prefix_sum, is_sq8, k, distances,
                                    labels, bitset);
    stdclock::time_point after = stdclock::now();
    double search_cost = (std::chrono::duration<double, std::micro>(after - before)).count();
    LOG_KNOWHERE_DEBUG_ << "IVF_NM search cost: " << search_cost
                        << ", quantization cost: " << ivf_index->index_ivf_stats.quantization_time
                        << ", data search cost: " << ivf_index->index_ivf_stats.search_time;

    if (STATISTICS_LEVEL) {
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
    //     LOG_KNOWHERE_DEBUG_ << "IndexIVF_FLAT::QueryImpl finished, show statistics:";
    //     LOG_KNOWHERE_DEBUG_ << GetStatistics()->ToString();
}

void
IVF_NM::SealImpl() {
#ifdef MILVUS_GPU_VERSION
    faiss::Index* index = index_.get();
    auto idx = dynamic_cast<faiss::IndexIVF*>(index);
    if (idx != nullptr) {
        idx->to_readonly_without_codes();
    }
#endif
}

int64_t
IVF_NM::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
IVF_NM::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

void
IVF_NM::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto ivf_index = dynamic_cast<faiss::IndexIVFFlat*>(index_.get());
    auto nb = ivf_index->invlists->compute_ntotal();
    auto nlist = ivf_index->nlist;
    auto code_size = ivf_index->code_size;
    // ivf codes, ivf ids and quantizer
    index_size_ = nb * code_size + nb * sizeof(int64_t) + nlist * code_size;
}

StatisticsPtr
IVF_NM::GetStatistics() {
    if (!STATISTICS_LEVEL) {
        return stats;
    }
    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    auto ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    auto lock = ivf_stats->Lock();
    ivf_stats->update_ivf_access_stats(ivf_index->nprobe_statistics);
    return ivf_stats;
}

void
IVF_NM::ClearStatistics() {
    if (!STATISTICS_LEVEL) {
        return;
    }
    auto ivf_stats = std::dynamic_pointer_cast<IVFStatistics>(stats);
    auto ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->clear_nprobe_statistics();
    ivf_index->index_ivf_stats.reset();
    auto lock = ivf_stats->Lock();
    ivf_stats->clear();
}

}  // namespace knowhere
}  // namespace milvus
