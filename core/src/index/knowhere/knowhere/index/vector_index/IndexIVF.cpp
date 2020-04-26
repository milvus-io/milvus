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
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#endif

#include <fiu-local.h>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

namespace milvus {
namespace knowhere {

using stdclock = std::chrono::high_resolution_clock;

BinarySet
IVF::Serialize(const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl(index_type_);
}

void
IVF::Load(const BinarySet& binary_set) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(binary_set, index_type_);
}

void
IVF::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GETTENSOR(dataset_ptr)

    faiss::Index* coarse_quantizer = new faiss::IndexFlatL2(dim);
    int64_t nlist = config[IndexParams::nlist].get<int64_t>();
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    auto index = std::make_shared<faiss::IndexIVFFlat>(coarse_quantizer, dim, nlist, metric_type);
    index->train(rows, (float*)p_data);

    index_.reset(faiss::clone_index(index.get()));
}

void
IVF::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSORWITHIDS(dataset_ptr)
    index_->add_with_ids(rows, (float*)p_data, p_ids);
}

void
IVF::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSOR(dataset_ptr)
    index_->add(rows, (float*)p_data);
}

DatasetPtr
IVF::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GETTENSOR(dataset_ptr)

    try {
        fiu_do_on("IVF.Search.throw_std_exception", throw std::exception());
        fiu_do_on("IVF.Search.throw_faiss_exception", throw faiss::FaissException(""));
        int64_t k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        QueryImpl(rows, (float*)p_data, k, p_dist, p_id, config);

        //    std::stringstream ss_res_id, ss_res_dist;
        //    for (int i = 0; i < 10; ++i) {
        //        printf("%llu", p_id[i]);
        //        printf("\n");
        //        printf("%.6f", p_dist[i]);
        //        printf("\n");
        //        ss_res_id << p_id[i] << " ";
        //        ss_res_dist << p_dist[i] << " ";
        //    }
        //    std::cout << std::endl << "after search: " << std::endl;
        //    std::cout << ss_res_id.str() << std::endl;
        //    std::cout << ss_res_dist.str() << std::endl << std::endl;

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
IVF::QueryById(const DatasetPtr& dataset_ptr, const Config& config) {
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

        //    std::stringstream ss_res_id, ss_res_dist;
        //    for (int i = 0; i < 10; ++i) {
        //        printf("%llu", res_ids[i]);
        //        printf("\n");
        //        printf("%.6f", res_dis[i]);
        //        printf("\n");
        //        ss_res_id << res_ids[i] << " ";
        //        ss_res_dist << res_dis[i] << " ";
        //    }
        //    std::cout << std::endl << "after search: " << std::endl;
        //    std::cout << ss_res_id.str() << std::endl;
        //    std::cout << ss_res_dist.str() << std::endl << std::endl;

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
IVF::GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) {
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

void
IVF::Seal() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    SealImpl();
}

VecIndexPtr
IVF::CopyCpuToGpu(const int64_t device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVF>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }

#else
    KNOWHERE_THROW_MSG("Calling IVF::CopyCpuToGpu when we are using CPU version");
#endif
}

void
IVF::GenGraph(const float* data, const int64_t k, GraphType& graph, const Config& config) {
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

        auto xq = data + batch_size * dim * i;
        QueryImpl(b_size, (float*)xq, K, res_dis.data(), res.data(), config);

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
IVF::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->max_codes = config["max_codes"];
    return params;
}

void
IVF::QueryImpl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& config) {
    auto params = GenParams(config);
    auto ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->nprobe = params->nprobe;
    stdclock::time_point before = stdclock::now();
    if (params->nprobe > 1 && n <= 4) {
        ivf_index->parallel_mode = 1;
    } else {
        ivf_index->parallel_mode = 0;
    }
    ivf_index->search(n, (float*)data, k, distances, labels, bitset_);
    stdclock::time_point after = stdclock::now();
    double search_cost = (std::chrono::duration<double, std::micro>(after - before)).count();
    LOG_KNOWHERE_DEBUG_ << "IVF search cost: " << search_cost
                        << ", quantization cost: " << faiss::indexIVF_stats.quantization_time
                        << ", data search cost: " << faiss::indexIVF_stats.search_time;
    faiss::indexIVF_stats.quantization_time = 0;
    faiss::indexIVF_stats.search_time = 0;
}

void
IVF::SealImpl() {
#ifdef MILVUS_GPU_VERSION
    faiss::Index* index = index_.get();
    auto idx = dynamic_cast<faiss::IndexIVF*>(index);
    if (idx != nullptr) {
        // To be deleted
        LOG_KNOWHERE_DEBUG_ << "Test before to_readonly: IVF READONLY " << std::boolalpha << idx->is_readonly();
        idx->to_readonly();
    }
#endif
}

}  // namespace knowhere
}  // namespace milvus
