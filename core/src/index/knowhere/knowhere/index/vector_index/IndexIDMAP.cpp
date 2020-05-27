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

#include "knowhere/index/vector_index/IndexIDMAP.h"

#include <faiss/AutoTune.h>
#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/clone_index.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuCloner.h>
#endif

#include <string>
#include <vector>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

namespace milvus {
namespace knowhere {

BinarySet
IDMAP::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl(index_type_);
}

void
IDMAP::Load(const BinarySet& binary_set) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(binary_set, index_type_);
}

void
IDMAP::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    const char* desc = "IDMap,Flat";
    int64_t dim = config[meta::DIM].get<int64_t>();
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    auto index = faiss::index_factory(dim, desc, metric_type);
    index_.reset(index);
}

void
IDMAP::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSORWITHIDS(dataset_ptr)
    index_->add_with_ids(rows, (float*)p_data, p_ids);
}

void
IDMAP::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const void*>(meta::TENSOR);

    // TODO: caiyd need check
    std::vector<int64_t> new_ids(rows);
    for (int i = 0; i < rows; ++i) {
        new_ids[i] = i;
    }

    index_->add_with_ids(rows, (float*)p_data, new_ids.data());
}

DatasetPtr
IDMAP::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GETTENSOR(dataset_ptr)

    int64_t k = config[meta::TOPK].get<int64_t>();
    auto elems = rows * k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    QueryImpl(rows, (float*)p_data, k, p_dist, p_id, Config());

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

#if 0
DatasetPtr
IDMAP::QueryById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    //    GETTENSOR(dataset)
    auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);

    int64_t k = config[meta::TOPK].get<int64_t>();
    auto elems = rows * k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    // todo: enable search by id (zhiru)
    //    auto blacklist = dataset_ptr->Get<faiss::ConcurrentBitsetPtr>("bitset");
    //    index_->searchById(rows, (float*)p_data, config[meta::TOPK].get<int64_t>(), p_dist, p_id, blacklist);
    index_->search_by_id(rows, p_data, k, p_dist, p_id, bitset_);

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}
#endif

VecIndexPtr
IDMAP::CopyCpuToGpu(const int64_t device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIDMAP>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
#else
    KNOWHERE_THROW_MSG("Calling IDMAP::CopyCpuToGpu when we are using CPU version");
#endif
}

const float*
IDMAP::GetRawVectors() {
    try {
        auto file_index = dynamic_cast<faiss::IndexIDMap*>(index_.get());
        auto flat_index = dynamic_cast<faiss::IndexFlat*>(file_index->index);
        return flat_index->xb.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

const int64_t*
IDMAP::GetRawIds() {
    try {
        auto file_index = dynamic_cast<faiss::IndexIDMap*>(index_.get());
        return file_index->id_map.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

#if 0
DatasetPtr
IDMAP::GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    //    GETTENSOR(dataset)
    // auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);
    auto elems = dataset_ptr->Get<int64_t>(meta::DIM);

    size_t p_x_size = sizeof(float) * elems;
    auto p_x = (float*)malloc(p_x_size);

    index_->get_vector_by_id(1, p_data, p_x, bitset_);

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::TENSOR, p_x);
    return ret_ds;
}
#endif

void
IDMAP::QueryImpl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& config) {
    index_->search(n, (float*)data, k, distances, labels, bitset_);
}

}  // namespace knowhere
}  // namespace milvus
