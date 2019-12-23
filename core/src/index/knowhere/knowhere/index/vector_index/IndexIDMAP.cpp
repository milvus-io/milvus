// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>

#include <faiss/AutoTune.h>
#include <faiss/clone_index.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>

#ifdef MILVUS_GPU_VERSION

#include <faiss/gpu/GpuCloner.h>

#endif

#include <vector>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

#ifdef MILVUS_GPU_VERSION

#include "knowhere/index/vector_index/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

#endif

namespace knowhere {

BinarySet
IDMAP::Serialize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl();
}

void
IDMAP::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary);
}

DatasetPtr
IDMAP::Search(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    config->CheckValid();
    GETTENSOR(dataset)

    auto elems = rows * config->k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    search_impl(rows, (float*)p_data, config->k, p_dist, p_id, Config());

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

void
IDMAP::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) {
    index_->search(n, (float*)data, k, distances, labels);
}

void
IDMAP::Add(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSOR(dataset)

    auto p_ids = dataset->Get<const int64_t*>(meta::IDS);
    index_->add_with_ids(rows, (float*)p_data, p_ids);
}

void
IDMAP::AddWithoutId(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSOR(dataset)

    std::vector<int64_t> new_ids(rows);
    for (int i = 0; i < rows; ++i) {
        new_ids[i] = i;
    }

    index_->add_with_ids(rows, (float*)p_data, new_ids.data());
}

int64_t
IDMAP::Count() {
    return index_->ntotal;
}

int64_t
IDMAP::Dimension() {
    return index_->d;
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

const char* type = "IDMap,Flat";

void
IDMAP::Train(const Config& config) {
    config->CheckValid();

    auto index = faiss::index_factory(config->d, type, GetMetricType(config->metric_type));
    index_.reset(index);
}

// VectorIndexPtr
// IDMAP::Clone() {
//    std::lock_guard<std::mutex> lk(mutex_);
//
//    auto clone_index = faiss::clone_index(index_.get());
//    std::shared_ptr<faiss::Index> new_index;
//    new_index.reset(clone_index);
//    return std::make_shared<IDMAP>(new_index);
//}

VectorIndexPtr
IDMAP::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
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

void
IDMAP::Seal() {
    // do nothing
}

}  // namespace knowhere
