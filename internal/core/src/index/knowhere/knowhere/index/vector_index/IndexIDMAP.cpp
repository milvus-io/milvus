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

    auto ret = SerializeImpl(index_type_);
    if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, ret);
    }
    return ret;
}

void
IDMAP::Load(const BinarySet& binary_set) {
    Assemble(const_cast<BinarySet&>(binary_set));
    LoadImpl(binary_set, index_type_);
}

void
IDMAP::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    // users will assign the metric type when querying
    // so we let L2 be the default type
    constexpr faiss::MetricType metric_type = faiss::METRIC_L2;

    auto dim = config[meta::DIM].get<int64_t>();
    auto index = std::make_shared<faiss::IndexFlat>(dim, metric_type);
    index_ = index;
}

void
IDMAP::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    GET_TENSOR_DATA(dataset_ptr)
    index_->add(rows, reinterpret_cast<const float*>(p_data));
}

DatasetPtr
IDMAP::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr)

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
}

DynamicResultSegment
IDMAP::QueryByDistance(const milvus::knowhere::DatasetPtr& dataset,
                       const milvus::knowhere::Config& config,
                       const faiss::BitsetView bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset)
    if (rows != 1) {
        KNOWHERE_THROW_MSG("QueryByDistance only accept nq = 1!");
    }

    auto default_type = index_->metric_type;
    if (config.contains(Metric::TYPE)) {
        index_->metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    }
    std::vector<faiss::RangeSearchPartialResult*> res;
    DynamicResultSegment result;
    auto radius = config[IndexParams::range_search_radius].get<float>();
    auto buffer_size = config.contains(IndexParams::range_search_buffer_size)
                           ? config[IndexParams::range_search_buffer_size].get<size_t>()
                           : 16384;
    auto real_idx = dynamic_cast<faiss::IndexFlat*>(index_.get());
    if (real_idx == nullptr) {
        KNOWHERE_THROW_MSG("Cannot dynamic_cast the index to faiss::IndexFlat type!");
    }
    if (index_->metric_type == faiss::MetricType::METRIC_L2) {
        radius *= radius;
    }
    real_idx->range_search(rows, reinterpret_cast<const float*>(p_data), radius, res, buffer_size, bitset);
    ExchangeDataset(result, res);
    MapUids(result);
    index_->metric_type = default_type;
    return result;
}

int64_t
IDMAP::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
IDMAP::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

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
        auto flat_index = dynamic_cast<faiss::IndexFlat*>(index_.get());
        return flat_index->xb.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IDMAP::QueryImpl(int64_t n,
                 const float* data,
                 int64_t k,
                 float* distances,
                 int64_t* labels,
                 const Config& config,
                 const faiss::BitsetView bitset) {
    // assign the metric type
    index_->metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    index_->search(n, data, k, distances, labels, bitset);
}

}  // namespace knowhere
}  // namespace milvus
