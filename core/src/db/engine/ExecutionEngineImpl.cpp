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

#include "db/engine/ExecutionEngineImpl.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "knowhere/common/Config.h"
#include "metrics/Metrics.h"
#include "scheduler/Utils.h"
#include "server/Config.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "wrapper/ConfAdapter.h"
#include "wrapper/ConfAdapterMgr.h"
#include "wrapper/VecImpl.h"
#include "wrapper/VecIndex.h"

#include <stdexcept>
#include <utility>
#include <vector>

//#define ON_SEARCH
namespace milvus {
namespace engine {

class CachedQuantizer : public cache::DataObj {
 public:
    explicit CachedQuantizer(knowhere::QuantizerPtr data) : data_(std::move(data)) {
    }

    knowhere::QuantizerPtr
    Data() {
        return data_;
    }

    int64_t
    Size() override {
        return data_->size;
    }

 private:
    knowhere::QuantizerPtr data_;
};

ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension, const std::string& location, EngineType index_type,
                                         MetricType metric_type, int32_t nlist)
    : location_(location), dim_(dimension), index_type_(index_type), metric_type_(metric_type), nlist_(nlist) {
    index_ = CreatetVecIndex(EngineType::FAISS_IDMAP);
    if (!index_) {
        throw Exception(DB_ERROR, "Unsupported index type");
    }

    TempMetaConf temp_conf;
    temp_conf.gpu_id = gpu_num_;
    temp_conf.dim = dimension;
    temp_conf.metric_type = (metric_type_ == MetricType::IP) ? knowhere::METRICTYPE::IP : knowhere::METRICTYPE::L2;
    auto adapter = AdapterMgr::GetInstance().GetAdapter(index_->GetType());
    auto conf = adapter->Match(temp_conf);

    auto ec = std::static_pointer_cast<BFIndex>(index_)->Build(conf);
    if (ec != KNOWHERE_SUCCESS) {
        throw Exception(DB_ERROR, "Build index error");
    }
}

ExecutionEngineImpl::ExecutionEngineImpl(VecIndexPtr index, const std::string& location, EngineType index_type,
                                         MetricType metric_type, int32_t nlist)
    : index_(std::move(index)), location_(location), index_type_(index_type), metric_type_(metric_type), nlist_(nlist) {
}

VecIndexPtr
ExecutionEngineImpl::CreatetVecIndex(EngineType type) {
    std::shared_ptr<VecIndex> index;
    switch (type) {
        case EngineType::FAISS_IDMAP: {
            index = GetVecIndexFactory(IndexType::FAISS_IDMAP);
            break;
        }
        case EngineType::FAISS_IVFFLAT: {
            index = GetVecIndexFactory(IndexType::FAISS_IVFFLAT_MIX);
            break;
        }
        case EngineType::FAISS_IVFSQ8: {
            index = GetVecIndexFactory(IndexType::FAISS_IVFSQ8_MIX);
            break;
        }
        case EngineType::NSG_MIX: {
            index = GetVecIndexFactory(IndexType::NSG_MIX);
            break;
        }
        case EngineType::FAISS_IVFSQ8H: {
            index = GetVecIndexFactory(IndexType::FAISS_IVFSQ8_HYBRID);
            break;
        }
        default: {
            ENGINE_LOG_ERROR << "Unsupported index type";
            return nullptr;
        }
    }
    return index;
}

void
ExecutionEngineImpl::HybridLoad() const {
    if (index_type_ != EngineType::FAISS_IVFSQ8H) {
        return;
    }

    if (index_->GetType() == IndexType::FAISS_IDMAP) {
        ENGINE_LOG_WARNING << "HybridLoad with type FAISS_IDMAP, ignore";
        return;
    }

    const std::string key = location_ + ".quantizer";
    std::vector<uint64_t> gpus = scheduler::get_gpu_pool();

    // cache hit
    {
        const int64_t NOT_FOUND = -1;
        int64_t device_id = NOT_FOUND;
        knowhere::QuantizerPtr quantizer = nullptr;

        for (auto& gpu : gpus) {
            auto cache = cache::GpuCacheMgr::GetInstance(gpu);
            if (auto cached_quantizer = cache->GetIndex(key)) {
                device_id = gpu;
                quantizer = std::static_pointer_cast<CachedQuantizer>(cached_quantizer)->Data();
            }
        }

        if (device_id != NOT_FOUND) {
            index_->SetQuantizer(quantizer);
            return;
        }
    }

    // cache miss
    {
        std::vector<int64_t> all_free_mem;
        for (auto& gpu : gpus) {
            auto cache = cache::GpuCacheMgr::GetInstance(gpu);
            auto free_mem = cache->CacheCapacity() - cache->CacheUsage();
            all_free_mem.push_back(free_mem);
        }

        auto max_e = std::max_element(all_free_mem.begin(), all_free_mem.end());
        auto best_index = std::distance(all_free_mem.begin(), max_e);
        auto best_device_id = gpus[best_index];

        auto quantizer_conf = std::make_shared<knowhere::QuantizerCfg>();
        quantizer_conf->mode = 1;
        quantizer_conf->gpu_id = best_device_id;
        auto quantizer = index_->LoadQuantizer(quantizer_conf);
        if (quantizer == nullptr) {
            ENGINE_LOG_ERROR << "quantizer is nullptr";
        }
        index_->SetQuantizer(quantizer);
        auto cache_quantizer = std::make_shared<CachedQuantizer>(quantizer);
        cache::GpuCacheMgr::GetInstance(best_device_id)->InsertItem(key, cache_quantizer);
    }
}

void
ExecutionEngineImpl::HybridUnset() const {
    if (index_type_ != EngineType::FAISS_IVFSQ8H) {
        return;
    }
    if (index_->GetType() == IndexType::FAISS_IDMAP) {
        return;
    }
    index_->UnsetQuantizer();
}

Status
ExecutionEngineImpl::AddWithIds(int64_t n, const float* xdata, const int64_t* xids) {
    auto status = index_->Add(n, xdata, xids);
    return status;
}

size_t
ExecutionEngineImpl::Count() const {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return count 0";
        return 0;
    }
    return index_->Count();
}

size_t
ExecutionEngineImpl::Size() const {
    return (size_t)(Count() * Dimension()) * sizeof(float);
}

size_t
ExecutionEngineImpl::Dimension() const {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return dimension " << dim_;
        return dim_;
    }
    return index_->Dimension();
}

size_t
ExecutionEngineImpl::PhysicalSize() const {
    return server::CommonUtil::GetFileSize(location_);
}

Status
ExecutionEngineImpl::Serialize() {
    auto status = write_index(index_, location_);
    return status;
}

Status
ExecutionEngineImpl::Load(bool to_cache) {
    index_ = std::static_pointer_cast<VecIndex>(cache::CpuCacheMgr::GetInstance()->GetIndex(location_));
    bool already_in_cache = (index_ != nullptr);
    if (!already_in_cache) {
        try {
            double physical_size = PhysicalSize();
            server::CollectExecutionEngineMetrics metrics(physical_size);
            index_ = read_index(location_);
            if (index_ == nullptr) {
                std::string msg = "Failed to load index from " + location_;
                ENGINE_LOG_ERROR << msg;
                return Status(DB_ERROR, msg);
            } else {
                ENGINE_LOG_DEBUG << "Disk io from: " << location_;
            }
        } catch (std::exception& e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache && to_cache) {
        Cache();
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToGpu(uint64_t device_id, bool hybrid) {
    if (hybrid) {
        const std::string key = location_ + ".quantizer";
        std::vector<uint64_t> gpus = scheduler::get_gpu_pool();

        const int64_t NOT_FOUND = -1;
        int64_t device_id = NOT_FOUND;

        // cache hit
        {
            knowhere::QuantizerPtr quantizer = nullptr;

            for (auto& gpu : gpus) {
                auto cache = cache::GpuCacheMgr::GetInstance(gpu);
                if (auto cached_quantizer = cache->GetIndex(key)) {
                    device_id = gpu;
                    quantizer = std::static_pointer_cast<CachedQuantizer>(cached_quantizer)->Data();
                }
            }

            if (device_id != NOT_FOUND) {
                // cache hit
                auto config = std::make_shared<knowhere::QuantizerCfg>();
                config->gpu_id = device_id;
                config->mode = 2;
                auto new_index = index_->LoadData(quantizer, config);
                index_ = new_index;
            }
        }

        if (device_id == NOT_FOUND) {
            // cache miss
            std::vector<int64_t> all_free_mem;
            for (auto& gpu : gpus) {
                auto cache = cache::GpuCacheMgr::GetInstance(gpu);
                auto free_mem = cache->CacheCapacity() - cache->CacheUsage();
                all_free_mem.push_back(free_mem);
            }

            auto max_e = std::max_element(all_free_mem.begin(), all_free_mem.end());
            auto best_index = std::distance(all_free_mem.begin(), max_e);
            device_id = gpus[best_index];

            auto pair = index_->CopyToGpuWithQuantizer(device_id);
            index_ = pair.first;

            // cache
            auto cached_quantizer = std::make_shared<CachedQuantizer>(pair.second);
            cache::GpuCacheMgr::GetInstance(device_id)->InsertItem(key, cached_quantizer);
        }
        return Status::OK();
    }

    auto index = std::static_pointer_cast<VecIndex>(cache::GpuCacheMgr::GetInstance(device_id)->GetIndex(location_));
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if (index_ == nullptr) {
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to gpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = index_->CopyToGpu(device_id);
            ENGINE_LOG_DEBUG << "CPU to GPU" << device_id;
        } catch (std::exception& e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache) {
        GpuCache(device_id);
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToIndexFileToGpu(uint64_t device_id) {
    auto to_index_data = std::make_shared<ToIndexData>(PhysicalSize());
    cache::DataObjPtr obj = std::static_pointer_cast<cache::DataObj>(to_index_data);
    milvus::cache::GpuCacheMgr::GetInstance(device_id)->InsertItem(location_, obj);
    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToCpu() {
    auto index = std::static_pointer_cast<VecIndex>(cache::CpuCacheMgr::GetInstance()->GetIndex(location_));
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if (index_ == nullptr) {
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to cpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = index_->CopyToCpu();
            ENGINE_LOG_DEBUG << "GPU to CPU";
        } catch (std::exception& e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache) {
        Cache();
    }
    return Status::OK();
}

ExecutionEnginePtr
ExecutionEngineImpl::Clone() {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to clone";
        return nullptr;
    }

    auto ret = std::make_shared<ExecutionEngineImpl>(dim_, location_, index_type_, metric_type_, nlist_);
    ret->Init();
    ret->index_ = index_->Clone();
    return ret;
}

Status
ExecutionEngineImpl::Merge(const std::string& location) {
    if (location == location_) {
        return Status(DB_ERROR, "Cannot Merge Self");
    }
    ENGINE_LOG_DEBUG << "Merge index file: " << location << " to: " << location_;

    auto to_merge = cache::CpuCacheMgr::GetInstance()->GetIndex(location);
    if (!to_merge) {
        try {
            double physical_size = server::CommonUtil::GetFileSize(location);
            server::CollectExecutionEngineMetrics metrics(physical_size);
            to_merge = read_index(location);
        } catch (std::exception& e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to merge";
        return Status(DB_ERROR, "index is null");
    }

    if (auto file_index = std::dynamic_pointer_cast<BFIndex>(to_merge)) {
        auto status = index_->Add(file_index->Count(), file_index->GetRawVectors(), file_index->GetRawIds());
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Merge: Add Error";
        }
        return status;
    } else {
        return Status(DB_ERROR, "file index type is not idmap");
    }
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string& location, EngineType engine_type) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<BFIndex>(index_);
    if (from_index == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: from_index is null, failed to build index";
        return nullptr;
    }

    auto to_index = CreatetVecIndex(engine_type);
    if (!to_index) {
        throw Exception(DB_ERROR, "Unsupported index type");
    }

    TempMetaConf temp_conf;
    temp_conf.gpu_id = gpu_num_;
    temp_conf.dim = Dimension();
    temp_conf.nlist = nlist_;
    temp_conf.metric_type = (metric_type_ == MetricType::IP) ? knowhere::METRICTYPE::IP : knowhere::METRICTYPE::L2;
    temp_conf.size = Count();

    auto adapter = AdapterMgr::GetInstance().GetAdapter(to_index->GetType());
    auto conf = adapter->Match(temp_conf);

    auto status = to_index->BuildAll(Count(), from_index->GetRawVectors(), from_index->GetRawIds(), conf);
    if (!status.ok()) {
        throw Exception(DB_ERROR, status.message());
    }

    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, nlist_);
}

Status
ExecutionEngineImpl::Search(int64_t n, const float* data, int64_t k, int64_t nprobe, float* distances, int64_t* labels,
                            bool hybrid) {
#if 0
    if (index_type_ == EngineType::FAISS_IVFSQ8H) {
        if (!hybrid) {
            const std::string key = location_ + ".quantizer";
            std::vector<uint64_t> gpus = scheduler::get_gpu_pool();

            const int64_t NOT_FOUND = -1;
            int64_t device_id = NOT_FOUND;

            // cache hit
            {
                knowhere::QuantizerPtr quantizer = nullptr;

                for (auto& gpu : gpus) {
                    auto cache = cache::GpuCacheMgr::GetInstance(gpu);
                    if (auto cached_quantizer = cache->GetIndex(key)) {
                        device_id = gpu;
                        quantizer = std::static_pointer_cast<CachedQuantizer>(cached_quantizer)->Data();
                    }
                }

                if (device_id != NOT_FOUND) {
                    // cache hit
                    auto config = std::make_shared<knowhere::QuantizerCfg>();
                    config->gpu_id = device_id;
                    config->mode = 2;
                    auto new_index = index_->LoadData(quantizer, config);
                    index_ = new_index;
                }
            }

            if (device_id == NOT_FOUND) {
                // cache miss
                std::vector<int64_t> all_free_mem;
                for (auto& gpu : gpus) {
                    auto cache = cache::GpuCacheMgr::GetInstance(gpu);
                    auto free_mem = cache->CacheCapacity() - cache->CacheUsage();
                    all_free_mem.push_back(free_mem);
                }

                auto max_e = std::max_element(all_free_mem.begin(), all_free_mem.end());
                auto best_index = std::distance(all_free_mem.begin(), max_e);
                device_id = gpus[best_index];

                auto pair = index_->CopyToGpuWithQuantizer(device_id);
                index_ = pair.first;

                // cache
                auto cached_quantizer = std::make_shared<CachedQuantizer>(pair.second);
                cache::GpuCacheMgr::GetInstance(device_id)->InsertItem(key, cached_quantizer);
            }
        }
    }
#endif

    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to search";
        return Status(DB_ERROR, "index is null");
    }

    ENGINE_LOG_DEBUG << "Search Params: [k]  " << k << " [nprobe] " << nprobe;

    // TODO(linxj): remove here. Get conf from function
    TempMetaConf temp_conf;
    temp_conf.k = k;
    temp_conf.nprobe = nprobe;

    auto adapter = AdapterMgr::GetInstance().GetAdapter(index_->GetType());
    auto conf = adapter->MatchSearch(temp_conf, index_->GetType());

    if (hybrid) {
        HybridLoad();
    }

    auto status = index_->Search(n, data, distances, labels, conf);

    if (hybrid) {
        HybridUnset();
    }

    if (!status.ok()) {
        ENGINE_LOG_ERROR << "Search error";
    }
    return status;
}

Status
ExecutionEngineImpl::Cache() {
    cache::DataObjPtr obj = std::static_pointer_cast<cache::DataObj>(index_);
    milvus::cache::CpuCacheMgr::GetInstance()->InsertItem(location_, obj);

    return Status::OK();
}

Status
ExecutionEngineImpl::GpuCache(uint64_t gpu_id) {
    cache::DataObjPtr obj = std::static_pointer_cast<cache::DataObj>(index_);
    milvus::cache::GpuCacheMgr::GetInstance(gpu_id)->InsertItem(location_, obj);

    return Status::OK();
}

// TODO(linxj): remove.
Status
ExecutionEngineImpl::Init() {
    server::Config& config = server::Config::GetInstance();
    Status s = config.GetResourceConfigIndexBuildDevice(gpu_num_);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
