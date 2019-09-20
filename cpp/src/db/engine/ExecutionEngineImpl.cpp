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

#include "ExecutionEngineImpl.h"
#include "cache/GpuCacheMgr.h"
#include "cache/CpuCacheMgr.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"

#include "src/wrapper/vec_index.h"
#include "src/wrapper/vec_impl.h"
#include "knowhere/common/Exception.h"

#include <stdexcept>

namespace zilliz {
namespace milvus {
namespace engine {

ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension,
                                         const std::string &location,
                                         EngineType index_type,
                                         MetricType metric_type,
                                         int32_t nlist)
    : location_(location),
      dim_(dimension),
      index_type_(index_type),
      metric_type_(metric_type),
      nlist_(nlist) {

    index_ = CreatetVecIndex(EngineType::FAISS_IDMAP);
    if (!index_) {
        throw Exception(DB_ERROR, "Could not create VecIndex");
    }

    Config build_cfg;
    build_cfg["dim"] = dimension;
    build_cfg["metric_type"] = (metric_type_ == MetricType::IP) ? "IP" : "L2";
    AutoGenParams(index_->GetType(), 0, build_cfg);
    auto ec = std::static_pointer_cast<BFIndex>(index_)->Build(build_cfg);
    if (ec != KNOWHERE_SUCCESS) {
        throw Exception(DB_ERROR, "Build index error");
    }
}

ExecutionEngineImpl::ExecutionEngineImpl(VecIndexPtr index,
                                         const std::string &location,
                                         EngineType index_type,
                                         MetricType metric_type,
                                         int32_t nlist)
    : index_(std::move(index)),
      location_(location),
      index_type_(index_type),
      metric_type_(metric_type),
      nlist_(nlist) {
}

VecIndexPtr ExecutionEngineImpl::CreatetVecIndex(EngineType type) {
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
        default: {
            ENGINE_LOG_ERROR << "Invalid engine type";
            return nullptr;
        }
    }
    return index;
}

Status ExecutionEngineImpl::AddWithIds(long n, const float *xdata, const long *xids) {
    auto ec = index_->Add(n, xdata, xids);
    if (ec != KNOWHERE_SUCCESS) {
        return Status(DB_ERROR, "Add error");
    }
    return Status::OK();
}

size_t ExecutionEngineImpl::Count() const {
    if(index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return count 0";
        return 0;
    }
    return index_->Count();
}

size_t ExecutionEngineImpl::Size() const {
    return (size_t) (Count() * Dimension()) * sizeof(float);
}

size_t ExecutionEngineImpl::Dimension() const {
    if(index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return dimension " << dim_;
        return dim_;
    }
    return index_->Dimension();
}

size_t ExecutionEngineImpl::PhysicalSize() const {
    return server::CommonUtil::GetFileSize(location_);
}

Status ExecutionEngineImpl::Serialize() {
    auto ec = write_index(index_, location_);
    if (ec != KNOWHERE_SUCCESS) {
        return Status(DB_ERROR, "Serialize: write to disk error");
    }
    return Status::OK();
}

Status ExecutionEngineImpl::Load(bool to_cache) {
    index_ = cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    bool already_in_cache = (index_ != nullptr);
    if (!already_in_cache) {
        try {
            double physical_size = PhysicalSize();
            server::CollectExecutionEngineMetrics metrics(physical_size);
            index_ = read_index(location_);
            if(index_ == nullptr) {
                std::string msg = "Failed to load index from " + location_;
                ENGINE_LOG_ERROR << msg;
                return Status(DB_ERROR, msg);
            } else {
                ENGINE_LOG_DEBUG << "Disk io from: " << location_;
            }
        } catch (std::exception &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache && to_cache) {
        Cache();
    }
    return Status::OK();
}

Status ExecutionEngineImpl::CopyToGpu(uint64_t device_id) {
    auto index = cache::GpuCacheMgr::GetInstance(device_id)->GetIndex(location_);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if(index_ == nullptr) {
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to gpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = index_->CopyToGpu(device_id);
            ENGINE_LOG_DEBUG << "CPU to GPU" << device_id;
        } catch (std::exception &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache) {
        GpuCache(device_id);
    }

    return Status::OK();
}

Status ExecutionEngineImpl::CopyToCpu() {
    auto index = cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if(index_ == nullptr) {
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to cpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = index_->CopyToCpu();
            ENGINE_LOG_DEBUG << "GPU to CPU";
        } catch (std::exception &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache) {
        Cache();
    }
    return Status::OK();
}

ExecutionEnginePtr ExecutionEngineImpl::Clone() {
    if(index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to clone";
        return nullptr;
    }

    auto ret = std::make_shared<ExecutionEngineImpl>(dim_, location_, index_type_, metric_type_, nlist_);
    ret->Init();
    ret->index_ = index_->Clone();
    return ret;
}

Status ExecutionEngineImpl::Merge(const std::string &location) {
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
        } catch (std::exception &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if(index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to merge";
        return Status(DB_ERROR, "index is null");
    }

    if (auto file_index = std::dynamic_pointer_cast<BFIndex>(to_merge)) {
        auto ec = index_->Add(file_index->Count(), file_index->GetRawVectors(), file_index->GetRawIds());
        if (ec != KNOWHERE_SUCCESS) {
            ENGINE_LOG_ERROR << "Merge: Add Error";
            return Status(DB_ERROR, "Merge: Add Error");
        }
        return Status::OK();
    } else {
        return Status(DB_ERROR, "file index type is not idmap");
    }
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string &location, EngineType engine_type) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<BFIndex>(index_);
    if(from_index == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: from_index is null, failed to build index";
        return nullptr;
    }

    auto to_index = CreatetVecIndex(engine_type);
    if (!to_index) {
        throw Exception(DB_ERROR, "Could not create VecIndex");
    }

    Config build_cfg;
    build_cfg["dim"] = Dimension();
    build_cfg["metric_type"] = (metric_type_ == MetricType::IP) ? "IP" : "L2";
    build_cfg["gpu_id"] = gpu_num_;
    build_cfg["nlist"] = nlist_;
    AutoGenParams(to_index->GetType(), Count(), build_cfg);

    auto ec = to_index->BuildAll(Count(),
                                 from_index->GetRawVectors(),
                                 from_index->GetRawIds(),
                                 build_cfg);
    if (ec != KNOWHERE_SUCCESS) { throw Exception(DB_ERROR, "Build index error"); }

    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, nlist_);
}

Status ExecutionEngineImpl::Search(long n,
                                   const float *data,
                                   long k,
                                   long nprobe,
                                   float *distances,
                                   long *labels) const {
    if(index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to search";
        return Status(DB_ERROR, "index is null");
    }

    ENGINE_LOG_DEBUG << "Search Params: [k]  " << k << " [nprobe] " << nprobe;
    auto cfg = Config::object{{"k", k}, {"nprobe", nprobe}};
    auto ec = index_->Search(n, data, distances, labels, cfg);
    if (ec != KNOWHERE_SUCCESS) {
        ENGINE_LOG_ERROR << "Search error";
        return Status(DB_ERROR, "Search: Search Error");
    }
    return Status::OK();
}

Status ExecutionEngineImpl::Cache() {
    cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index_, PhysicalSize());
    zilliz::milvus::cache::CpuCacheMgr::GetInstance()->InsertItem(location_, obj);

    return Status::OK();
}

Status ExecutionEngineImpl::GpuCache(uint64_t gpu_id) {
    cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index_, PhysicalSize());
    zilliz::milvus::cache::GpuCacheMgr::GetInstance(gpu_id)->InsertItem(location_, obj);

    return Status::OK();
}

// TODO(linxj): remove.
Status ExecutionEngineImpl::Init() {
    using namespace zilliz::milvus::server;
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_DB);
    gpu_num_ = server_config.GetInt32Value(CONFIG_DB_BUILD_INDEX_GPU, std::stoi(CONFIG_DB_BUILD_INDEX_GPU_DEFAULT));

    return Status::OK();
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
