/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <stdexcept>
#include "src/cache/GpuCacheMgr.h"

#include "src/metrics/Metrics.h"
#include "db/Log.h"
#include "utils/CommonUtil.h"

#include "src/cache/CpuCacheMgr.h"
#include "ExecutionEngineImpl.h"
#include "wrapper/knowhere/vec_index.h"
#include "wrapper/knowhere/vec_impl.h"
#include "knowhere/common/exception.h"
#include "db/Exception.h"


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
    if (!index_) throw Exception("Create Empty VecIndex");

    Config build_cfg;
    build_cfg["dim"] = dimension;
    build_cfg["metric_type"] = (metric_type_ == MetricType::IP) ? "IP" : "L2";
    AutoGenParams(index_->GetType(), 0, build_cfg);
    auto ec = std::static_pointer_cast<BFIndex>(index_)->Build(build_cfg);
    if (ec != server::KNOWHERE_SUCCESS) { throw Exception("Build index error"); }
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
    if (ec != server::KNOWHERE_SUCCESS) {
        return Status::Error("Add error");
    }
    return Status::OK();
}

size_t ExecutionEngineImpl::Count() const {
    return index_->Count();
}

size_t ExecutionEngineImpl::Size() const {
    return (size_t) (Count() * Dimension()) * sizeof(float);
}

size_t ExecutionEngineImpl::Dimension() const {
    return index_->Dimension();
}

size_t ExecutionEngineImpl::PhysicalSize() const {
    return server::CommonUtil::GetFileSize(location_);
}

Status ExecutionEngineImpl::Serialize() {
    auto ec = write_index(index_, location_);
    if (ec != server::KNOWHERE_SUCCESS) {
        return Status::Error("Serialize: write to disk error");
    }
    return Status::OK();
}

Status ExecutionEngineImpl::Load(bool to_cache) {
    index_ = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    bool already_in_cache = (index_ != nullptr);
    if (!index_) {
        try {
            double physical_size = PhysicalSize();
            server::CollectExecutionEngineMetrics metrics(physical_size);
            index_ = read_index(location_);
            ENGINE_LOG_DEBUG << "Disk io from: " << location_;
        } catch (knowhere::KnowhereException &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status::Error(e.what());
        } catch (std::exception &e) {
            return Status::Error(e.what());
        }
    }

    if (!already_in_cache && to_cache) {
        Cache();
    }
    return Status::OK();
}

Status ExecutionEngineImpl::CopyToGpu(uint64_t device_id) {
    auto index = zilliz::milvus::cache::GpuCacheMgr::GetInstance(device_id)->GetIndex(location_);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        try {
            index_ = index_->CopyToGpu(device_id);
            ENGINE_LOG_DEBUG << "CPU to GPU" << device_id;
        } catch (knowhere::KnowhereException &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status::Error(e.what());
        } catch (std::exception &e) {
            return Status::Error(e.what());
        }
    }

    if (!already_in_cache) {
        GpuCache(device_id);
    }

    return Status::OK();
}

Status ExecutionEngineImpl::CopyToCpu() {
    auto index = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location_);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        try {
            index_ = index_->CopyToCpu();
            ENGINE_LOG_DEBUG << "GPU to CPU";
        } catch (knowhere::KnowhereException &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status::Error(e.what());
        } catch (std::exception &e) {
            return Status::Error(e.what());
        }
    }

    if (!already_in_cache) {
        Cache();
    }
    return Status::OK();
}

ExecutionEnginePtr ExecutionEngineImpl::Clone() {
    auto ret = std::make_shared<ExecutionEngineImpl>(dim_, location_, index_type_, metric_type_, nlist_);
    ret->Init();
    ret->index_ = index_->Clone();
    return ret;
}

Status ExecutionEngineImpl::Merge(const std::string &location) {
    if (location == location_) {
        return Status::Error("Cannot Merge Self");
    }
    ENGINE_LOG_DEBUG << "Merge index file: " << location << " to: " << location_;

    auto to_merge = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(location);
    if (!to_merge) {
        try {
            double physical_size = server::CommonUtil::GetFileSize(location);
            server::CollectExecutionEngineMetrics metrics(physical_size);
            to_merge = read_index(location);
        } catch (knowhere::KnowhereException &e) {
            ENGINE_LOG_ERROR << e.what();
            return Status::Error(e.what());
        } catch (std::exception &e) {
            return Status::Error(e.what());
        }
    }

    if (auto file_index = std::dynamic_pointer_cast<BFIndex>(to_merge)) {
        auto ec = index_->Add(file_index->Count(), file_index->GetRawVectors(), file_index->GetRawIds());
        if (ec != server::KNOWHERE_SUCCESS) {
            ENGINE_LOG_ERROR << "Merge: Add Error";
            return Status::Error("Merge: Add Error");
        }
        return Status::OK();
    } else {
        return Status::Error("file index type is not idmap");
    }
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string &location, EngineType engine_type) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<BFIndex>(index_);
    auto to_index = CreatetVecIndex(engine_type);
    if (!to_index) {
        throw Exception("Create Empty VecIndex");
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
    if (ec != server::KNOWHERE_SUCCESS) { throw Exception("Build index error"); }

    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, nlist_);
}

Status ExecutionEngineImpl::Search(long n,
                                   const float *data,
                                   long k,
                                   long nprobe,
                                   float *distances,
                                   long *labels) const {
    ENGINE_LOG_DEBUG << "Search Params: [k]  " << k << " [nprobe] " << nprobe;
    auto ec = index_->Search(n, data, distances, labels, Config::object{{"k", k}, {"nprobe", nprobe}});
    if (ec != server::KNOWHERE_SUCCESS) {
        ENGINE_LOG_ERROR << "Search error";
        return Status::Error("Search: Search Error");
    }
    return Status::OK();
}

Status ExecutionEngineImpl::Cache() {
    zilliz::milvus::cache::CpuCacheMgr::GetInstance()->InsertItem(location_, index_);

    return Status::OK();
}

Status ExecutionEngineImpl::GpuCache(uint64_t gpu_id) {
    zilliz::milvus::cache::GpuCacheMgr::GetInstance(gpu_id)->InsertItem(location_, index_);

    return Status::OK();
}

// TODO(linxj): remove.
Status ExecutionEngineImpl::Init() {
    using namespace zilliz::milvus::server;
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
    gpu_num_ = server_config.GetInt32Value("gpu_index", 0);

    return Status::OK();
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
