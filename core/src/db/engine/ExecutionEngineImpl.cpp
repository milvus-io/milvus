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

#include "db/engine/ExecutionEngineImpl.h"

#include <faiss/utils/ConcurrentBitset.h>
#include <fiu-local.h>

#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "config/Config.h"
#include "db/Utils.h"
#include "knowhere/common/Config.h"
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/GPUIndex.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/gpu/Quantizer.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "metrics/Metrics.h"
#include "scheduler/Utils.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/Status.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace engine {

namespace {

Status
MappingMetricType(MetricType metric_type, milvus::json& conf) {
    switch (metric_type) {
        case MetricType::IP:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::IP;
            break;
        case MetricType::L2:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::L2;
            break;
        case MetricType::HAMMING:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::HAMMING;
            break;
        case MetricType::JACCARD:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::JACCARD;
            break;
        case MetricType::TANIMOTO:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::TANIMOTO;
            break;
        case MetricType::SUBSTRUCTURE:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::SUBSTRUCTURE;
            break;
        case MetricType::SUPERSTRUCTURE:
            conf[knowhere::Metric::TYPE] = knowhere::Metric::SUPERSTRUCTURE;
            break;
        default:
            return Status(DB_ERROR, "Unsupported metric type");
    }

    return Status::OK();
}

bool
IsBinaryIndexType(knowhere::IndexType type) {
    return type == knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP || type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
}

}  // namespace

#ifdef MILVUS_GPU_VERSION
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
#endif

ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension, const std::string& location, EngineType index_type,
                                         MetricType metric_type, const milvus::json& index_params)
    : location_(location),
      dim_(dimension),
      index_type_(index_type),
      metric_type_(metric_type),
      index_params_(index_params) {
    EngineType tmp_index_type =
        utils::IsBinaryMetricType((int32_t)metric_type) ? EngineType::FAISS_BIN_IDMAP : EngineType::FAISS_IDMAP;
    index_ = CreatetVecIndex(tmp_index_type);
    if (!index_) {
        throw Exception(DB_ERROR, "Unsupported index type");
    }

    milvus::json conf = index_params;
    conf[knowhere::meta::DEVICEID] = gpu_num_;
    conf[knowhere::meta::DIM] = dimension;
    MappingMetricType(metric_type, conf);
    LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckTrain(conf, index_->index_mode())) {
        throw Exception(DB_ERROR, "Illegal index params");
    }

    fiu_do_on("ExecutionEngineImpl.throw_exception", throw Exception(DB_ERROR, ""));
    if (auto bf_index = std::dynamic_pointer_cast<knowhere::IDMAP>(index_)) {
        bf_index->Train(knowhere::DatasetPtr(), conf);
    } else if (auto bf_bin_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_)) {
        bf_bin_index->Train(knowhere::DatasetPtr(), conf);
    }
}

ExecutionEngineImpl::ExecutionEngineImpl(knowhere::VecIndexPtr index, const std::string& location,
                                         EngineType index_type, MetricType metric_type,
                                         const milvus::json& index_params)
    : index_(std::move(index)),
      location_(location),
      index_type_(index_type),
      metric_type_(metric_type),
      index_params_(index_params) {
}

knowhere::VecIndexPtr
ExecutionEngineImpl::CreatetVecIndex(EngineType type) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    knowhere::IndexMode mode = knowhere::IndexMode::MODE_CPU;
#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();
    bool gpu_resource_enable = true;
    config.GetGpuResourceConfigEnable(gpu_resource_enable);
    fiu_do_on("ExecutionEngineImpl.CreatetVecIndex.gpu_res_disabled", gpu_resource_enable = false);
    if (gpu_resource_enable) {
        mode = knowhere::IndexMode::MODE_GPU;
    }
#endif

    fiu_do_on("ExecutionEngineImpl.CreateVecIndex.invalid_type", type = EngineType::INVALID);
    knowhere::VecIndexPtr index = nullptr;
    switch (type) {
        case EngineType::FAISS_IDMAP: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IDMAP, mode);
            break;
        }
        case EngineType::FAISS_IVFFLAT: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, mode);
            break;
        }
        case EngineType::FAISS_PQ: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFPQ, mode);
            break;
        }
        case EngineType::FAISS_IVFSQ8: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, mode);
            break;
        }
#ifdef MILVUS_GPU_VERSION
        case EngineType::FAISS_IVFSQ8H: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H, mode);
            break;
        }
#endif
        case EngineType::FAISS_BIN_IDMAP: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, mode);
            break;
        }
        case EngineType::FAISS_BIN_IVFFLAT: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, mode);
            break;
        }
        case EngineType::NSG_MIX: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_NSG, mode);
            break;
        }
        case EngineType::SPTAG_KDT: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, mode);
            break;
        }
        case EngineType::SPTAG_BKT: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, mode);
            break;
        }
        case EngineType::HNSW: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_HNSW, mode);
            break;
        }
        case EngineType::ANNOY: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_ANNOY, mode);
            break;
        }
        default: {
            LOG_ENGINE_ERROR_ << "Unsupported index type " << (int)type;
            return nullptr;
        }
    }
    if (index == nullptr) {
        std::string err_msg = "Invalid index type " + std::to_string((int)type) + " mod " + std::to_string((int)mode);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(DB_ERROR, err_msg);
    }
    return index;
}

void
ExecutionEngineImpl::HybridLoad() const {
#ifdef MILVUS_GPU_VERSION
    auto hybrid_index = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_);
    if (hybrid_index == nullptr) {
        LOG_ENGINE_WARNING_ << "HybridLoad only support with IVFSQHybrid";
        return;
    }

    const std::string key = location_ + ".quantizer";

    server::Config& config = server::Config::GetInstance();
    std::vector<int64_t> gpus;
    Status s = config.GetGpuResourceConfigSearchResources(gpus);
    if (!s.ok()) {
        LOG_ENGINE_ERROR_ << s.message();
        return;
    }

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
            hybrid_index->SetQuantizer(quantizer);
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

        milvus::json quantizer_conf{{knowhere::meta::DEVICEID, best_device_id}, {"mode", 1}};
        auto quantizer = hybrid_index->LoadQuantizer(quantizer_conf);
        LOG_ENGINE_DEBUG_ << "Quantizer params: " << quantizer_conf.dump();
        if (quantizer == nullptr) {
            LOG_ENGINE_ERROR_ << "quantizer is nullptr";
        }
        hybrid_index->SetQuantizer(quantizer);
        auto cache_quantizer = std::make_shared<CachedQuantizer>(quantizer);
        cache::GpuCacheMgr::GetInstance(best_device_id)->InsertItem(key, cache_quantizer);
    }
#endif
}

void
ExecutionEngineImpl::HybridUnset() const {
#ifdef MILVUS_GPU_VERSION
    auto hybrid_index = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_);
    if (hybrid_index == nullptr) {
        return;
    }
    hybrid_index->UnsetQuantizer();
#endif
}

Status
ExecutionEngineImpl::AddWithIds(int64_t n, const float* xdata, const int64_t* xids) {
    auto dataset = knowhere::GenDatasetWithIds(n, index_->Dim(), xdata, xids);
    index_->Add(dataset, knowhere::Config());
    return Status::OK();
}

Status
ExecutionEngineImpl::AddWithIds(int64_t n, const uint8_t* xdata, const int64_t* xids) {
    auto dataset = knowhere::GenDatasetWithIds(n, index_->Dim(), xdata, xids);
    index_->Add(dataset, knowhere::Config());
    return Status::OK();
}

size_t
ExecutionEngineImpl::Count() const {
    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, return count 0";
        return 0;
    }
    return index_->Count();
}

size_t
ExecutionEngineImpl::Dimension() const {
    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, return dimension " << dim_;
        return dim_;
    }
    return index_->Dim();
}

size_t
ExecutionEngineImpl::Size() const {
    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, return size 0";
        return 0;
    }
    return index_->Size();
}

Status
ExecutionEngineImpl::Serialize() {
    std::string segment_dir;
    utils::GetParentPath(location_, segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(segment_dir);
    segment_writer_ptr->SetVectorIndex(index_);
    auto status = segment_writer_ptr->WriteVectorIndex(location_);

    if (!status.ok()) {
        return status;
    }

    // here we reset index size by file size,
    // since some index type(such as SQ8) data size become smaller after serialized
    index_->SetIndexSize(server::CommonUtil::GetFileSize(location_));
    LOG_ENGINE_DEBUG_ << "Finish serialize index file: " << location_ << " size: " << index_->Size();

    if (index_->Size() == 0) {
        std::string msg = "Failed to serialize file: " + location_ + " reason: out of disk space or memory";
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Load(bool to_cache) {
    index_ = std::static_pointer_cast<knowhere::VecIndex>(cache::CpuCacheMgr::GetInstance()->GetIndex(location_));
    bool already_in_cache = (index_ != nullptr);
    if (!already_in_cache) {
        std::string segment_dir;
        utils::GetParentPath(location_, segment_dir);
        auto segment_reader_ptr = std::make_shared<segment::SegmentReader>(segment_dir);
        knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();

        if (utils::IsRawIndexType((int32_t)index_type_)) {
            if (index_type_ == EngineType::FAISS_IDMAP) {
                index_ = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IDMAP);
            } else {
                index_ = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP);
            }
            milvus::json conf{{knowhere::meta::DEVICEID, gpu_num_}, {knowhere::meta::DIM, dim_}};
            MappingMetricType(metric_type_, conf);
            auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
            LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
            if (!adapter->CheckTrain(conf, index_->index_mode())) {
                throw Exception(DB_ERROR, "Illegal index params");
            }

            auto status = segment_reader_ptr->Load();
            if (!status.ok()) {
                std::string msg = "Failed to load segment from " + location_;
                LOG_ENGINE_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            segment::SegmentPtr segment_ptr;
            segment_reader_ptr->GetSegment(segment_ptr);
            auto& vectors = segment_ptr->vectors_ptr_;
            auto& deleted_docs = segment_ptr->deleted_docs_ptr_->GetDeletedDocs();

            auto& vectors_uids = vectors->GetMutableUids();
            auto count = vectors_uids.size();
            index_->SetUids(vectors_uids);
            LOG_ENGINE_DEBUG_ << "set uids " << index_->GetUids().size() << " for index " << location_;

            auto& vectors_data = vectors->GetData();

            auto attrs = segment_ptr->attrs_ptr_;

            auto attrs_it = attrs->attrs.begin();
            for (; attrs_it != attrs->attrs.end(); ++attrs_it) {
                attr_data_.insert(std::pair(attrs_it->first, attrs_it->second->GetData()));
                attr_size_.insert(std::pair(attrs_it->first, attrs_it->second->GetNbytes()));
            }

            vector_count_ = count;

            faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(count);
            for (auto& offset : deleted_docs) {
                concurrent_bitset_ptr->set(offset);
            }

            auto dataset = knowhere::GenDataset(count, this->dim_, vectors_data.data());
            if (index_type_ == EngineType::FAISS_IDMAP) {
                auto bf_index = std::static_pointer_cast<knowhere::IDMAP>(index_);
                bf_index->Train(knowhere::DatasetPtr(), conf);
                bf_index->AddWithoutIds(dataset, conf);
                bf_index->SetBlacklist(concurrent_bitset_ptr);
            } else if (index_type_ == EngineType::FAISS_BIN_IDMAP) {
                auto bin_bf_index = std::static_pointer_cast<knowhere::BinaryIDMAP>(index_);
                bin_bf_index->Train(knowhere::DatasetPtr(), conf);
                bin_bf_index->AddWithoutIds(dataset, conf);
                bin_bf_index->SetBlacklist(concurrent_bitset_ptr);
            }

            LOG_ENGINE_DEBUG_ << "Finished loading raw data from segment " << segment_dir;
        } else {
            try {
                segment::SegmentPtr segment_ptr;
                segment_reader_ptr->GetSegment(segment_ptr);
                auto status = segment_reader_ptr->LoadVectorIndex(location_, segment_ptr->vector_index_ptr_);
                index_ = segment_ptr->vector_index_ptr_->GetVectorIndex();

                if (index_ == nullptr) {
                    std::string msg = "Failed to load index from " + location_;
                    LOG_ENGINE_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                } else {
                    bool gpu_enable = false;
#ifdef MILVUS_GPU_VERSION
                    server::Config& config = server::Config::GetInstance();
                    STATUS_CHECK(config.GetGpuResourceConfigEnable(gpu_enable));
#endif
                    if (!gpu_enable && index_->index_mode() == knowhere::IndexMode::MODE_GPU) {
                        std::string err_msg = "Index with type " + index_->index_type() + " must be used in GPU mode";
                        LOG_ENGINE_ERROR_ << err_msg;
                        return Status(DB_ERROR, err_msg);
                    }
                    segment::DeletedDocsPtr deleted_docs_ptr;
                    auto status = segment_reader_ptr->LoadDeletedDocs(deleted_docs_ptr);
                    if (!status.ok()) {
                        std::string msg = "Failed to load deleted docs from " + location_;
                        LOG_ENGINE_ERROR_ << msg;
                        return Status(DB_ERROR, msg);
                    }
                    auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

                    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr =
                        std::make_shared<faiss::ConcurrentBitset>(index_->Count());
                    for (auto& offset : deleted_docs) {
                        if (!concurrent_bitset_ptr->test(offset)) {
                            concurrent_bitset_ptr->set(offset);
                        }
                    }

                    index_->SetBlacklist(concurrent_bitset_ptr);

                    std::vector<segment::doc_id_t> uids;
                    segment_reader_ptr->LoadUids(uids);
                    index_->SetUids(uids);
                    LOG_ENGINE_DEBUG_ << "set uids " << index_->GetUids().size() << " for index " << location_;

                    LOG_ENGINE_DEBUG_ << "Finished loading index file from segment " << segment_dir;
                }
            } catch (std::exception& e) {
                LOG_ENGINE_ERROR_ << e.what();
                return Status(DB_ERROR, e.what());
            }
        }
    }

    if (!already_in_cache && to_cache) {
        Cache();
    }
    return Status::OK();
}  // namespace engine

Status
ExecutionEngineImpl::CopyToGpu(uint64_t device_id, bool hybrid) {
#if 0
    if (hybrid) {
        const std::string key = location_ + ".quantizer";
        std::vector<uint64_t> gpus{device_id};

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
                milvus::json quantizer_conf{{knowhere::meta::DEVICEID : device_id}, {"mode" : 2}};
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
#endif

#ifdef MILVUS_GPU_VERSION
    auto data_obj_ptr = cache::GpuCacheMgr::GetInstance(device_id)->GetIndex(location_);
    auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if (index_ == nullptr) {
            LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, failed to copy to gpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            /* Index data is copied to GPU first, then added into GPU cache.
             * Add lock here to avoid multiple INDEX are copied to one GPU card at same time.
             * And reserve space to avoid GPU out of memory issue.
             */
            LOG_ENGINE_DEBUG_ << "CPU to GPU" << device_id << " start";
            auto gpu_cache_mgr = cache::GpuCacheMgr::GetInstance(device_id);
            // gpu_cache_mgr->Reserve(index_->Size());
            index_ = knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
            // gpu_cache_mgr->InsertItem(location_, std::static_pointer_cast<cache::DataObj>(index_));
            LOG_ENGINE_DEBUG_ << "CPU to GPU" << device_id << " finished";
        } catch (std::exception& e) {
            LOG_ENGINE_ERROR_ << e.what();
            return Status(DB_ERROR, e.what());
        }
    }
#endif

    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToIndexFileToGpu(uint64_t device_id) {
#ifdef MILVUS_GPU_VERSION
    // the ToIndexData is only a placeholder, cpu-copy-to-gpu action is performed in
    if (index_) {
        auto gpu_cache_mgr = milvus::cache::GpuCacheMgr::GetInstance(device_id);
        gpu_num_ = device_id;
        gpu_cache_mgr->Reserve(index_->Size());
    }
#endif
    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToCpu() {
#ifdef MILVUS_GPU_VERSION
    auto index = std::static_pointer_cast<knowhere::VecIndex>(cache::CpuCacheMgr::GetInstance()->GetIndex(location_));
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        index_ = index;
    } else {
        if (index_ == nullptr) {
            LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, failed to copy to cpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
            LOG_ENGINE_DEBUG_ << "GPU to CPU";
        } catch (std::exception& e) {
            LOG_ENGINE_ERROR_ << e.what();
            return Status(DB_ERROR, e.what());
        }
    }

    if (!already_in_cache) {
        Cache();
    }
    return Status::OK();
#else
    LOG_ENGINE_ERROR_ << "Calling ExecutionEngineImpl::CopyToCpu when using CPU version";
    return Status(DB_ERROR, "Calling ExecutionEngineImpl::CopyToCpu when using CPU version");
#endif
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string& location, EngineType engine_type) {
    LOG_ENGINE_DEBUG_ << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<knowhere::IDMAP>(index_);
    auto bin_from_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_);
    if (from_index == nullptr && bin_from_index == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: from_index is null, failed to build index";
        return nullptr;
    }

    auto to_index = CreatetVecIndex(engine_type);
    if (!to_index) {
        throw Exception(DB_ERROR, "Unsupported index type");
    }

    milvus::json conf = index_params_;
    conf[knowhere::meta::DIM] = Dimension();
    conf[knowhere::meta::ROWS] = Count();
    conf[knowhere::meta::DEVICEID] = gpu_num_;
    MappingMetricType(metric_type_, conf);
    LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(to_index->index_type());
    if (!adapter->CheckTrain(conf, to_index->index_mode())) {
        throw Exception(DB_ERROR, "Illegal index params");
    }
    LOG_ENGINE_DEBUG_ << "Index config: " << conf.dump();

    std::vector<segment::doc_id_t> uids;
    faiss::ConcurrentBitsetPtr blacklist;
    if (from_index) {
        auto dataset =
            knowhere::GenDatasetWithIds(Count(), Dimension(), from_index->GetRawVectors(), from_index->GetRawIds());
        to_index->BuildAll(dataset, conf);
        uids = from_index->GetUids();
        blacklist = from_index->GetBlacklist();
    } else if (bin_from_index) {
        auto dataset = knowhere::GenDatasetWithIds(Count(), Dimension(), bin_from_index->GetRawVectors(),
                                                   bin_from_index->GetRawIds());
        to_index->BuildAll(dataset, conf);
        uids = bin_from_index->GetUids();
        blacklist = bin_from_index->GetBlacklist();
    }

#ifdef MILVUS_GPU_VERSION
    /* for GPU index, need copy back to CPU */
    if (to_index->index_mode() == knowhere::IndexMode::MODE_GPU) {
        auto device_index = std::dynamic_pointer_cast<knowhere::GPUIndex>(to_index);
        to_index = device_index->CopyGpuToCpu(conf);
    }
#endif

    to_index->SetUids(uids);
    LOG_ENGINE_DEBUG_ << "Set " << to_index->GetUids().size() << "uids for " << location;
    if (blacklist != nullptr) {
        to_index->SetBlacklist(blacklist);
        LOG_ENGINE_DEBUG_ << "Set blacklist for index " << location;
    }

    LOG_ENGINE_DEBUG_ << "Finish build index: " << location;
    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, index_params_);
}

void
MapAndCopyResult(const knowhere::DatasetPtr& dataset, const std::vector<milvus::segment::doc_id_t>& uids, int64_t nq,
                 int64_t k, float* distances, int64_t* labels) {
    int64_t* res_ids = dataset->Get<int64_t*>(knowhere::meta::IDS);
    float* res_dist = dataset->Get<float*>(knowhere::meta::DISTANCE);

    memcpy(distances, res_dist, sizeof(float) * nq * k);

    /* map offsets to ids */
    int64_t num = nq * k;
    for (int64_t i = 0; i < num; ++i) {
        int64_t offset = res_ids[i];
        if (offset != -1) {
            labels[i] = uids[offset];
        } else {
            labels[i] = -1;
        }
    }

    free(res_ids);
    free(res_dist);
}

template <typename T>
void
ProcessRangeQuery(std::vector<T> data, T value, query::CompareOperator type, faiss::ConcurrentBitsetPtr& bitset) {
    switch (type) {
        case query::CompareOperator::LT: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] >= value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
            break;
        }
        case query::CompareOperator::LTE: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] > value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
            break;
        }
        case query::CompareOperator::GT: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] <= value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
            break;
        }
        case query::CompareOperator::GTE: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] < value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
            break;
        }
        case query::CompareOperator::EQ: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] != value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
        }
        case query::CompareOperator::NE: {
            for (uint64_t i = 0; i < data.size(); ++i) {
                if (data[i] == value) {
                    if (!bitset->test(i)) {
                        bitset->set(i);
                    }
                }
            }
            break;
        }
    }
}

Status
ExecutionEngineImpl::ExecBinaryQuery(milvus::query::GeneralQueryPtr general_query, faiss::ConcurrentBitsetPtr bitset,
                                     std::unordered_map<std::string, DataType>& attr_type, uint64_t& nq, uint64_t& topk,
                                     std::vector<float>& distances, std::vector<int64_t>& labels) {
    if (bitset == nullptr) {
        bitset = std::make_shared<faiss::ConcurrentBitset>(vector_count_);
    }

    if (general_query->leaf == nullptr) {
        Status status;
        if (general_query->bin->left_query != nullptr) {
            status = ExecBinaryQuery(general_query->bin->left_query, bitset, attr_type, nq, topk, distances, labels);
        }
        if (general_query->bin->right_query != nullptr) {
            status = ExecBinaryQuery(general_query->bin->right_query, bitset, attr_type, nq, topk, distances, labels);
        }
        return status;
    } else {
        if (general_query->leaf->term_query != nullptr) {
            // process attrs_data
            auto field_name = general_query->leaf->term_query->field_name;
            auto type = attr_type.at(field_name);
            auto size = attr_size_.at(field_name);
            switch (type) {
                case DataType::INT8: {
                    std::vector<int8_t> data;
                    data.resize(size / sizeof(int8_t));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);

                    std::vector<int8_t> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(int8_t);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(int8_t));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
                case DataType::INT16: {
                    std::vector<int16_t> data;
                    data.resize(size / sizeof(int16_t));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);
                    std::vector<int16_t> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(int16_t);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(int16_t));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
                case DataType::INT32: {
                    std::vector<int32_t> data;
                    data.resize(size / sizeof(int32_t));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);

                    std::vector<int32_t> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(int32_t);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(int32_t));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
                case DataType::INT64: {
                    std::vector<int64_t> data;
                    data.resize(size / sizeof(int64_t));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);

                    std::vector<int64_t> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(int64_t);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(int64_t));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
                case DataType::FLOAT: {
                    std::vector<float> data;
                    data.resize(size / sizeof(float));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);

                    std::vector<float> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(float);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(int64_t));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
                case DataType::DOUBLE: {
                    std::vector<double> data;
                    data.resize(size / sizeof(double));
                    memcpy(data.data(), attr_data_.at(field_name).data(), size);

                    std::vector<double> term_value;
                    auto term_size =
                        general_query->leaf->term_query->field_value.size() * (sizeof(int8_t)) / sizeof(double);
                    term_value.resize(term_size);
                    memcpy(term_value.data(), general_query->leaf->term_query->field_value.data(),
                           term_size * sizeof(double));

                    for (uint64_t i = 0; i < data.size(); ++i) {
                        bool value_in_term = false;
                        for (auto query_value : term_value) {
                            if (data[i] == query_value) {
                                value_in_term = true;
                                break;
                            }
                        }
                        if (!value_in_term) {
                            if (!bitset->test(i)) {
                                bitset->set(i);
                            }
                        }
                    }
                    break;
                }
            }
            return Status::OK();
        }
        if (general_query->leaf->range_query != nullptr) {
            auto field_name = general_query->leaf->range_query->field_name;
            auto com_expr = general_query->leaf->range_query->compare_expr;
            auto type = attr_type.at(field_name);
            auto size = attr_size_.at(field_name);
            for (uint64_t j = 0; j < com_expr.size(); ++j) {
                auto operand = com_expr[j].operand;
                switch (type) {
                    case DataType::INT8: {
                        std::vector<int8_t> data;
                        data.resize(size / sizeof(int8_t));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        int8_t value = atoi(operand.c_str());
                        ProcessRangeQuery<int8_t>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                    case DataType::INT16: {
                        std::vector<int16_t> data;
                        data.resize(size / sizeof(int16_t));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        int16_t value = atoi(operand.c_str());
                        ProcessRangeQuery<int16_t>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                    case DataType::INT32: {
                        std::vector<int32_t> data;
                        data.resize(size / sizeof(int32_t));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        int32_t value = atoi(operand.c_str());
                        ProcessRangeQuery<int32_t>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                    case DataType::INT64: {
                        std::vector<int64_t> data;
                        data.resize(size / sizeof(int64_t));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        int64_t value = atoi(operand.c_str());
                        ProcessRangeQuery<int64_t>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                    case DataType::FLOAT: {
                        std::vector<float> data;
                        data.resize(size / sizeof(float));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        std::istringstream iss(operand);
                        double value;
                        iss >> value;
                        ProcessRangeQuery<float>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                    case DataType::DOUBLE: {
                        std::vector<double> data;
                        data.resize(size / sizeof(double));
                        memcpy(data.data(), attr_data_.at(field_name).data(), size);
                        std::istringstream iss(operand);
                        double value;
                        iss >> value;
                        ProcessRangeQuery<double>(data, value, com_expr[j].compare_operator, bitset);
                        break;
                    }
                }
            }
            return Status::OK();
        }
        if (general_query->leaf->vector_query != nullptr) {
            // Do search
            faiss::ConcurrentBitsetPtr list;
            list = index_->GetBlacklist();
            // Do OR
            for (uint64_t i = 0; i < vector_count_; ++i) {
                if (list->test(i) || bitset->test(i)) {
                    bitset->set(i);
                }
            }
            index_->SetBlacklist(bitset);
            auto vector_query = general_query->leaf->vector_query;
            topk = vector_query->topk;
            nq = vector_query->query_vector.float_data.size() / dim_;

            distances.resize(nq * topk);
            labels.resize(nq * topk);

            return Search(nq, vector_query->query_vector.float_data.data(), topk, vector_query->extra_params,
                          distances.data(), labels.data());
        }
    }
}

Status
ExecutionEngineImpl::Search(int64_t n, const float* data, int64_t k, const milvus::json& extra_params, float* distances,
                            int64_t* labels, bool hybrid) {
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
                    milvus::json quantizer_conf{{knowhere::meta::DEVICEID : device_id}, {"mode" : 2}};
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
    TimeRecorder rc(LogOut("[%s][%ld] ExecutionEngineImpl::Search float", "search", 0));

    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    conf[knowhere::meta::TOPK] = k;
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckSearch(conf, index_->index_type(), index_->index_mode())) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Illegal search params", "search", 0);
        throw Exception(DB_ERROR, "Illegal search params");
    }

    if (hybrid) {
        HybridLoad();
    }

    rc.RecordSection("query prepare");
    auto dataset = knowhere::GenDataset(n, index_->Dim(), data);
    auto result = index_->Query(dataset, conf);
    rc.RecordSection("query done");

    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids().size(),
                                location_.c_str());
    MapAndCopyResult(result, index_->GetUids(), n, k, distances, labels);
    rc.RecordSection("map uids " + std::to_string(n * k));

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Search(int64_t n, const uint8_t* data, int64_t k, const milvus::json& extra_params,
                            float* distances, int64_t* labels, bool hybrid) {
    TimeRecorder rc(LogOut("[%s][%ld] ExecutionEngineImpl::Search uint8", "search", 0));

    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    conf[knowhere::meta::TOPK] = k;
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckSearch(conf, index_->index_type(), index_->index_mode())) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Illegal search params", "search", 0);
        throw Exception(DB_ERROR, "Illegal search params");
    }

    if (hybrid) {
        HybridLoad();
    }

    rc.RecordSection("query prepare");
    auto dataset = knowhere::GenDataset(n, index_->Dim(), data);
    auto result = index_->Query(dataset, conf);
    rc.RecordSection("query done");

    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids().size(),
                                location_.c_str());
    MapAndCopyResult(result, index_->GetUids(), n, k, distances, labels);
    rc.RecordSection("map uids " + std::to_string(n * k));

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::GetVectorByID(const int64_t& id, float* vector, bool hybrid) {
    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, failed to search";
        return Status(DB_ERROR, "index is null");
    }

    if (hybrid) {
        HybridLoad();
    }

    // Only one id for now
    std::vector<int64_t> ids{id};
    auto dataset = knowhere::GenDatasetWithIds(1, index_->Dim(), nullptr, ids.data());
    auto result = index_->GetVectorById(dataset, knowhere::Config());
    float* res_vec = (float*)(result->Get<void*>(knowhere::meta::TENSOR));
    memcpy(vector, res_vec, sizeof(float) * 1 * index_->Dim());

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::GetVectorByID(const int64_t& id, uint8_t* vector, bool hybrid) {
    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: index is null, failed to search";
        return Status(DB_ERROR, "index is null");
    }

    LOG_ENGINE_DEBUG_ << "Get binary vector by id:  " << id;

    if (hybrid) {
        HybridLoad();
    }

    // Only one id for now
    std::vector<int64_t> ids{id};
    auto dataset = knowhere::GenDatasetWithIds(1, index_->Dim(), nullptr, ids.data());
    auto result = index_->GetVectorById(dataset, knowhere::Config());
    uint8_t* res_vec = (uint8_t*)(result->Get<void*>(knowhere::meta::TENSOR));
    memcpy(vector, res_vec, sizeof(uint8_t) * 1 * index_->Dim());

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Cache() {
    auto cpu_cache_mgr = milvus::cache::CpuCacheMgr::GetInstance();
    cache::DataObjPtr obj = std::static_pointer_cast<cache::DataObj>(index_);
    cpu_cache_mgr->InsertItem(location_, obj);
    return Status::OK();
}

// TODO(linxj): remove.
Status
ExecutionEngineImpl::Init() {
#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();
    std::vector<int64_t> gpu_ids;
    Status s = config.GetGpuResourceConfigBuildIndexResources(gpu_ids);
    if (!s.ok()) {
        gpu_num_ = -1;
        return s;
    }
    for (auto id : gpu_ids) {
        if (gpu_num_ == id) {
            return Status::OK();
        }
    }

    std::string msg = "Invalid gpu_num";
    return Status(SERVER_INVALID_ARGUMENT, msg);
#else
    return Status::OK();
#endif
}

}  // namespace engine
}  // namespace milvus
