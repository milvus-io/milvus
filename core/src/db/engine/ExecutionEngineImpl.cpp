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
    ENGINE_LOG_DEBUG << "Index params: " << conf.dump();
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
            ENGINE_LOG_ERROR << "Unsupported index type " << (int)type;
            return nullptr;
        }
    }
    if (index == nullptr) {
        std::string err_msg = "Invalid index type " + std::to_string((int)type) + " mod " + std::to_string((int)mode);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(DB_ERROR, err_msg);
    }
    return index;
}

void
ExecutionEngineImpl::HybridLoad() const {
#ifdef MILVUS_GPU_VERSION
    auto hybrid_index = std::dynamic_pointer_cast<knowhere::IVFSQHybrid>(index_);
    if (hybrid_index == nullptr) {
        ENGINE_LOG_WARNING << "HybridLoad only support with IVFSQHybrid";
        return;
    }

    const std::string key = location_ + ".quantizer";

    server::Config& config = server::Config::GetInstance();
    std::vector<int64_t> gpus;
    Status s = config.GetGpuResourceConfigSearchResources(gpus);
    if (!s.ok()) {
        ENGINE_LOG_ERROR << s.message();
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
        ENGINE_LOG_DEBUG << "Quantizer params: " << quantizer_conf.dump();
        if (quantizer == nullptr) {
            ENGINE_LOG_ERROR << "quantizer is nullptr";
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
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return count 0";
        return 0;
    }
    return index_->Count();
}

size_t
ExecutionEngineImpl::Dimension() const {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return dimension " << dim_;
        return dim_;
    }
    return index_->Dim();
}

size_t
ExecutionEngineImpl::Size() const {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, return size 0";
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
    segment_writer_ptr->WriteVectorIndex(location_);

    // here we reset index size by file size,
    // since some index type(such as SQ8) data size become smaller after serialized
    index_->SetIndexSize(server::CommonUtil::GetFileSize(location_));
    ENGINE_LOG_DEBUG << "Finish serialize index file: " << location_ << " size: " << index_->Size();

    if (index_->Size() == 0) {
        std::string msg = "Failed to serialize file: " + location_ + " reason: out of disk space or memory";
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Load(bool to_cache) {
    // TODO(zhiru): refactor

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
            ENGINE_LOG_DEBUG << "Index params: " << conf.dump();
            if (!adapter->CheckTrain(conf, index_->index_mode())) {
                throw Exception(DB_ERROR, "Illegal index params");
            }

            auto status = segment_reader_ptr->Load();
            if (!status.ok()) {
                std::string msg = "Failed to load segment from " + location_;
                ENGINE_LOG_ERROR << msg;
                return Status(DB_ERROR, msg);
            }

            segment::SegmentPtr segment_ptr;
            segment_reader_ptr->GetSegment(segment_ptr);
            auto& vectors = segment_ptr->vectors_ptr_;
            auto& deleted_docs = segment_ptr->deleted_docs_ptr_->GetDeletedDocs();

            auto vectors_uids = vectors->GetUids();
            index_->SetUids(vectors_uids);
            ENGINE_LOG_DEBUG << "set uids " << index_->GetUids().size() << " for index " << location_;

            auto vectors_data = vectors->GetData();

            faiss::ConcurrentBitsetPtr concurrent_bitset_ptr =
                std::make_shared<faiss::ConcurrentBitset>(vectors->GetCount());
            for (auto& offset : deleted_docs) {
                if (!concurrent_bitset_ptr->test(offset)) {
                    concurrent_bitset_ptr->set(offset);
                }
            }

            auto dataset = knowhere::GenDataset(vectors->GetCount(), this->dim_, vectors_data.data());
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

            ENGINE_LOG_DEBUG << "Finished loading raw data from segment " << segment_dir;
        } else {
            try {
                segment::SegmentPtr segment_ptr;
                segment_reader_ptr->GetSegment(segment_ptr);
                auto status = segment_reader_ptr->LoadVectorIndex(location_, segment_ptr->vector_index_ptr_);
                index_ = segment_ptr->vector_index_ptr_->GetVectorIndex();

                if (index_ == nullptr) {
                    std::string msg = "Failed to load index from " + location_;
                    ENGINE_LOG_ERROR << msg;
                    return Status(DB_ERROR, msg);
                } else {
                    segment::DeletedDocsPtr deleted_docs_ptr;
                    auto status = segment_reader_ptr->LoadDeletedDocs(deleted_docs_ptr);
                    if (!status.ok()) {
                        std::string msg = "Failed to load deleted docs from " + location_;
                        ENGINE_LOG_ERROR << msg;
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
                    ENGINE_LOG_DEBUG << "set uids " << index_->GetUids().size() << " for index " << location_;

                    ENGINE_LOG_DEBUG << "Finished loading index file from segment " << segment_dir;
                }
            } catch (std::exception& e) {
                ENGINE_LOG_ERROR << e.what();
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
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to gpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            /* Index data is copied to GPU first, then added into GPU cache.
             * Add lock here to avoid multiple INDEX are copied to one GPU card at same time.
             * And reserve space to avoid GPU out of memory issue.
             */
            ENGINE_LOG_DEBUG << "CPU to GPU" << device_id << " start";
            auto gpu_cache_mgr = cache::GpuCacheMgr::GetInstance(device_id);
            // gpu_cache_mgr->Reserve(index_->Size());
            index_ = knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
            // gpu_cache_mgr->InsertItem(location_, std::static_pointer_cast<cache::DataObj>(index_));
            ENGINE_LOG_DEBUG << "CPU to GPU" << device_id << " finished";
        } catch (std::exception& e) {
            ENGINE_LOG_ERROR << e.what();
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
            ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to copy to cpu";
            return Status(DB_ERROR, "index is null");
        }

        try {
            index_ = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
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
#else
    ENGINE_LOG_ERROR << "Calling ExecutionEngineImpl::CopyToCpu when using CPU version";
    return Status(DB_ERROR, "Calling ExecutionEngineImpl::CopyToCpu when using CPU version");
#endif
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string& location, EngineType engine_type) {
    ENGINE_LOG_DEBUG << "Build index file: " << location << " from: " << location_;

    auto from_index = std::dynamic_pointer_cast<knowhere::IDMAP>(index_);
    auto bin_from_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_);
    if (from_index == nullptr && bin_from_index == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: from_index is null, failed to build index";
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
    ENGINE_LOG_DEBUG << "Index params: " << conf.dump();
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(to_index->index_type());
    if (!adapter->CheckTrain(conf, to_index->index_mode())) {
        throw Exception(DB_ERROR, "Illegal index params");
    }
    ENGINE_LOG_DEBUG << "Index config: " << conf.dump();

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
    ENGINE_LOG_DEBUG << "Set " << to_index->GetUids().size() << "uids for " << location;
    if (blacklist != nullptr) {
        to_index->SetBlacklist(blacklist);
        ENGINE_LOG_DEBUG << "Set blacklist for index " << location;
    }

    ENGINE_LOG_DEBUG << "Finish build index: " << location;
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
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    conf[knowhere::meta::TOPK] = k;
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckSearch(conf, index_->index_type(), index_->index_mode())) {
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] Illegal search params", "search", 0);
        throw Exception(DB_ERROR, "Illegal search params");
    }

    if (hybrid) {
        HybridLoad();
    }

    rc.RecordSection("query prepare");
    auto dataset = knowhere::GenDataset(n, index_->Dim(), data);
    auto result = index_->Query(dataset, conf);
    rc.RecordSection("query done");

    ENGINE_LOG_DEBUG << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids().size(),
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
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    conf[knowhere::meta::TOPK] = k;
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckSearch(conf, index_->index_type(), index_->index_mode())) {
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] Illegal search params", "search", 0);
        throw Exception(DB_ERROR, "Illegal search params");
    }

    if (hybrid) {
        HybridLoad();
    }

    rc.RecordSection("query prepare");
    auto dataset = knowhere::GenDataset(n, index_->Dim(), data);
    auto result = index_->Query(dataset, conf);
    rc.RecordSection("query done");

    ENGINE_LOG_DEBUG << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids().size(),
                               location_.c_str());
    MapAndCopyResult(result, index_->GetUids(), n, k, distances, labels);
    rc.RecordSection("map uids " + std::to_string(n * k));

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Search(int64_t n, const std::vector<int64_t>& ids, int64_t k, const milvus::json& extra_params,
                            float* distances, int64_t* labels, bool hybrid) {
    TimeRecorder rc(LogOut("[%s][%ld] ExecutionEngineImpl::Search vector of ids", "search", 0));

    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    conf[knowhere::meta::TOPK] = k;
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
    if (!adapter->CheckSearch(conf, index_->index_type(), index_->index_mode())) {
        ENGINE_LOG_ERROR << LogOut("[%s][%ld] Illegal search params", "search", 0);
        throw Exception(DB_ERROR, "Illegal search params");
    }

    if (hybrid) {
        HybridLoad();
    }

    rc.RecordSection("search prepare");

    // std::string segment_dir;
    // utils::GetParentPath(location_, segment_dir);
    // segment::SegmentReader segment_reader(segment_dir);
    //    segment::IdBloomFilterPtr id_bloom_filter_ptr;
    //    segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

    // Check if the id is present. If so, find its offset
    const std::vector<segment::doc_id_t>& uids = index_->GetUids();

    std::vector<int64_t> offsets;
    /*
    std::vector<segment::doc_id_t> uids;
    auto status = segment_reader.LoadUids(uids);
    if (!status.ok()) {
        return status;
    }
     */

    // There is only one id in ids
    for (auto& id : ids) {
        //        if (id_bloom_filter_ptr->Check(id)) {
        //            if (uids.empty()) {
        //                segment_reader.LoadUids(uids);
        //            }
        //            auto found = std::find(uids.begin(), uids.end(), id);
        //            if (found != uids.end()) {
        //                auto offset = std::distance(uids.begin(), found);
        //                offsets.emplace_back(offset);
        //            }
        //        }
        auto found = std::find(uids.begin(), uids.end(), id);
        if (found != uids.end()) {
            auto offset = std::distance(uids.begin(), found);
            offsets.emplace_back(offset);
        }
    }

    rc.RecordSection("get offset");

    if (!offsets.empty()) {
        auto dataset = knowhere::GenDatasetWithIds(offsets.size(), index_->Dim(), nullptr, offsets.data());
        auto result = index_->QueryById(dataset, conf);
        rc.RecordSection("query by id done");

        ENGINE_LOG_DEBUG << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids().size(),
                                   location_.c_str());
        MapAndCopyResult(result, uids, offsets.size(), k, distances, labels);
        rc.RecordSection("map uids " + std::to_string(offsets.size() * k));
    }

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::GetVectorByID(const int64_t& id, float* vector, bool hybrid) {
    if (index_ == nullptr) {
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to search";
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
        ENGINE_LOG_ERROR << "ExecutionEngineImpl: index is null, failed to search";
        return Status(DB_ERROR, "index is null");
    }

    ENGINE_LOG_DEBUG << "Get binary vector by id:  " << id;

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
