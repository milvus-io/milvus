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
#include "cache/FpgaCacheMgr.h"
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
#ifdef MILVUS_FPGA_VERSION
#include <faiss/index_io.h>
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/fpga/IndexFPGAIVFPQ.h"
#include "knowhere/index/vector_index/fpga/utils.h"
#endif
#ifdef MILVUS_APU_VERSION
#include <knowhere/index/vector_index/fpga/ApuInst.h>
#include <knowhere/index/vector_index/fpga/GsiHammingIndex.h>
#include <knowhere/index/vector_index/fpga/GsiTanimotoIndex.h>
#endif
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/GPUIndex.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif
#include "knowhere/index/vector_index/IndexType.h"
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

knowhere::IndexType
MappingIndexType(EngineType type) {
    switch (type) {
        case EngineType::FAISS_IDMAP:
            return knowhere::IndexEnum::INDEX_FAISS_IDMAP;
        case EngineType::FAISS_IVFFLAT:
            return knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
        case EngineType::FAISS_PQ:
            return knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
        case EngineType::FAISS_IVFSQ8:
            return knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
#ifdef MILVUS_GPU_VERSION
        case EngineType::FAISS_IVFSQ8H:
            return knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H;
#endif
        case EngineType::FAISS_BIN_IDMAP:
            return knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
        case EngineType::FAISS_BIN_IVFFLAT:
            return knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
        case EngineType::NSG_MIX:
            return knowhere::IndexEnum::INDEX_NSG;
        case EngineType::SPTAG_KDT:
            return knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT;
        case EngineType::SPTAG_BKT:
            return knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT;
        case EngineType::HNSW:
            return knowhere::IndexEnum::INDEX_HNSW;
        case EngineType::ANNOY:
            return knowhere::IndexEnum::INDEX_ANNOY;
        default:
            break;
    }

    return knowhere::IndexEnum::INVALID;
}

}  // namespace

#ifdef MILVUS_GPU_VERSION
class CachedQuantizer : public cache::DataObj {
 public:
    explicit CachedQuantizer(knowhere::FaissIVFQuantizerPtr data) : data_(std::move(data)) {
    }

    knowhere::FaissIVFQuantizerPtr
    Data() {
        return data_;
    }

    int64_t
    Size() override {
        return data_->size;
    }

 private:
    knowhere::FaissIVFQuantizerPtr data_;
};
#endif

ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension, const std::string& location, EngineType index_type,
                                         MetricType metric_type, const milvus::json& index_params, int64_t time_stamp)
    : location_(location),
      dim_(dimension),
      index_type_(index_type),
      metric_type_(metric_type),
      index_params_(index_params),
      time_stamp_(time_stamp) {
}

ExecutionEngineImpl::ExecutionEngineImpl(knowhere::VecIndexPtr index, const std::string& location,
                                         EngineType index_type, MetricType metric_type,
                                         const milvus::json& index_params, int64_t time_stamp)
    : index_(std::move(index)),
      location_(location),
      index_type_(index_type),
      metric_type_(metric_type),
      index_params_(index_params),
      time_stamp_(time_stamp) {
}

knowhere::IndexMode
ExecutionEngineImpl::GetModeFromConfig() {
#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();
    bool gpu_resource_enable = true;
    config.GetGpuResourceConfigEnable(gpu_resource_enable);
    fiu_do_on("ExecutionEngineImpl.GetModeFromConfig.gpu_res_disabled", gpu_resource_enable = false);
    if (gpu_resource_enable) {
        return knowhere::IndexMode::MODE_GPU;
    }
#endif
    return knowhere::IndexMode::MODE_CPU;
}

knowhere::VecIndexPtr
ExecutionEngineImpl::CreatetVecIndex(EngineType type, knowhere::IndexMode mode) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
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
        default:
            break;
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

    const std::string key = location_ + cache::Quantizer_Suffix;

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
        knowhere::FaissIVFQuantizerPtr quantizer = nullptr;

        for (auto& gpu : gpus) {
            auto cache = cache::GpuCacheMgr::GetInstance(gpu);
            if (auto cached_quantizer = cache->GetItem(key)) {
                device_id = gpu;
                quantizer = std::static_pointer_cast<CachedQuantizer>(cached_quantizer)->Data();
                break;
            }
        }

        if (device_id != NOT_FOUND) {
            hybrid_index->SetQuantizer(quantizer);
            return;
        }
    }

    // cache miss
    {
        int64_t max_e = INT_FAST64_MIN;
        int64_t best_device_id = 0;
        for (auto& gpu : gpus) {
            auto cache = cache::GpuCacheMgr::GetInstance(gpu);
            auto free_mem = cache->CacheCapacity() - cache->CacheUsage();
            if (free_mem > max_e) {
                max_e = free_mem;
                best_device_id = gpu;
            }
        }

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
    index_->UpdateIndexSize();
    LOG_ENGINE_DEBUG_ << "Finish serialize index file: " << location_ << " size: " << index_->Size();

    if (index_->Size() == 0) {
        std::string msg = "Failed to serialize file: " + location_ + " reason: out of disk space or memory";
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Load(bool load_blacklist, bool to_cache) {
    std::string segment_dir;
    utils::GetParentPath(location_, segment_dir);
    auto segment_reader_ptr = std::make_shared<segment::SegmentReader>(segment_dir);
    auto cpu_cache_mgr = cache::CpuCacheMgr::GetInstance();

    // step 1: Load index
    LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() get index from cache";
    index_ = std::static_pointer_cast<knowhere::VecIndex>(cpu_cache_mgr->GetItem(location_));
    if (!index_) {
        // not in the cache
        knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();

        if (utils::IsRawIndexType((int32_t)index_type_)) {
            LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() load raw file";
            if (index_type_ == EngineType::FAISS_IDMAP) {
                index_ = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IDMAP);
            } else {
                index_ = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP);
            }
            milvus::json conf{{knowhere::meta::DEVICEID, gpu_num_}, {knowhere::meta::DIM, dim_}};
            MappingMetricType(metric_type_, conf);
            auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_->index_type());
            LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
            auto mode = index_->index_mode();
            if (!adapter->CheckTrain(conf, mode)) {
                throw Exception(DB_ERROR, "Illegal index params");
            }

            segment::VectorsPtr vectors = nullptr;
            auto status = segment_reader_ptr->LoadsVectors(vectors);
            if (!status.ok()) {
                std::string msg = "Failed to load vectors from " + location_;
                LOG_ENGINE_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            auto& vectors_uids = vectors->GetMutableUids();
            std::shared_ptr<std::vector<int64_t>> vector_uids_ptr = std::make_shared<std::vector<int64_t>>();
            vector_uids_ptr->swap(vectors_uids);
            index_->SetUids(vector_uids_ptr);
            LOG_ENGINE_DEBUG_ << "set uids " << vector_uids_ptr->size() << " for index " << location_;

            auto& vectors_data = vectors->GetData();
            auto count = vector_uids_ptr->size();
            auto dataset = knowhere::GenDataset(count, this->dim_, vectors_data.data());
            if (index_type_ == EngineType::FAISS_IDMAP) {
                auto bf_index = std::static_pointer_cast<knowhere::IDMAP>(index_);
                bf_index->Train(knowhere::DatasetPtr(), conf);
                bf_index->AddWithoutIds(dataset, conf);
            } else if (index_type_ == EngineType::FAISS_BIN_IDMAP) {
                auto bin_bf_index = std::static_pointer_cast<knowhere::BinaryIDMAP>(index_);
                bin_bf_index->Train(knowhere::DatasetPtr(), conf);
                bin_bf_index->AddWithoutIds(dataset, conf);
            }

            LOG_ENGINE_DEBUG_ << "Finished loading raw data from segment " << segment_dir;
        } else {
            try {
                LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() load index file";
                segment::SegmentPtr segment_ptr;
                segment_reader_ptr->GetSegment(segment_ptr);
                auto status = segment_reader_ptr->LoadVectorIndex(location_, segment_ptr->vector_index_ptr_);
                index_ = segment_ptr->vector_index_ptr_->GetVectorIndex();
                if (index_ == nullptr) {
                    std::string msg = "Failed to load index from " + location_;
                    LOG_ENGINE_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                }

                segment::UidsPtr uids_ptr = nullptr;
                segment_reader_ptr->LoadUids(uids_ptr);
                index_->SetUids(uids_ptr);
                LOG_ENGINE_DEBUG_ << "set uids " << index_->GetUids()->size() << " for index " << location_;

                LOG_ENGINE_DEBUG_ << "Finished loading index file from segment " << segment_dir;
            } catch (std::exception& e) {
                LOG_ENGINE_ERROR_ << e.what();
                return Status(DB_ERROR, e.what());
            }
        }

        if (to_cache) {
            LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() insert index to cache";
            cpu_cache_mgr->InsertItem(location_, index_);
        }
    }

    // step 2: Load blacklist
    if (load_blacklist) {
        LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() get blacklist";
        auto blacklist_cache_key = segment_dir + cache::Blacklist_Suffix;
        blacklist_ = std::static_pointer_cast<knowhere::Blacklist>(cpu_cache_mgr->GetItem(blacklist_cache_key));

        bool cache_miss = true;
        if (blacklist_ != nullptr) {
            if (blacklist_->time_stamp_ == time_stamp_) {
                cache_miss = false;
            } else {
                LOG_ENGINE_DEBUG_ << "Mismatched time stamp  " << blacklist_->time_stamp_ << " < " << time_stamp_;
            }
        }

        if (cache_miss) {
            LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::Load() cache blacklist";
            segment::DeletedDocsPtr deleted_docs_ptr;
            auto status = segment_reader_ptr->LoadDeletedDocs(deleted_docs_ptr);
            if (!status.ok()) {
                std::string msg = "Failed to load deleted docs from " + location_;
                LOG_ENGINE_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }
            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

            blacklist_ = std::make_shared<knowhere::Blacklist>();
            blacklist_->time_stamp_ = time_stamp_;
            if (!deleted_docs.empty()) {
                auto concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(index_->Count());
                for (auto& offset : deleted_docs) {
                    concurrent_bitset_ptr->set(offset);
                }
                blacklist_->bitset_ = concurrent_bitset_ptr;
            }

            LOG_ENGINE_DEBUG_ << "Finished loading blacklist_ deleted docs size " << deleted_docs.size();

            if (to_cache) {
                cpu_cache_mgr->InsertItem(blacklist_cache_key, blacklist_);
            }
        }
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToGpu(uint64_t device_id, bool hybrid) {
#ifdef MILVUS_GPU_VERSION
    auto data_obj_ptr = cache::GpuCacheMgr::GetInstance(device_id)->GetItem(location_);
    auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
    bool already_in_cache = (index != nullptr);
    if (already_in_cache) {
        LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::CopyToGpu: already_in_cache in gpu" << device_id;
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

            bool gpu_cache_enable = false;
            STATUS_CHECK(server::Config::GetInstance().GetGpuResourceConfigCacheEnable(gpu_cache_enable));

            /* CopyCpuToGpu() is an asynchronous method.
             * It should be make sure that the CPU index is always valid.
             * Therefore, we reserve its shared pointer.
             */
            index_reserve_ = index_;
            if (gpu_cache_enable) {
                gpu_cache_mgr->Reserve(index_->Size());
            }
            index_ = knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
            if (index_ == nullptr) {
                LOG_ENGINE_DEBUG_ << "copy to GPU failed, search on CPU";
                index_ = index_reserve_;
            } else {
                if (gpu_cache_enable) {
                    gpu_cache_mgr->InsertItem(location_, std::static_pointer_cast<cache::DataObj>(index_));
                    LOG_ENGINE_DEBUG_ << "ExecutionEngineImpl::CopyToGpu: Gpu cache in device " << device_id;
                }
                LOG_ENGINE_DEBUG_ << "CPU to GPU" << device_id << " finished";
            }
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
    auto index = std::static_pointer_cast<knowhere::VecIndex>(cache::CpuCacheMgr::GetInstance()->GetItem(location_));
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

Status
ExecutionEngineImpl::CopyToFpga() {
#ifdef MILVUS_FPGA_VERSION
    auto cache_index_ =
        std::static_pointer_cast<knowhere::VecIndex>(cache::FpgaCacheMgr::GetInstance()->GetItem(location_));
    bool already_in_cache = (cache_index_ != nullptr);
    if (!already_in_cache) {
        int64_t indexsize = index_->IndexSize();
        std::shared_ptr<knowhere::IVFPQ> ivfpq = std::static_pointer_cast<knowhere::IVFPQ>(index_);
        std::shared_ptr<knowhere::FPGAIVFPQ> indexFpga = std::make_shared<knowhere::FPGAIVFPQ>(ivfpq->index_);
        indexFpga->SetIndexSize(indexsize);
        indexFpga->CopyIndexToFpga();
        indexFpga->SetUids(index_->GetUids());

        index_ = indexFpga;
        FpgaCache();
    } else {
        index_ = cache_index_;
    }
    LOG_ENGINE_DEBUG_ << "copy to fpga time ";
#endif
    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToApu(uint32_t row_count) {
#ifdef MILVUS_APU_VERSION

    auto cache_index_ =
        std::static_pointer_cast<knowhere::VecIndex>(cache::FpgaCacheMgr::GetInstance()->GetItem(location_));
    bool already_in_cache = (cache_index_ != nullptr);

    if (!already_in_cache) {
        cache::FpgaCacheMgr::GetInstance()->ClearCache();  // clear cache to support cache switch .
        std::shared_ptr<knowhere::GsiBaseIndex> gsi_index;
        // factory is needed here
        if (metric_type_ == MetricType::HAMMING)
            gsi_index = std::make_shared<knowhere::GsiHammingIndex>(dim_);
        else
            gsi_index = std::make_shared<knowhere::GsiTanimotoIndex>(dim_);

        gsi_index->SetUids(index_->GetUids());
        gsi_index->CopyIndexToFpga(row_count, location_);
        index_ = gsi_index;
        FpgaCache();
    } else {
        index_ = cache_index_;
    }
#endif
    return Status::OK();
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

    milvus::json conf = index_params_;
    conf[knowhere::meta::DIM] = Dimension();
    conf[knowhere::meta::ROWS] = Count();
    conf[knowhere::meta::DEVICEID] = gpu_num_;
    MappingMetricType(metric_type_, conf);
    LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(MappingIndexType(engine_type));
    auto mode = GetModeFromConfig();
    if (!adapter->CheckTrain(conf, mode)) {
        throw Exception(DB_ERROR, "Illegal index params");
    }
    LOG_ENGINE_DEBUG_ << "Index config: " << conf.dump();

    auto to_index = CreatetVecIndex(engine_type, mode);
    std::shared_ptr<std::vector<segment::doc_id_t>> uids;
    faiss::ConcurrentBitsetPtr blacklist;
    if (from_index) {
        auto dataset = knowhere::GenDataset(Count(), Dimension(), from_index->GetRawVectors());
        to_index->BuildAll(dataset, conf);
        uids = from_index->GetUids();
    } else if (bin_from_index) {
        auto dataset = knowhere::GenDataset(Count(), Dimension(), bin_from_index->GetRawVectors());
        to_index->BuildAll(dataset, conf);
        uids = bin_from_index->GetUids();
    }

#ifdef MILVUS_GPU_VERSION
    /* for GPU index, need copy back to CPU */
    if (to_index->index_mode() == knowhere::IndexMode::MODE_GPU) {
        auto device_index = std::dynamic_pointer_cast<knowhere::GPUIndex>(to_index);
        to_index = device_index->CopyGpuToCpu(conf);
    }
#endif

    to_index->SetUids(uids);
    LOG_ENGINE_DEBUG_ << "Set " << to_index->UidsSize() << "uids for " << location;

    LOG_ENGINE_DEBUG_ << "Finish build index: " << location;
    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, index_params_,
                                                 time_stamp_);
}

void
CopyResult(const knowhere::DatasetPtr& dataset, int64_t result_len, float* distances, int64_t* labels) {
    float* res_dist = dataset->Get<float*>(knowhere::meta::DISTANCE);
    memcpy(distances, res_dist, sizeof(float) * result_len);
    free(res_dist);

    int64_t* res_ids = dataset->Get<int64_t*>(knowhere::meta::IDS);
    memcpy(labels, res_ids, sizeof(int64_t) * result_len);
    free(res_ids);
}

Status
ExecutionEngineImpl::Search(int64_t n, const float* data, int64_t k, const milvus::json& extra_params, float* distances,
                            int64_t* labels, bool hybrid) {
    TimeRecorder rc(LogOut("[%s][%ld] ExecutionEngineImpl::Search float", "search", 0));

    if (index_ == nullptr) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ExecutionEngineImpl: index is null, failed to search", "search", 0);
        return Status(DB_ERROR, "index is null");
    }

    milvus::json conf = extra_params;
    if (conf.contains(knowhere::Metric::TYPE))
        MappingMetricType(conf[knowhere::Metric::TYPE], conf);
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
    auto result = index_->Query(dataset, conf, (blacklist_ ? blacklist_->bitset_ : nullptr));
    rc.RecordSection("query done");

    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids()->size(),
                                location_.c_str());
    CopyResult(result, n * k, distances, labels);
    rc.RecordSection("copy result " + std::to_string(n * k));

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
    if (conf.contains(knowhere::Metric::TYPE))
        MappingMetricType(conf[knowhere::Metric::TYPE], conf);
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
    auto result = index_->Query(dataset, conf, (blacklist_ ? blacklist_->bitset_ : nullptr));
    rc.RecordSection("query done");

    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld] get %ld uids from index %s", "search", 0, index_->GetUids()->size(),
                                location_.c_str());
    CopyResult(result, n * k, distances, labels);
    rc.RecordSection("copy result " + std::to_string(n * k));

    if (hybrid) {
        HybridUnset();
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::Cache() {
    auto cpu_cache_mgr = milvus::cache::CpuCacheMgr::GetInstance();
    if (index_) {
        cpu_cache_mgr->InsertItem(location_, index_);
    }

    if (blacklist_) {
        std::string segment_dir;
        utils::GetParentPath(location_, segment_dir);
        cpu_cache_mgr->InsertItem(segment_dir + cache::Blacklist_Suffix, blacklist_);
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::FpgaCache() {
#ifdef MILVUS_FPGA_VERSION
    auto fpga_cache_mgr = milvus::cache::FpgaCacheMgr::GetInstance();
    cache::DataObjPtr obj = std::static_pointer_cast<cache::DataObj>(index_);
    fpga_cache_mgr->InsertItem(location_, obj);
#endif
    return Status::OK();
}

Status
ExecutionEngineImpl::ReleaseCache() {
    LOG_ENGINE_DEBUG_ << "Release cache for file " << location_;
    server::CommonUtil::EraseFromCache(location_);
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
