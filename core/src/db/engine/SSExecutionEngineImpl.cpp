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

#include "db/engine/SSExecutionEngineImpl.h"

#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "segment/SSSegmentReader.h"
#include "segment/SSSegmentWriter.h"
#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/Status.h"
#include "utils/TimeRecorder.h"

#include "knowhere/common/Config.h"
#include "knowhere/index/structured_index/StructuredIndexSort.h"
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

#ifdef MILVUS_GPU_VERSION

#include "knowhere/index/vector_index/gpu/GPUIndex.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/gpu/Quantizer.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"

#endif

namespace milvus {
namespace engine {

namespace {
Status
GetRequiredIndexFields(const query::QueryPtr& query_ptr, std::vector<std::string>& field_names) {
    return Status::OK();
}

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

}  // namespace

SSExecutionEngineImpl::SSExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor)
    : segment_visitor_(segment_visitor) {
    segment_reader_ = std::make_shared<segment::SSSegmentReader>(dir_root, segment_visitor);
}

knowhere::VecIndexPtr
SSExecutionEngineImpl::CreatetVecIndex(EngineType type) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    knowhere::IndexMode mode = knowhere::IndexMode::MODE_CPU;
#ifdef MILVUS_GPU_VERSION
    if (config.gpu.enable()) {
        mode = knowhere::IndexMode::MODE_GPU;
    }
#endif

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
        case EngineType::FAISS_IVFSQ8NR: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR, mode);
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
        case EngineType::HNSW: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_HNSW, mode);
            break;
        }
        case EngineType::HNSW_SQ8NM: {
            index = vec_index_factory.CreateVecIndex(knowhere::IndexEnum::INDEX_HNSW_SQ8NM, mode);
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
    }
    return index;
}

Status
SSExecutionEngineImpl::Load(ExecutionEngineContext& context) {
    if (context.query_ptr_ != nullptr) {
        return LoadForSearch(context.query_ptr_);
    } else {
        return LoadForIndex();
    }
}

Status
SSExecutionEngineImpl::LoadForSearch(const query::QueryPtr& query_ptr) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    std::vector<std::string> field_names;
    GetRequiredIndexFields(query_ptr, field_names);

    return Load(field_names);
}

Status
SSExecutionEngineImpl::LoadForIndex() {
    std::vector<std::string> field_names;

    auto field_visitors = segment_visitor_->GetFieldVisitors();
    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor != nullptr && element_visitor->GetFile() == nullptr) {
            field_names.push_back(field_visitor->GetField()->GetName());
            break;
        }
    }

    return Load(field_names);
}

Status
SSExecutionEngineImpl::Load(const std::vector<std::string>& field_names) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    for (auto& name : field_names) {
        FIELD_TYPE field_type = FIELD_TYPE::NONE;
        segment_ptr->GetFieldType(name, field_type);

        bool index_exist = false;
        if (field_type == FIELD_TYPE::VECTOR || field_type == FIELD_TYPE::VECTOR_FLOAT ||
            field_type == FIELD_TYPE::VECTOR_BINARY) {
            knowhere::VecIndexPtr index_ptr;
            segment_reader_->LoadVectorIndex(name, index_ptr);
            index_exist = (index_ptr != nullptr);
        } else {
            knowhere::IndexPtr index_ptr;
            segment_reader_->LoadStructuredIndex(name, index_ptr);
            index_exist = (index_ptr != nullptr);
        }

        // index not yet build, load raw data
        if (!index_exist) {
            std::vector<uint8_t> raw;
            segment_reader_->LoadField(name, raw);
        }
    }

    return Status::OK();
}

Status
SSExecutionEngineImpl::CopyToGpu(uint64_t device_id) {
#ifdef MILVUS_GPU_VERSION
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    engine::VECTOR_INDEX_MAP new_map;
    engine::VECTOR_INDEX_MAP& indice = segment_ptr->GetVectorIndice();
    for (auto& pair : indice) {
        auto gpu_index = knowhere::cloner::CopyCpuToGpu(pair.second, device_id, knowhere::Config());
        new_map.insert(std::make_pair(pair.first, gpu_index));
    }

    indice.swap(new_map);
#endif
    return Status::OK();
}

Status
SSExecutionEngineImpl::Search(ExecutionEngineContext& context) {
    return Status::OK();
}

Status
SSExecutionEngineImpl::BuildIndex() {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    auto field_visitors = segment_visitor_->GetFieldVisitors();
    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor != nullptr && element_visitor->GetFile() == nullptr) {
            break;
        }
    }

    //    knowhere::VecIndexPtr field_raw;
    //    segment_ptr->GetVectorIndex(field_name, field_raw);
    //    if (field_raw == nullptr) {
    //        return Status(DB_ERROR, "Field raw not available");
    //    }
    //
    //    auto from_index = std::dynamic_pointer_cast<knowhere::IDMAP>(field_raw);
    //    auto bin_from_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(field_raw);
    //    if (from_index == nullptr && bin_from_index == nullptr) {
    //        LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: from_index is null, failed to build index";
    //        return Status(DB_ERROR, "Field to build index");
    //    }
    //
    //    EngineType engine_type = static_cast<EngineType>(index.engine_type_);
    //    new_index = CreatetVecIndex(engine_type);
    //    if (!new_index) {
    //        return Status(DB_ERROR, "Unsupported index type");
    //    }

    //    milvus::json conf = index.extra_params_;
    //    conf[knowhere::meta::DIM] = Dimension();
    //    conf[knowhere::meta::ROWS] = Count();
    //    conf[knowhere::meta::DEVICEID] = gpu_num_;
    //    MappingMetricType(metric_type_, conf);
    //    LOG_ENGINE_DEBUG_ << "Index params: " << conf.dump();
    //    auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(to_index->index_type());
    //    if (!adapter->CheckTrain(conf, to_index->index_mode())) {
    //        throw Exception(DB_ERROR, "Illegal index params");
    //    }
    //    LOG_ENGINE_DEBUG_ << "Index config: " << conf.dump();
    //
    //    std::vector<segment::doc_id_t> uids;
    //    faiss::ConcurrentBitsetPtr blacklist;
    //    if (from_index) {
    //        auto dataset =
    //            knowhere::GenDatasetWithIds(Count(), Dimension(), from_index->GetRawVectors(),
    //            from_index->GetRawIds());
    //        to_index->BuildAll(dataset, conf);
    //        uids = from_index->GetUids();
    //        blacklist = from_index->GetBlacklist();
    //    } else if (bin_from_index) {
    //        auto dataset = knowhere::GenDatasetWithIds(Count(), Dimension(), bin_from_index->GetRawVectors(),
    //                                                   bin_from_index->GetRawIds());
    //        to_index->BuildAll(dataset, conf);
    //        uids = bin_from_index->GetUids();
    //        blacklist = bin_from_index->GetBlacklist();
    //    }
    //
    //#ifdef MILVUS_GPU_VERSION
    //    /* for GPU index, need copy back to CPU */
    //    if (to_index->index_mode() == knowhere::IndexMode::MODE_GPU) {
    //        auto device_index = std::dynamic_pointer_cast<knowhere::GPUIndex>(to_index);
    //        to_index = device_index->CopyGpuToCpu(conf);
    //    }
    //#endif
    //
    //    to_index->SetUids(uids);
    //    LOG_ENGINE_DEBUG_ << "Set " << to_index->GetUids().size() << "uids for " << location;
    //    if (blacklist != nullptr) {
    //        to_index->SetBlacklist(blacklist);
    //        LOG_ENGINE_DEBUG_ << "Set blacklist for index " << location;
    //    }
    //
    //    LOG_ENGINE_DEBUG_ << "Finish build index: " << location;
    //    return std::make_shared<ExecutionEngineImpl>(to_index, location, engine_type, metric_type_, index_params_);

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
