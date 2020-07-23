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
#include "db/SnapshotUtils.h"
#include "db/Utils.h"
#include "db/snapshot/CompoundOperations.h"
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
    : root_path_(dir_root), segment_visitor_(segment_visitor) {
    segment_reader_ = std::make_shared<segment::SSSegmentReader>(dir_root, segment_visitor);
}

knowhere::VecIndexPtr
SSExecutionEngineImpl::CreatetVecIndex(const std::string& index_name) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    knowhere::IndexMode mode = knowhere::IndexMode::MODE_CPU;
#ifdef MILVUS_GPU_VERSION
    if (config.gpu.enable()) {
        mode = knowhere::IndexMode::MODE_GPU;
    }
#endif

    knowhere::VecIndexPtr index = vec_index_factory.CreateVecIndex(index_name, mode);
    if (index == nullptr) {
        std::string err_msg = "Invalid index type: " + index_name + " mode: " + std::to_string((int)mode);
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
    TimeRecorderAuto rc("SSExecutionEngineImpl::Load");
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
    TimeRecorderAuto rc("SSExecutionEngineImpl::CopyToGpu");

    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    engine::VECTOR_INDEX_MAP new_map;
    engine::VECTOR_INDEX_MAP& indice = segment_ptr->GetVectorIndice();
    for (auto& pair : indice) {
        auto gpu_index = knowhere::cloner::CopyCpuToGpu(pair.second, device_id, knowhere::Config());
        new_map.insert(std::make_pair(pair.first, gpu_index));
    }

    indice.swap(new_map);
    gpu_num_ = device_id;
#endif
    return Status::OK();
}

Status
SSExecutionEngineImpl::Search(ExecutionEngineContext& context) {
    return Status::OK();
}

Status
SSExecutionEngineImpl::BuildIndex() {
    TimeRecorderAuto rc("SSExecutionEngineImpl::BuildIndex");

    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    auto& snapshot = segment_visitor_->GetSnapshot();
    auto collection = snapshot->GetCollection();
    auto& segment = segment_visitor_->GetSegment();

    auto field_visitors = segment_visitor_->GetFieldVisitors();
    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor == nullptr) {
            continue;  // no index specified
        }
        if (element_visitor->GetFile() != nullptr) {
            continue;  // index already build?
        }

        auto& field = field_visitor->GetField();
        auto& element = element_visitor->GetElement();

        if (!IsVectorField(field)) {
            continue;  // not vector field?
        }

        // add segment files
        snapshot::OperationContext context;
        context.prev_partition = snapshot->GetResource<snapshot::Partition>(segment->GetPartitionId());
        auto build_op = std::make_shared<snapshot::AddSegmentFileOperation>(context, snapshot);

        auto add_segment_file = [&](const std::string& element_name, snapshot::SegmentFilePtr& seg_file) -> Status {
            snapshot::SegmentFileContext sf_context;
            sf_context.field_name = field->GetName();
            sf_context.field_element_name = element->GetName();
            sf_context.collection_id = segment->GetCollectionId();
            sf_context.partition_id = segment->GetPartitionId();

            return build_op->CommitNewSegmentFile(sf_context, seg_file);
        };

        // create snapshot index file
        snapshot::SegmentFilePtr index_file;
        add_segment_file(element->GetName(), index_file);

        // create snapshot compress file
        std::string index_name = element->GetName();
        if (index_name == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR ||
            index_name == knowhere::IndexEnum::INDEX_HNSW_SQ8NM) {
            auto compress_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_COMPRESS_SQ8);
            if (compress_visitor) {
                std::string compress_name = compress_visitor->GetElement()->GetName();
                snapshot::SegmentFilePtr compress_file;
                add_segment_file(compress_name, compress_file);
            }
        }

        knowhere::VecIndexPtr index_raw;
        segment_ptr->GetVectorIndex(field->GetName(), index_raw);

        auto from_index = std::dynamic_pointer_cast<knowhere::IDMAP>(index_raw);
        auto bin_from_index = std::dynamic_pointer_cast<knowhere::BinaryIDMAP>(index_raw);
        if (from_index == nullptr && bin_from_index == nullptr) {
            LOG_ENGINE_ERROR_ << "ExecutionEngineImpl: from_index is not IDMAP, skip build index for ";
            return Status(DB_ERROR, "Field to build index");
        }

        // build index by knowhere
        auto to_index = CreatetVecIndex(index_name);
        if (!to_index) {
            throw Exception(DB_ERROR, "Unsupported index type");
        }

        auto element_json = element->GetParams();
        auto metric_type = element_json[engine::PARAM_INDEX_METRIC_TYPE];
        auto index_params = element_json[engine::PARAM_INDEX_EXTRA_PARAMS];

        auto field_json = field->GetParams();
        auto dimension = field_json[milvus::knowhere::meta::DIM];

        auto segment_commit = snapshot->GetSegmentCommitBySegmentId(segment->GetID());
        auto row_count = segment_commit->GetRowCount();

        milvus::json conf = index_params;
        conf[knowhere::meta::DIM] = dimension;
        conf[knowhere::meta::ROWS] = row_count;
        conf[knowhere::meta::DEVICEID] = gpu_num_;
        MappingMetricType(metric_type, conf);
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
                knowhere::GenDatasetWithIds(row_count, dimension, from_index->GetRawVectors(), from_index->GetRawIds());
            to_index->BuildAll(dataset, conf);
            uids = from_index->GetUids();
            blacklist = from_index->GetBlacklist();
        } else if (bin_from_index) {
            auto dataset = knowhere::GenDatasetWithIds(row_count, dimension, bin_from_index->GetRawVectors(),
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
        if (blacklist != nullptr) {
            to_index->SetBlacklist(blacklist);
        }

        rc.RecordSection("build index");

        auto segment_writer_ptr = std::make_shared<segment::SSSegmentWriter>(root_path_, segment_visitor_);
        segment_writer_ptr->SetVectorIndex(field->GetName(), to_index);
        segment_writer_ptr->WriteVectorIndex(field->GetName());

        rc.RecordSection("serialize index");

        // finish transaction
        build_op->Push();
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
