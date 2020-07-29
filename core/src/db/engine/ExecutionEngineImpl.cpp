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

#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "config/ServerConfig.h"
#include "db/SnapshotUtils.h"
#include "db/Utils.h"
#include "db/snapshot/CompoundOperations.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
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

ExecutionEngineImpl::ExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor)
    : gpu_enable_(config.gpu.enable()) {
    segment_reader_ = std::make_shared<segment::SegmentReader>(dir_root, segment_visitor);
}

knowhere::VecIndexPtr
ExecutionEngineImpl::CreateVecIndex(const std::string& index_name) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    knowhere::IndexMode mode = knowhere::IndexMode::MODE_CPU;
#ifdef MILVUS_GPU_VERSION
    if (gpu_enable_) {
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
ExecutionEngineImpl::Load(ExecutionEngineContext& context) {
    if (context.query_ptr_ != nullptr) {
        return LoadForSearch(context.query_ptr_);
    } else {
        return Load(context.target_fields_);
    }
}

Status
ExecutionEngineImpl::LoadForSearch(const query::QueryPtr& query_ptr) {
    return Load(query_ptr->index_fields);
}

Status
ExecutionEngineImpl::CreateStructuredIndex(const milvus::engine::meta::hybrid::DataType field_type,
                                           std::vector<uint8_t>& raw_data, knowhere::IndexPtr& index_ptr) {
    switch (field_type) {
        case engine::meta::hybrid::DataType::INT32: {
            auto size = raw_data.size() / sizeof(int32_t);
            std::vector<int32_t> int32_data(size, 0);
            memcpy(int32_data.data(), raw_data.data(), size);
            auto int32_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int32_t>>(
                raw_data.size(), reinterpret_cast<const int32_t*>(raw_data.data()));
            index_ptr = std::static_pointer_cast<knowhere::Index>(int32_index_ptr);
            break;
        }
        case engine::meta::hybrid::DataType::INT64: {
            auto int64_index_ptr = std::make_shared<knowhere::StructuredIndexSort<int64_t>>(
                raw_data.size(), reinterpret_cast<const int64_t*>(raw_data.data()));
            index_ptr = std::static_pointer_cast<knowhere::Index>(int64_index_ptr);
        }
        case engine::meta::hybrid::DataType::FLOAT: {
            auto float_index_ptr = std::make_shared<knowhere::StructuredIndexSort<float>>(
                raw_data.size(), reinterpret_cast<const float*>(raw_data.data()));
            index_ptr = std::static_pointer_cast<knowhere::Index>(float_index_ptr);
        }
        case engine::meta::hybrid::DataType::DOUBLE: {
            auto double_index_ptr = std::make_shared<knowhere::StructuredIndexSort<double>>(
                raw_data.size(), reinterpret_cast<const double*>(raw_data.data()));
            index_ptr = std::static_pointer_cast<knowhere::Index>(double_index_ptr);
        }
        default: { return Status(DB_ERROR, "Field is not structured type"); }
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::Load(const TargetFields& field_names) {
    TimeRecorderAuto rc("ExecutionEngineImpl::Load");

    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    for (auto& name : field_names) {
        FIELD_TYPE field_type = FIELD_TYPE::NONE;
        segment_ptr->GetFieldType(name, field_type);

        bool index_exist = false;
        if (field_type == FIELD_TYPE::VECTOR_FLOAT || field_type == FIELD_TYPE::VECTOR_BINARY) {
            knowhere::VecIndexPtr index_ptr;
            segment_reader_->LoadVectorIndex(name, index_ptr);
            index_exist = (index_ptr != nullptr);
        } else {
            knowhere::IndexPtr index_ptr;
            segment_reader_->LoadStructuredIndex(name, index_ptr);
            index_exist = (index_ptr != nullptr);
            if (!index_exist) {
                std::vector<uint8_t> raw_data;
                segment_reader_->LoadField(name, raw_data);
                STATUS_CHECK(CreateStructuredIndex(field_type, raw_data, index_ptr));
                segment_ptr->SetStructuredIndex(name, index_ptr);
            }
        }

        // index not yet build, load raw data
        if (!index_exist) {
            std::vector<uint8_t> raw;
            segment_reader_->LoadField(name, raw);
        }

        target_fields_.insert(name);
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::CopyToGpu(uint64_t device_id) {
#ifdef MILVUS_GPU_VERSION
    TimeRecorderAuto rc("ExecutionEngineImpl::CopyToGpu");

    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    engine::VECTOR_INDEX_MAP new_map;
    engine::VECTOR_INDEX_MAP& indice = segment_ptr->GetVectorIndice();
    for (auto& pair : indice) {
        if (pair.second != nullptr) {
            auto gpu_index = knowhere::cloner::CopyCpuToGpu(pair.second, device_id, knowhere::Config());
            new_map.insert(std::make_pair(pair.first, gpu_index));
        }
    }

    indice.swap(new_map);
    gpu_num_ = device_id;
#endif
    return Status::OK();
}

Status
ExecutionEngineImpl::Search(ExecutionEngineContext& context) {
    try {
        faiss::ConcurrentBitsetPtr bitset;
        std::string vector_placeholder;
        faiss::ConcurrentBitsetPtr list;

        SegmentPtr segment_ptr;
        segment_reader_->GetSegment(segment_ptr);
        knowhere::VecIndexPtr vec_index = nullptr;
        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type;

        auto segment_visitor = segment_reader_->GetSegmentVisitor();
        auto field_visitors = segment_visitor->GetFieldVisitors();
        for (auto& pair : field_visitors) {
            auto& field_visitor = pair.second;
            auto& field = field_visitor->GetField();
            auto type = field->GetFtype();
            if (field->GetFtype() == (int)engine::meta::hybrid::DataType::VECTOR_FLOAT ||
                field->GetFtype() == (int)engine::meta::hybrid::DataType::VECTOR_BINARY) {
                segment_ptr->GetVectorIndex(field->GetName(), vec_index);
                break;
            } else if (type == (int)engine::meta::hybrid::DataType::UID) {
                continue;
            } else {
                attr_type.insert(std::make_pair(field->GetName(), (engine::meta::hybrid::DataType)type));
            }
        }

        list = vec_index->GetBlacklist();
        entity_count_ = list->capacity();
        // Parse general query
        auto status = ExecBinaryQuery(context.query_ptr_->root, bitset, attr_type, vector_placeholder);
        if (!status.ok()) {
            return status;
        }

        // Do And
        for (int64_t i = 0; i < entity_count_; i++) {
            if (list->test(i) && !bitset->test(i)) {
                list->clear(i);
            }
        }
        vec_index->SetBlacklist(list);

        auto vector_query = context.query_ptr_->vectors.at(vector_placeholder);

        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& exception) {
        return Status{DB_ERROR, "Illegal search params"};
    }

    return Status::OK();
}

Status
ExecutionEngineImpl::ExecBinaryQuery(const milvus::query::GeneralQueryPtr& general_query,
                                     faiss::ConcurrentBitsetPtr& bitset,
                                     std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                                     std::string& vector_placeholder) {
    Status status = Status::OK();
    if (general_query->leaf == nullptr) {
        faiss::ConcurrentBitsetPtr left_bitset, right_bitset;
        if (general_query->bin->left_query != nullptr) {
            status = ExecBinaryQuery(general_query->bin->left_query, left_bitset, attr_type, vector_placeholder);
            if (!status.ok()) {
                return status;
            }
        }
        if (general_query->bin->right_query != nullptr) {
            status = ExecBinaryQuery(general_query->bin->right_query, right_bitset, attr_type, vector_placeholder);
            if (!status.ok()) {
                return status;
            }
        }

        if (left_bitset == nullptr || right_bitset == nullptr) {
            bitset = left_bitset != nullptr ? left_bitset : right_bitset;
        } else {
            switch (general_query->bin->relation) {
                case milvus::query::QueryRelation::AND:
                case milvus::query::QueryRelation::R1: {
                    bitset = (*left_bitset) & right_bitset;
                    break;
                }
                case milvus::query::QueryRelation::OR:
                case milvus::query::QueryRelation::R2:
                case milvus::query::QueryRelation::R3: {
                    bitset = (*left_bitset) | right_bitset;
                    break;
                }
                case milvus::query::QueryRelation::R4: {
                    for (uint64_t i = 0; i < entity_count_; ++i) {
                        if (left_bitset->test(i) && !right_bitset->test(i)) {
                            bitset->set(i);
                        }
                    }
                    break;
                }
                default: {
                    std::string msg = "Invalid QueryRelation in RangeQuery";
                    return Status{SERVER_INVALID_ARGUMENT, msg};
                }
            }
        }
        return status;
    } else {
        bitset = std::make_shared<faiss::ConcurrentBitset>(entity_count_);
        if (general_query->leaf->term_query != nullptr) {
            // process attrs_data
            status = ProcessTermQuery(bitset, general_query->leaf->term_query, attr_type);
            if (!status.ok()) {
                return status;
            }
        }
        if (general_query->leaf->range_query != nullptr) {
            status = ProcessRangeQuery(attr_type, bitset, general_query->leaf->range_query);
            if (!status.ok()) {
                return status;
            }
        }
        if (!general_query->leaf->vector_placeholder.empty()) {
            // skip vector query
            vector_placeholder = general_query->leaf->vector_placeholder;
            bitset = nullptr;
        }
    }
    return status;
}

template <typename T>
Status
ProcessIndexedTermQuery(faiss::ConcurrentBitsetPtr& bitset, knowhere::IndexPtr& index_ptr,
                        milvus::json& term_values_json) {
    try {
        auto T_index = std::dynamic_pointer_cast<knowhere::StructuredIndexSort<T>>(index_ptr);
        if (not T_index) {
            return Status{SERVER_INVALID_ARGUMENT, "Attribute's type is wrong"};
        }
        size_t term_size = term_values_json.size();
        std::vector<T> term_value(term_size);
        size_t offset = 0;
        for (auto& data : term_values_json) {
            term_value[offset] = data.get<T>();
            ++offset;
        }

        bitset = T_index->In(term_size, term_value.data());
    } catch (std::exception& exception) {
        return Status{SERVER_INVALID_DSL_PARAMETER, exception.what()};
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::IndexedTermQuery(faiss::ConcurrentBitsetPtr& bitset, const std::string& field_name,
                                      const meta::hybrid::DataType& data_type, milvus::json& term_values_json) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);
    knowhere::IndexPtr index_ptr = nullptr;
    auto attr_index = segment_ptr->GetStructuredIndex(field_name, index_ptr);
    switch (data_type) {
        case meta::hybrid::DataType::INT8: {
            ProcessIndexedTermQuery<int8_t>(bitset, index_ptr, term_values_json);
            break;
        }
        case meta::hybrid::DataType::INT16: {
            ProcessIndexedTermQuery<int16_t>(bitset, index_ptr, term_values_json);
            break;
        }
        case meta::hybrid::DataType::INT32: {
            ProcessIndexedTermQuery<int32_t>(bitset, index_ptr, term_values_json);
            break;
        }
        case meta::hybrid::DataType::INT64: {
            ProcessIndexedTermQuery<int64_t>(bitset, index_ptr, term_values_json);
            break;
        }
        case meta::hybrid::DataType::FLOAT: {
            ProcessIndexedTermQuery<float>(bitset, index_ptr, term_values_json);
            break;
        }
        case meta::hybrid::DataType::DOUBLE: {
            ProcessIndexedTermQuery<double>(bitset, index_ptr, term_values_json);
            break;
        }
        default: { return Status{SERVER_INVALID_ARGUMENT, "Attribute:" + field_name + " type is wrong"}; }
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::ProcessTermQuery(faiss::ConcurrentBitsetPtr& bitset, const query::TermQueryPtr& term_query,
                                      std::unordered_map<std::string, meta::hybrid::DataType>& attr_type) {
    auto status = Status::OK();
    auto term_query_json = term_query->json_obj;
    auto term_it = term_query_json.begin();
    if (term_it != term_query_json.end()) {
        const std::string& field_name = term_it.key();
        if (term_it.value().is_object()) {
            milvus::json term_values_json = term_it.value()["values"];
            status = IndexedTermQuery(bitset, field_name, attr_type.at(field_name), term_values_json);
        } else {
            status = IndexedTermQuery(bitset, field_name, attr_type.at(field_name), term_it.value());
        }
    }
    return status;
}

template <typename T>
Status
ProcessIndexedRangeQuery(faiss::ConcurrentBitsetPtr& bitset, knowhere::IndexPtr& index_ptr,
                         milvus::json& range_values_json) {
    try {
        auto T_index = std::dynamic_pointer_cast<knowhere::StructuredIndexSort<T>>(index_ptr);

        bool flag = false;
        for (auto& range_value_it : range_values_json.items()) {
            const std::string& comp_op = range_value_it.key();
            T value = range_value_it.value().get<T>();
            if (not flag) {
                bitset = (*bitset) | T_index->Range(value, knowhere::s_map_operator_type.at(comp_op));
                flag = true;
            } else {
                bitset = (*bitset) & T_index->Range(value, knowhere::s_map_operator_type.at(comp_op));
            }
        }
    } catch (std::exception& exception) {
        return Status{SERVER_INVALID_DSL_PARAMETER, exception.what()};
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::IndexedRangeQuery(faiss::ConcurrentBitsetPtr& bitset, const meta::hybrid::DataType& data_type,
                                       knowhere::IndexPtr& index_ptr, milvus::json& range_values_json) {
    auto status = Status::OK();
    switch (data_type) {
        case meta::hybrid::DataType::INT8: {
            ProcessIndexedRangeQuery<int8_t>(bitset, index_ptr, range_values_json);
            break;
        }
        case meta::hybrid::DataType::INT16: {
            ProcessIndexedRangeQuery<int16_t>(bitset, index_ptr, range_values_json);
            break;
        }
        case meta::hybrid::DataType::INT32: {
            ProcessIndexedRangeQuery<int32_t>(bitset, index_ptr, range_values_json);
            break;
        }
        case meta::hybrid::DataType::INT64: {
            ProcessIndexedRangeQuery<int64_t>(bitset, index_ptr, range_values_json);
            break;
        }
        case meta::hybrid::DataType::FLOAT: {
            ProcessIndexedRangeQuery<float>(bitset, index_ptr, range_values_json);
            break;
        }
        case meta::hybrid::DataType::DOUBLE: {
            ProcessIndexedRangeQuery<double>(bitset, index_ptr, range_values_json);
            break;
        }
        default:
            break;
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::ProcessRangeQuery(const std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                                       faiss::ConcurrentBitsetPtr& bitset, const query::RangeQueryPtr& range_query) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    auto status = Status::OK();
    auto range_query_json = range_query->json_obj;
    auto range_it = range_query_json.begin();
    if (range_it != range_query_json.end()) {
        const std::string& field_name = range_it.key();
        knowhere::IndexPtr index_ptr = nullptr;
        segment_ptr->GetStructuredIndex(field_name, index_ptr);
        IndexedRangeQuery(bitset, attr_type.at(field_name), index_ptr, range_it.value());
    }
    return Status::OK();
}

Status
ExecutionEngineImpl::BuildIndex() {
    TimeRecorderAuto rc("ExecutionEngineImpl::BuildIndex");

    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    auto segment_visitor = segment_reader_->GetSegmentVisitor();
    auto& snapshot = segment_visitor->GetSnapshot();
    auto collection = snapshot->GetCollection();
    auto& segment = segment_visitor->GetSegment();

    for (auto& field_name : target_fields_) {
        auto field_visitor = segment_visitor->GetFieldVisitor(field_name);
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
        auto build_op = std::make_shared<snapshot::ChangeSegmentFileOperation>(context, snapshot);

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
        auto to_index = CreateVecIndex(index_name);
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

        auto root_path = segment_reader_->GetRootPath();
        auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(root_path, segment_visitor);
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
