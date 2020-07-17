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

#include "db/SnapshotHandlers.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshot.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/SSSegmentReader.h"
#include "utils/StringHelpFunctions.h"

#include <unordered_map>
#include <utility>

namespace milvus {
namespace engine {

LoadVectorFieldElementHandler::LoadVectorFieldElementHandler(const std::shared_ptr<server::Context>& context,
                                                             snapshot::ScopedSnapshotT ss,
                                                             const snapshot::FieldPtr& field)
    : BaseT(ss), context_(context), field_(field) {
}

Status
LoadVectorFieldElementHandler::Handle(const snapshot::FieldElementPtr& field_element) {
    if (field_->GetFtype() != engine::FieldType::VECTOR_FLOAT &&
        field_->GetFtype() != engine::FieldType::VECTOR_BINARY) {
        return Status(DB_ERROR, "Should be VECTOR field");
    }
    if (field_->GetID() != field_element->GetFieldId()) {
        return Status::OK();
    }
    // SS TODO
    return Status::OK();
}

LoadVectorFieldHandler::LoadVectorFieldHandler(const std::shared_ptr<server::Context>& context,
                                               snapshot::ScopedSnapshotT ss)
    : BaseT(ss), context_(context) {
}

Status
LoadVectorFieldHandler::Handle(const snapshot::FieldPtr& field) {
    if (field->GetFtype() != engine::FieldType::VECTOR_FLOAT && field->GetFtype() != engine::FieldType::VECTOR_BINARY) {
        return Status::OK();
    }
    if (context_ && context_->IsConnectionBroken()) {
        LOG_ENGINE_DEBUG_ << "Client connection broken, stop load collection";
        return Status(DB_ERROR, "Connection broken");
    }

    // SS TODO
    auto element_handler = std::make_shared<LoadVectorFieldElementHandler>(context_, ss_, field);
    element_handler->Iterate();

    auto status = element_handler->GetStatus();
    if (!status.ok()) {
        return status;
    }

    // SS TODO: Do Load

    return status;
}

///////////////////////////////////////////////////////////////////////////////
SegmentsToSearchCollector::SegmentsToSearchCollector(snapshot::ScopedSnapshotT ss, meta::FilesHolder& holder)
    : BaseT(ss), holder_(holder) {
}

Status
SegmentsToSearchCollector::Handle(const snapshot::SegmentCommitPtr& segment_commit) {
    // SS TODO
    meta::SegmentSchema schema;
    /* schema.id_ = segment_commit->GetSegmentId(); */
    /* schema.file_type_ = resRow["file_type"]; */
    /* schema.file_size_ = resRow["file_size"]; */
    /* schema.row_count_ = resRow["row_count"]; */
    /* schema.date_ = resRow["date"]; */
    /* schema.engine_type_ = resRow["engine_type"]; */
    /* schema.created_on_ = resRow["created_on"]; */
    /* schema.updated_time_ = resRow["updated_time"]; */

    /* schema.dimension_ = collection_schema.dimension_; */
    /* schema.index_file_size_ = collection_schema.index_file_size_; */
    /* schema.index_params_ = collection_schema.index_params_; */
    /* schema.metric_type_ = collection_schema.metric_type_; */

    /* auto status = utils::GetCollectionFilePath(options_, schema); */
    /* if (!status.ok()) { */
    /*     ret = status; */
    /*     continue; */
    /* } */

    holder_.MarkFile(schema);
}

///////////////////////////////////////////////////////////////////////////////
GetEntityByIdSegmentHandler::GetEntityByIdSegmentHandler(const std::shared_ptr<milvus::server::Context>& context,
                                                         engine::snapshot::ScopedSnapshotT ss,
                                                         const std::string& dir_root, const IDNumbers& ids,
                                                         const std::vector<std::string>& field_names)
    : BaseT(ss),
      context_(context),
      dir_root_(dir_root),
      ids_(ids),
      field_names_(field_names),
      vector_data_(),
      attr_type_(),
      attr_data_() {
    for (auto& field_name : field_names_) {
        auto field_ptr = ss_->GetField(field_name);
        auto field_type = field_ptr->GetFtype();
        attr_type_.push_back((meta::hybrid::DataType)field_type);
    }
}

Status
GetEntityByIdSegmentHandler::Handle(const snapshot::SegmentPtr& segment) {
    LOG_ENGINE_DEBUG_ << "Get entity by id in segment " << segment->GetID();

    auto segment_visitor = SegmentVisitor::Build(ss_, segment->GetID());
    if (segment_visitor == nullptr) {
        return Status(DB_ERROR, "Fail to build segment visitor with id " + std::to_string(segment->GetID()));
    }
    segment::SSSegmentReader segment_reader(dir_root_, segment_visitor);

    auto uid_field_visitor = segment_visitor->GetFieldVisitor(DEFAULT_UID_NAME);

    /* load UID's bloom filter file */
    segment::IdBloomFilterPtr id_bloom_filter_ptr;
    STATUS_CHECK(segment_reader.LoadBloomFilter(id_bloom_filter_ptr));

    /* load UID's raw data */
    std::vector<int64_t> uids;
    STATUS_CHECK(segment_reader.LoadUids(uids));

    /* load UID's deleted docs */
    segment::DeletedDocsPtr deleted_docs_ptr;
    STATUS_CHECK(segment_reader.LoadDeletedDocs(deleted_docs_ptr));

    auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

    for (auto id : ids_) {
        AttrsData& attr_ref = attr_data_[id];
        VectorsData& vector_ref = vector_data_[id];

        // fast check using bloom filter
        if (!id_bloom_filter_ptr->Check(id)) {
            continue;
        }

        // check if id really exists in uids
        auto found = std::find(uids.begin(), uids.end(), id);
        if (found == uids.end()) {
            continue;
        }

        // check if this id is deleted
        auto offset = std::distance(uids.begin(), found);
        auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
        if (deleted != deleted_docs.end()) {
            continue;
        }

        // get data from each field
        auto& field_visitors_map = segment_visitor->GetFieldVisitors();
        for (auto& iter : field_visitors_map) {
            const engine::snapshot::FieldPtr& field = iter.second->GetField();
            std::string name = field->GetName();
        }
        //        std::unordered_map<std::string, std::vector<uint8_t>> raw_attrs;
        //        for (size_t i = 0; i < field_names_.size(); i++) {
        //            auto& field_name = field_names_[i];
        //            auto field_ptr = ss_->GetField(field_name);
        //
        //            auto field_type = attr_type_[i];
        //
        //            if (field_type == meta::hybrid::DataType::VECTOR_BINARY) {
        //                auto field_params = field_ptr->GetParams();
        //                auto dim = field_params[knowhere::meta::DIM].get<int64_t>();
        //                size_t vector_size = dim / 8;
        //                std::vector<uint8_t> raw_vector;
        //                STATUS_CHECK(segment_reader.LoadVectors(offset * vector_size, vector_size, raw_vector));
        //
        //                vector_ref.vector_count_ = 1;
        //                vector_ref.binary_data_.swap(raw_vector);
        //            } else if (field_type == meta::hybrid::DataType::VECTOR_FLOAT) {
        //                auto field_params = field_ptr->GetParams();
        //                auto dim = field_params[knowhere::meta::DIM].get<int64_t>();
        //                size_t vector_size = dim * sizeof(float);
        //                std::vector<uint8_t> raw_vector;
        //                STATUS_CHECK(segment_reader.LoadVectors(offset * vector_size, vector_size, raw_vector));
        //
        //                vector_ref.vector_count_ = 1;
        //                std::vector<float> float_vector;
        //                float_vector.resize(dim);
        //                memcpy(float_vector.data(), raw_vector.data(), vector_size);
        //                vector_ref.float_data_.swap(float_vector);
        //            } else {
        //                size_t num_bytes;
        //                switch (field_type) {
        //                    case meta::hybrid::DataType::INT8:
        //                        num_bytes = 1;
        //                        break;
        //                    case meta::hybrid::DataType::INT16:
        //                        num_bytes = 2;
        //                        break;
        //                    case meta::hybrid::DataType::INT32:
        //                    case meta::hybrid::DataType::FLOAT:
        //                        num_bytes = 4;
        //                        break;
        //                    case meta::hybrid::DataType::INT64:
        //                    case meta::hybrid::DataType::DOUBLE:
        //                        num_bytes = 8;
        //                        break;
        //                    default: {
        //                        std::string msg = "Field type of " + field_name + " not supported";
        //                        return Status(DB_ERROR, msg);
        //                    }
        //                }
        //                std::vector<uint8_t> raw_attr;
        //                STATUS_CHECK(segment_reader.LoadAttrs(field_name, offset * num_bytes, num_bytes, raw_attr));
        //                raw_attrs.insert(std::make_pair(field_name, raw_attr));
        //            }
        //        }
        //
        //        attr_ref.attr_count_ = 1;
        //        attr_ref.attr_data_ = raw_attrs;
    }

    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
HybridQueryHelperSegmentHandler::HybridQueryHelperSegmentHandler(const server::ContextPtr& context,
                                                                 engine::snapshot::ScopedSnapshotT ss,
                                                                 const std::vector<std::string>& partition_patterns)
    : BaseT(ss), context_(context), partition_patterns_(partition_patterns), segments_() {
}

Status
HybridQueryHelperSegmentHandler::Handle(const snapshot::SegmentPtr& segment) {
    if (partition_patterns_.empty()) {
        segments_.push_back(segment);
    } else {
        auto p_id = segment->GetPartitionId();
        auto p_ptr = ss_->GetResource<snapshot::Partition>(p_id);
        auto& p_name = p_ptr->GetName();
        for (auto& pattern : partition_patterns_) {
            if (StringHelpFunctions::IsRegexMatch(p_name, pattern)) {
                segments_.push_back(segment);
                break;
            }
        }
    }
    return Status::OK();
}
}  // namespace engine
}  // namespace milvus
