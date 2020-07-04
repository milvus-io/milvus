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
#include "db/meta/MetaTypes.h"
#include "db/snapshot/ResourceHelper.h"
#include "segment/SegmentReader.h"

namespace milvus {
namespace engine {

LoadVectorFieldElementHandler::LoadVectorFieldElementHandler(const std::shared_ptr<server::Context>& context,
                                                             snapshot::ScopedSnapshotT ss,
                                                             const snapshot::FieldPtr& field)
    : BaseT(ss), context_(context), field_(field) {
}

Status
LoadVectorFieldElementHandler::Handle(const snapshot::FieldElementPtr& field_element) {
    if (field_->GetFtype() != snapshot::FieldType::VECTOR) {
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
    if (field->GetFtype() != snapshot::FieldType::VECTOR) {
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
GetVectorByIdSegmentHandler::GetVectorByIdSegmentHandler(const std::shared_ptr<milvus::server::Context>& context,
                                                         engine::snapshot::ScopedSnapshotT ss, const VectorIds& ids)
    : BaseT(ss), context_(context), vector_ids_(ids), data_() {
}

Status
GetVectorByIdSegmentHandler::Handle(const snapshot::SegmentPtr& segment) {
    LOG_ENGINE_DEBUG_ << "Getting vector by id in segment " << segment->GetID();

    // sometimes not all of id_array can be found, we need to return empty vector for id not found
    // for example:
    // id_array = [1, -1, 2, -1, 3]
    // vectors should return [valid_vector, empty_vector, valid_vector, empty_vector, valid_vector]
    // the ID2RAW is to ensure returned vector sequence is consist with id_array
    std::map<int64_t, VectorsData> map_id2vector;

    // Load bloom filter
    std::string segment_dir = snapshot::GetResPath<snapshot::Segment>(segment);
    segment::SegmentReader segment_reader(segment_dir);
    segment::IdBloomFilterPtr id_bloom_filter_ptr;
    STATUS_CHECK(segment_reader.LoadBloomFilter(id_bloom_filter_ptr));

    for (auto vector_id : vector_ids_) {
        // each id must has a VectorsData
        // if vector not found for an id, its VectorsData's vector_count = 0, else 1
        VectorsData& vector_ref = map_id2vector[vector_id];

        // Check if the id is present in bloom filter.
        if (!id_bloom_filter_ptr->Check(vector_id)) {
            continue;
        }

        // Load uids and check if the id is indeed present. If yes, find its offset.
        std::vector<segment::doc_id_t> uids;
        STATUS_CHECK(segment_reader.LoadUids(uids));

        auto found = std::find(uids.begin(), uids.end(), vector_id);
        if (found != uids.end()) {
            auto offset = std::distance(uids.begin(), found);

            // Check whether the id has been deleted
            segment::DeletedDocsPtr deleted_docs_ptr;
            STATUS_CHECK(segment_reader.LoadDeletedDocs(deleted_docs_ptr));
            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

            auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
            if (deleted == deleted_docs.end()) {
                // SS TODO
                // Load raw vector
//                bool is_binary = utils::IsBinaryMetricType(file.metric_type_);
//                size_t single_vector_bytes = is_binary ? file.dimension_ / 8 : file.dimension_ * sizeof(float);
//                std::vector<uint8_t> raw_vector;
//                STATUS_CHECK(segment_reader.LoadVectors(offset * single_vector_bytes, single_vector_bytes, raw_vector));
//
//                vector_ref.vector_count_ = 1;
//                if (is_binary) {
//                    vector_ref.binary_data_.swap(raw_vector);
//                } else {
//                    std::vector<float> float_vector;
//                    float_vector.resize(file.dimension_);
//                    memcpy(float_vector.data(), raw_vector.data(), single_vector_bytes);
//                    vector_ref.float_data_.swap(float_vector);
//                }
            }
        }
    }

    for (auto id : vector_ids_) {
        VectorsData& vector_ref = map_id2vector[id];

        VectorsData data;
        data.vector_count_ = vector_ref.vector_count_;
        if (data.vector_count_ > 0) {
            data.float_data_ = vector_ref.float_data_;    // copy data since there could be duplicated id
            data.binary_data_ = vector_ref.binary_data_;  // copy data since there could be duplicated id
        }
        data_.emplace_back(data);
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
