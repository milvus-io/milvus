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

SegmentsToSearchCollector::SegmentsToSearchCollector(snapshot::ScopedSnapshotT ss,
        meta::FilesHolder& holder)
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

}  // namespace engine
}  // namespace milvus
