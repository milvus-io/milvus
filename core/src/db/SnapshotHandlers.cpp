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
#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshot.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/SegmentReader.h"

#include <unordered_map>
#include <utility>

namespace milvus {
namespace engine {

///////////////////////////////////////////////////////////////////////////////
SegmentsToSearchCollector::SegmentsToSearchCollector(snapshot::ScopedSnapshotT ss, snapshot::IDS_TYPE& segment_ids)
    : BaseT(ss), segment_ids_(segment_ids) {
}

Status
SegmentsToSearchCollector::Handle(const snapshot::SegmentCommitPtr& segment_commit) {
    segment_ids_.push_back(segment_commit->GetSegmentId());
    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
SegmentsToIndexCollector::SegmentsToIndexCollector(snapshot::ScopedSnapshotT ss, const std::string& field_name,
                                                   snapshot::IDS_TYPE& segment_ids)
    : BaseT(ss), field_name_(field_name), segment_ids_(segment_ids) {
}

Status
SegmentsToIndexCollector::Handle(const snapshot::SegmentCommitPtr& segment_commit) {
    if (segment_commit->GetRowCount() < engine::BUILD_INDEX_THRESHOLD) {
        return Status::OK();
    }

    auto segment_visitor = engine::SegmentVisitor::Build(ss_, segment_commit->GetSegmentId());
    if (field_name_.empty()) {
        auto field_visitors = segment_visitor->GetFieldVisitors();
        for (auto& pair : field_visitors) {
            auto& field_visitor = pair.second;
            auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
            if (element_visitor != nullptr && element_visitor->GetFile() == nullptr) {
                segment_ids_.push_back(segment_commit->GetSegmentId());
                break;
            }
        }
    } else {
        auto field_visitor = segment_visitor->GetFieldVisitor(field_name_);
        auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor != nullptr && element_visitor->GetFile() == nullptr) {
            segment_ids_.push_back(segment_commit->GetSegmentId());
        }
    }

    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
GetEntityByIdSegmentHandler::GetEntityByIdSegmentHandler(const std::shared_ptr<milvus::server::Context>& context,
                                                         engine::snapshot::ScopedSnapshotT ss,
                                                         const std::string& dir_root, const IDNumbers& ids,
                                                         const std::vector<std::string>& field_names,
                                                         std::vector<bool>& valid_row)
    : BaseT(ss), context_(context), dir_root_(dir_root), ids_(ids), field_names_(field_names), valid_row_(valid_row) {
}

Status
GetEntityByIdSegmentHandler::Handle(const snapshot::SegmentPtr& segment) {
    LOG_ENGINE_DEBUG_ << "Get entity by id in segment " << segment->GetID();

    auto segment_visitor = SegmentVisitor::Build(ss_, segment->GetID());
    if (segment_visitor == nullptr) {
        return Status(DB_ERROR, "Fail to build segment visitor with id " + std::to_string(segment->GetID()));
    }
    segment::SegmentReader segment_reader(dir_root_, segment_visitor);

    std::vector<int64_t> uids;
    STATUS_CHECK(segment_reader.LoadUids(uids));

    segment::IdBloomFilterPtr id_bloom_filter_ptr;
    STATUS_CHECK(segment_reader.LoadBloomFilter(id_bloom_filter_ptr));

    segment::DeletedDocsPtr deleted_docs_ptr;
    STATUS_CHECK(segment_reader.LoadDeletedDocs(deleted_docs_ptr));

    std::vector<int64_t> offsets;
    int i = 0;
    for (auto id : ids_) {
        // fast check using bloom filter
        if (!id_bloom_filter_ptr->Check(id)) {
            i++;
            continue;
        }

        // check if id really exists in uids
        auto found = std::find(uids.begin(), uids.end(), id);
        if (found == uids.end()) {
            i++;
            continue;
        }

        // check if this id is deleted
        auto offset = std::distance(uids.begin(), found);
        if (deleted_docs_ptr) {
            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
            auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
            if (deleted != deleted_docs.end()) {
                i++;
                continue;
            }
        }
        valid_row_[i] = true;
        offsets.push_back(offset);
        i++;
    }

    STATUS_CHECK(segment_reader.LoadFieldsEntities(field_names_, offsets, data_chunk_));

    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
LoadCollectionHandler::LoadCollectionHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss,
                                             const std::string& dir_root, const std::vector<std::string>& field_names,
                                             bool force)
    : BaseT(ss), context_(context), dir_root_(dir_root), field_names_(field_names), force_(force) {
}

Status
LoadCollectionHandler::Handle(const snapshot::SegmentPtr& segment) {
    auto seg_visitor = engine::SegmentVisitor::Build(ss_, segment->GetID());
    segment::SegmentReaderPtr segment_reader = std::make_shared<segment::SegmentReader>(dir_root_, seg_visitor);

    SegmentPtr segment_ptr;
    segment_reader->GetSegment(segment_ptr);

    // if the input field_names is empty, will load all fields of this collection
    if (field_names_.empty()) {
        field_names_ = ss_->GetFieldNames();
    }

    // SegmentReader will load data into cache
    for (auto& field_name : field_names_) {
        DataType ftype = DataType::NONE;
        segment_ptr->GetFieldType(field_name, ftype);

        knowhere::IndexPtr index_ptr;
        if (IsVectorField(ftype)) {
            knowhere::VecIndexPtr vec_index_ptr;
            segment_reader->LoadVectorIndex(field_name, vec_index_ptr);
            index_ptr = vec_index_ptr;
        } else {
            segment_reader->LoadStructuredIndex(field_name, index_ptr);
        }

        // if index doesn't exist, load the raw file
        if (index_ptr == nullptr) {
            engine::BinaryDataPtr raw;
            segment_reader->LoadField(field_name, raw);
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
