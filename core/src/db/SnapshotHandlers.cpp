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
                                                   snapshot::IDS_TYPE& segment_ids, int64_t build_index_threshold)
    : BaseT(ss), field_name_(field_name), segment_ids_(segment_ids), build_index_threshold_(build_index_threshold) {
}

Status
SegmentsToIndexCollector::Handle(const snapshot::SegmentCommitPtr& segment_commit) {
    if (segment_commit->GetRowCount() < build_index_threshold_) {
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
SegmentsToMergeCollector::SegmentsToMergeCollector(snapshot::ScopedSnapshotT ss, snapshot::IDS_TYPE& segment_ids,
                                                   int64_t row_count_threshold)
    : BaseT(ss), segment_ids_(segment_ids), row_count_threshold_(row_count_threshold) {
}

Status
SegmentsToMergeCollector::Handle(const snapshot::SegmentCommitPtr& segment_commit) {
    if (segment_commit->GetRowCount() >= row_count_threshold_) {
        return Status::OK();
    }

    // if any field has build index, don't merge this segment
    auto segment_visitor = engine::SegmentVisitor::Build(ss_, segment_commit->GetSegmentId());
    auto field_visitors = segment_visitor->GetFieldVisitors();
    bool has_index = false;
    for (auto& kv : field_visitors) {
        auto element_visitor = kv.second->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor != nullptr && element_visitor->GetFile() != nullptr) {
            has_index = true;
            break;
        }
    }

    if (!has_index) {
        segment_ids_.push_back(segment_commit->GetSegmentId());
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
    ids_left_ = ids_;
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

    std::vector<idx_t> ids_in_this_segment;
    std::vector<int64_t> offsets;
    for (auto it = ids_left_.begin(); it != ids_left_.end();) {
        idx_t id = *it;
        // fast check using bloom filter
        if (!id_bloom_filter_ptr->Check(id)) {
            ++it;
            continue;
        }

        // check if id really exists in uids
        auto found = std::find(uids.begin(), uids.end(), id);
        if (found == uids.end()) {
            ++it;
            continue;
        }

        // check if this id is deleted
        auto offset = std::distance(uids.begin(), found);
        if (deleted_docs_ptr) {
            auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
            auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
            if (deleted != deleted_docs.end()) {
                ++it;
                continue;
            }
        }

        ids_in_this_segment.push_back(id);
        offsets.push_back(offset);
        ids_left_.erase(it);
    }

    if (offsets.empty()) {
        return Status::OK();
    }

    engine::DataChunkPtr data_chunk;
    STATUS_CHECK(segment_reader.LoadFieldsEntities(field_names_, offsets, data_chunk));

    // record id in which chunk, and its position within the chunk
    for (int64_t i = 0; i < ids_in_this_segment.size(); ++i) {
        auto pair = std::make_pair(data_chunk, i);
        result_map_.insert(std::make_pair(ids_in_this_segment[i], pair));
    }

    return Status::OK();
}

Status
GetEntityByIdSegmentHandler::PostIterate() {
    // construct result
    // Note: makesure the result sequence is according to input ids
    // for example:
    // No.1, No.3, No.5 id are in segment_1
    // No.2, No.4, No.6 id are in segment_2
    // After iteration, we got two DataChunk,
    // the chunk_1 for No.1, No.3, No.5 entities, the chunk_2 for No.2, No.4, No.6 entities
    // now we combine chunk_1 and chunk_2 into one DataChunk, and the entities sequence is 1,2,3,4,5,6
    Segment temp_segment;
    auto& fields = ss_->GetResources<snapshot::Field>();
    for (auto& kv : fields) {
        const snapshot::FieldPtr& field = kv.second.Get();
        STATUS_CHECK(temp_segment.AddField(field));
    }

    temp_segment.Reserve(field_names_, result_map_.size());

    valid_row_.clear();
    valid_row_.reserve(ids_.size());
    for (auto id : ids_) {
        auto iter = result_map_.find(id);
        if (iter == result_map_.end()) {
            valid_row_.push_back(false);
        } else {
            valid_row_.push_back(true);

            auto pair = iter->second;
            temp_segment.AppendChunk(pair.first, pair.second, pair.second);
        }
    }

    data_chunk_ = std::make_shared<engine::DataChunk>();
    data_chunk_->count_ = temp_segment.GetRowCount();
    data_chunk_->fixed_fields_.swap(temp_segment.GetFixedFields());
    data_chunk_->variable_fields_.swap(temp_segment.GetVariableFields());

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
