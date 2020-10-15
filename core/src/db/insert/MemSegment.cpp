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

#include "db/insert/MemSegment.h"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "config/ServerConfig.h"
#include "db/Types.h"
#include "db/Utils.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/Snapshots.h"
#include "db/wal/WalManager.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "metrics/Metrics.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MemSegment::MemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options)
    : collection_id_(collection_id), partition_id_(partition_id), options_(options) {
}

Status
MemSegment::Add(const DataChunkPtr& chunk, idx_t op_id) {
    if (chunk == nullptr) {
        return Status::OK();
    }

    MemAction action;
    action.op_id_ = op_id;
    action.insert_data_ = chunk;
    actions_.emplace_back(action);

    current_mem_ += utils::GetSizeOfChunk(chunk);
    total_row_count_ += chunk->count_;

    return Status::OK();
}

Status
MemSegment::Delete(const std::vector<idx_t>& ids, idx_t op_id) {
    if (ids.empty()) {
        return Status::OK();
    }

    // previous action is delete? combine delete action
    if (!actions_.empty()) {
        MemAction& pre_action = *actions_.rbegin();
        if (!pre_action.delete_ids_.empty()) {
            for (auto& id : ids) {
                pre_action.delete_ids_.insert(id);
            }
            return Status::OK();
        }
    }

    // create new action
    MemAction action;
    action.op_id_ = op_id;
    for (auto& id : ids) {
        action.delete_ids_.insert(id);
    }
    actions_.emplace_back(action);

    return Status::OK();
}

Status
MemSegment::Serialize(snapshot::ScopedSnapshotT& ss, std::shared_ptr<snapshot::MultiSegmentsOperation>& operation) {
    int64_t mem_size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(mem_size);

    // empty segment, nothing to do
    if (actions_.empty()) {
        return Status::OK();
    }

    // get max op id
    for (auto& action : actions_) {
        if (action.op_id_ > max_op_id_) {
            max_op_id_ = action.op_id_;
        }
    }

    // delete in mem
    auto status = ApplyDeleteToMem(ss);
    if (!status.ok()) {
        if (status.code() == DB_EMPTY_COLLECTION) {
            // segment is empty, do nothing
            return Status::OK();
        }
    }

    // create new segment and serialize
    segment::SegmentWriterPtr segment_writer;
    status = CreateNewSegment(ss, operation, segment_writer);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to create new segment";
        return status;
    }

    status = PutChunksToWriter(segment_writer);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to copy data to segment writer";
        return status;
    }

    // delete action could delete all entities of the segment
    // no need to serialize empty segment
    if (segment_writer->RowCount() == 0) {
        return Status::OK();
    }

    int64_t seg_id = 0;
    segment_writer->GetSegmentID(seg_id);
    status = segment_writer->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize segment: " << seg_id;
        return status;
    }

    operation->CommitRowCount(seg_id, segment_writer->RowCount());

    LOG_ENGINE_DEBUG_ << "New segment " << seg_id << " of collection " << collection_id_ << " committed";

    return Status::OK();
}

Status
MemSegment::CreateNewSegment(snapshot::ScopedSnapshotT& ss,
                             std::shared_ptr<snapshot::MultiSegmentsOperation>& operation,
                             segment::SegmentWriterPtr& writer) {
    // create new segment
    snapshot::SegmentPtr new_segment;
    snapshot::OperationContext new_seg_ctx;
    new_seg_ctx.prev_partition = ss->GetResource<snapshot::Partition>(partition_id_);
    auto status = operation->CommitNewSegment(new_seg_ctx, new_segment);
    if (!status.ok()) {
        std::string err_msg = "MemSegment::CreateNewSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    snapshot::SegmentFile::VecT new_segment_files;
    // create segment raw files (placeholder)
    auto names = ss->GetFieldNames();
    for (auto& name : names) {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = new_segment->GetID();
        sf_context.field_name = name;
        sf_context.field_element_name = engine::ELEMENT_RAW_DATA;

        snapshot::SegmentFilePtr seg_file;
        status = operation->CommitNewSegmentFile(sf_context, seg_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
        new_segment_files.emplace_back(seg_file);
    }

    // create deleted_doc and bloom_filter files (placeholder)
    {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = new_segment->GetID();
        sf_context.field_name = engine::FIELD_UID;
        sf_context.field_element_name = engine::ELEMENT_DELETED_DOCS;

        snapshot::SegmentFilePtr delete_doc_file, bloom_filter_file;
        status = operation->CommitNewSegmentFile(sf_context, delete_doc_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }

        sf_context.field_element_name = engine::ELEMENT_BLOOM_FILTER;
        status = operation->CommitNewSegmentFile(sf_context, bloom_filter_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }

        new_segment_files.emplace_back(delete_doc_file);
        new_segment_files.emplace_back(bloom_filter_file);
    }

    auto visitor = SegmentVisitor::Build(ss, new_segment, new_segment_files);

    // create segment writer
    writer = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, visitor);

    return Status::OK();
}

Status
MemSegment::ApplyDeleteToMem(snapshot::ScopedSnapshotT& ss) {
    auto outer_iter = actions_.begin();
    for (; outer_iter != actions_.end(); ++outer_iter) {
        MemAction& action = (*outer_iter);
        if (action.delete_ids_.empty()) {
            continue;
        }

        auto inner_iter = actions_.begin();
        for (; inner_iter != outer_iter; ++inner_iter) {
            MemAction& insert_action = (*inner_iter);
            if (insert_action.insert_data_ == nullptr) {
                continue;
            }

            DataChunkPtr& chunk = insert_action.insert_data_;
            // load chunk uids
            auto iter = chunk->fixed_fields_.find(FIELD_UID);
            if (iter == chunk->fixed_fields_.end()) {
                continue;  // no uid field?
            }

            BinaryDataPtr& uid_data = iter->second;
            if (uid_data == nullptr) {
                continue;  // no uid data?
            }
            if (uid_data->data_.size() / sizeof(idx_t) != chunk->count_) {
                continue;  // invalid uid data?
            }
            auto uid = reinterpret_cast<idx_t*>(uid_data->data_.data());

            // calculte delete offsets
            std::vector<offset_t> offsets;
            for (int64_t i = 0; i < chunk->count_; ++i) {
                if (action.delete_ids_.find(uid[i]) != action.delete_ids_.end()) {
                    offsets.push_back(i);
                }
            }

            // construct a new engine::Segment, delete entities from chunks
            // since the temp_set is empty, it shared BinaryData with the chunk
            // the DeleteEntity() will delete elements from the chunk
            Segment temp_set;
            auto& fields = ss->GetResources<snapshot::Field>();
            for (auto& kv : fields) {
                const snapshot::FieldPtr& field = kv.second.Get();
                STATUS_CHECK(temp_set.AddField(field));
            }
            STATUS_CHECK(temp_set.AddChunk(chunk));
            temp_set.DeleteEntity(offsets);
            chunk->count_ = temp_set.GetRowCount();
        }
    }

    int64_t row_count = 0;
    for (auto& action : actions_) {
        if (action.insert_data_ != nullptr) {
            row_count += action.insert_data_->count_;
        }
    }

    if (row_count == 0) {
        return Status(DB_EMPTY_COLLECTION, "All entities deleted");
    }

    return Status::OK();
}

Status
MemSegment::PutChunksToWriter(const segment::SegmentWriterPtr& writer) {
    if (writer == nullptr) {
        return Status(DB_ERROR, "Segment writer is null pointer");
    }

    for (auto& action : actions_) {
        DataChunkPtr chunk = action.insert_data_;
        if (chunk == nullptr || chunk->count_ == 0) {
            continue;
        }

        // copy data to writer
        writer->AddChunk(chunk);

        // free memory immediately since the data has been added into writer
        action.insert_data_ = nullptr;
    }
    actions_.clear();

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
