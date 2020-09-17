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
MemSegment::Serialize() {
    int64_t size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(size);

    // delete in mem
    STATUS_CHECK(ApplyDeleteToMem());

    // create new segment and serialize
    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id_);
    if (!status.ok()) {
        std::string err_msg = "Failed to get latest snapshot: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    // get max op id
    idx_t max_op_id = 0;
    for (auto& action : actions_) {
        if (action.op_id_ > max_op_id) {
            max_op_id = action.op_id_;
        }
    }

    std::shared_ptr<snapshot::NewSegmentOperation> new_seg_operation;
    segment::SegmentWriterPtr segment_writer;
    status = CreateNewSegment(ss, new_seg_operation, segment_writer, max_op_id);
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

    STATUS_CHECK(new_seg_operation->CommitRowCount(segment_writer->RowCount()));
    STATUS_CHECK(new_seg_operation->Push());
    LOG_ENGINE_DEBUG_ << "New segment " << seg_id << " of collection " << collection_id_ << " serialized";

    // notify wal the max operation id is done
    WalManager::GetInstance().OperationDone(ss->GetName(), max_op_id);

    return Status::OK();
}

Status
MemSegment::CreateNewSegment(snapshot::ScopedSnapshotT& ss, std::shared_ptr<snapshot::NewSegmentOperation>& operation,
                             segment::SegmentWriterPtr& writer, idx_t max_op_id) {
    // create segment
    snapshot::SegmentPtr segment;
    snapshot::OperationContext context;
    //    context.lsn = max_op_id;
    context.prev_partition = ss->GetResource<snapshot::Partition>(partition_id_);
    operation = std::make_shared<snapshot::NewSegmentOperation>(context, ss);
    auto status = operation->CommitNewSegment(segment);
    if (!status.ok()) {
        std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    // create segment raw files (placeholder)
    auto names = ss->GetFieldNames();
    for (auto& name : names) {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = segment->GetID();
        sf_context.field_name = name;
        sf_context.field_element_name = engine::ELEMENT_RAW_DATA;

        snapshot::SegmentFilePtr seg_file;
        status = operation->CommitNewSegmentFile(sf_context, seg_file);
        if (!status.ok()) {
            std::string err_msg = "MemSegment::CreateSegment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    // create deleted_doc and bloom_filter files (placeholder)
    {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = collection_id_;
        sf_context.partition_id = partition_id_;
        sf_context.segment_id = segment->GetID();
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
    }

    auto ctx = operation->GetContext();
    auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);

    // create segment writer
    writer = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, visitor);

    return Status::OK();
}

Status
MemSegment::ApplyDeleteToMem() {
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

            // delete entities from chunks
            Segment temp_set;
            STATUS_CHECK(temp_set.SetFields(collection_id_));
            STATUS_CHECK(temp_set.AddChunk(chunk));
            temp_set.DeleteEntity(offsets);
            chunk->count_ = temp_set.GetRowCount();
        }
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
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
