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

#include "db/merge/MergeTask.h"
#include "db/Utils.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/Snapshots.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Log.h"

#include <memory>
#include <string>

namespace milvus {
namespace engine {

MergeTask::MergeTask(const DBOptions& options, const snapshot::ScopedSnapshotT& ss, const snapshot::IDS_TYPE& segments)
    : options_(options), snapshot_(ss), segments_(segments) {
}

Status
MergeTask::Execute() {
    snapshot::OperationContext context;
    for (auto& id : segments_) {
        auto seg = snapshot_->GetResource<snapshot::Segment>(id);
        if (!seg) {
            return Status(DB_ERROR, "Snapshot segment is null");
        }

        context.stale_segments.push_back(seg);
        if (!context.prev_partition) {
            snapshot::PartitionPtr partition = snapshot_->GetResource<snapshot::Partition>(seg->GetPartitionId());
            context.prev_partition = partition;
        }
    }

    auto op = std::make_shared<snapshot::MergeOperation>(context, snapshot_);
    snapshot::SegmentPtr new_seg;
    auto status = op->CommitNewSegment(new_seg);
    if (!status.ok()) {
        return status;
    }

    // create segment raw files (placeholder)
    auto names = snapshot_->GetFieldNames();
    for (auto& name : names) {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = new_seg->GetCollectionId();
        sf_context.partition_id = new_seg->GetPartitionId();
        sf_context.segment_id = new_seg->GetID();
        sf_context.field_name = name;
        sf_context.field_element_name = engine::DEFAULT_RAW_DATA_NAME;

        snapshot::SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        if (!status.ok()) {
            std::string err_msg = "MergeTask create segment failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    // create deleted_doc and bloom_filter files (placeholder)
    {
        snapshot::SegmentFileContext sf_context;
        sf_context.collection_id = new_seg->GetCollectionId();
        sf_context.partition_id = new_seg->GetPartitionId();
        sf_context.segment_id = new_seg->GetID();
        sf_context.field_name = engine::DEFAULT_UID_NAME;
        sf_context.field_element_name = engine::DEFAULT_DELETED_DOCS_NAME;

        snapshot::SegmentFilePtr delete_doc_file, bloom_filter_file;
        status = op->CommitNewSegmentFile(sf_context, delete_doc_file);
        if (!status.ok()) {
            std::string err_msg = "MergeTask create bloom filter segment file failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }

        sf_context.field_element_name = engine::DEFAULT_BLOOM_FILTER_NAME;
        status = op->CommitNewSegmentFile(sf_context, bloom_filter_file);
        if (!status.ok()) {
            std::string err_msg = "MergeTask create deleted-doc segment file failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    auto ctx = op->GetContext();
    auto visitor = SegmentVisitor::Build(snapshot_, ctx.new_segment, ctx.new_segment_files);

    // create segment writer
    segment::SegmentWriterPtr segment_writer = std::make_shared<segment::SegmentWriter>(options_.meta_.path_, visitor);

    // merge
    for (auto& id : segments_) {
        auto seg = snapshot_->GetResource<snapshot::Segment>(id);

        auto read_visitor = SegmentVisitor::Build(snapshot_, id);
        segment::SegmentReaderPtr segment_reader =
            std::make_shared<segment::SegmentReader>(options_.meta_.path_, read_visitor);
        status = segment_writer->Merge(segment_reader);
        if (!status.ok()) {
            std::string err_msg = "MergeTask merge failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    status = segment_writer->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize segment: " << new_seg->GetID();
        return status;
    }

    status = op->Push();

    return status;
}

}  // namespace engine
}  // namespace milvus
