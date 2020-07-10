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

#include "db/merge/SSMergeTask.h"
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

SSMergeTask::SSMergeTask(const DBOptions& options, const snapshot::ScopedSnapshotT& ss,
                         const snapshot::IDS_TYPE& segments)
    : options_(options), snapshot_(ss), segments_(segments) {
}

Status
SSMergeTask::Execute() {
    if (segments_.size() <= 1) {
        return Status::OK();
    }

    snapshot::OperationContext context;
    for (auto& id : segments_) {
        auto seg = snapshot_->GetResource<snapshot::Segment>(id);
        if (!seg) {
            return Status(DB_ERROR, "snapshot segment is null");
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

    // TODO: merge each field, each field create a new SegmentFile
    snapshot::SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "ivfsq8";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;
    sf_context.segment_id = new_seg->GetID();
    snapshot::SegmentFilePtr seg_file;
    status = op->CommitNewSegmentFile(sf_context, seg_file);

    status = op->Push();

    return status;
}

}  // namespace engine
}  // namespace milvus
