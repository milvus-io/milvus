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

#include "db/snapshot/ResourceOperations.h"
#include <memory>

namespace milvus {
namespace engine {
namespace snapshot {

Status
CollectionCommitOperation::DoExecute(Store& store) {
    auto prev_resource = GetPrevResource();
    if (!prev_resource)
        return Status(SS_INVALID_CONTEX_ERROR, "Invalid CollectionCommitOperation Context");
    resource_ = std::make_shared<CollectionCommit>(*prev_resource);
    resource_->ResetStatus();
    if (context_.stale_partition_commit) {
        resource_->GetMappings().erase(context_.stale_partition_commit->GetID());
    } else if (context_.new_partition_commit) {
        auto prev_partition_commit =
            GetStartedSS()->GetPartitionCommitByPartitionId(context_.new_partition_commit->GetPartitionId());
        if (prev_partition_commit)
            resource_->GetMappings().erase(prev_partition_commit->GetID());
        resource_->GetMappings().insert(context_.new_partition_commit->GetID());
    } else if (context_.new_schema_commit) {
        resource_->SetSchemaId(context_.new_schema_commit->GetID());
    }
    resource_->SetID(0);
    AddStep(*BaseT::resource_, false);
    return Status::OK();
}

PartitionOperation::PartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss)
    : BaseT(OperationContext(), prev_ss), context_(context) {
}

Status
PartitionOperation::PreCheck() {
    return Status::OK();
}

Status
PartitionOperation::DoExecute(Store& store) {
    auto status = CheckStale();
    if (!status.ok())
        return status;
    resource_ = std::make_shared<Partition>(context_.name, GetStartedSS()->GetCollection()->GetID());
    AddStep(*resource_, false);
    return status;
}

PartitionCommitOperation::PartitionCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
PartitionCommitOperation::PreCheck() {
    return Status::OK();
}

PartitionCommitPtr
PartitionCommitOperation::GetPrevResource() const {
    auto& segment_commit = context_.new_segment_commit;
    if (!segment_commit)
        return nullptr;
    return GetStartedSS()->GetPartitionCommitByPartitionId(segment_commit->GetPartitionId());
}

Status
PartitionCommitOperation::DoExecute(Store& store) {
    auto prev_resource = GetPrevResource();
    if (prev_resource) {
        resource_ = std::make_shared<PartitionCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        auto prev_segment_commit =
            GetStartedSS()->GetSegmentCommitBySegmentId(context_.new_segment_commit->GetSegmentId());
        if (prev_segment_commit)
            resource_->GetMappings().erase(prev_segment_commit->GetID());
        if (context_.stale_segments.size() > 0) {
            for (auto& stale_segment : context_.stale_segments) {
                if (stale_segment->GetPartitionId() != prev_resource->GetPartitionId()) {
                    return Status(SS_INVALID_CONTEX_ERROR, "All stale segments should from specified partition");
                }
                auto stale_segment_commit = GetStartedSS()->GetSegmentCommitBySegmentId(stale_segment->GetID());
                resource_->GetMappings().erase(stale_segment_commit->GetID());
            }
        }
    } else {
        if (!context_.new_partition) {
            return Status(SS_INVALID_CONTEX_ERROR, "Partition is required");
        }
        resource_ =
            std::make_shared<PartitionCommit>(GetStartedSS()->GetCollectionId(), context_.new_partition->GetID());
    }

    if (context_.new_segment_commit) {
        resource_->GetMappings().insert(context_.new_segment_commit->GetID());
    }
    AddStep(*resource_, false);
    return Status::OK();
}

SegmentCommitOperation::SegmentCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

SegmentCommit::Ptr
SegmentCommitOperation::GetPrevResource() const {
    if (context_.new_segment_files.size() > 0) {
        return GetStartedSS()->GetSegmentCommitBySegmentId(context_.new_segment_files[0]->GetSegmentId());
    }
    return nullptr;
}

SegmentOperation::SegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}

Status
SegmentOperation::PreCheck() {
    if (!context_.prev_partition)
        return Status(SS_INVALID_CONTEX_ERROR, "Invalid SegmentOperation Context");
    return Status::OK();
}

Status
SegmentOperation::DoExecute(Store& store) {
    if (!context_.prev_partition) {
        return Status(SS_INVALID_CONTEX_ERROR, "Invalid SegmentOperation Context");
    }
    auto prev_num = GetStartedSS()->GetMaxSegmentNumByPartition(context_.prev_partition->GetID());
    resource_ = std::make_shared<Segment>(context_.prev_partition->GetID(), prev_num + 1);
    AddStep(*resource_, false);
    return Status::OK();
}

Status
SegmentCommitOperation::DoExecute(Store& store) {
    auto prev_resource = GetPrevResource();

    if (prev_resource) {
        resource_ = std::make_shared<SegmentCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        if (context_.stale_segment_file) {
            resource_->GetMappings().erase(context_.stale_segment_file->GetID());
        }
    } else {
        resource_ = std::make_shared<SegmentCommit>(GetStartedSS()->GetLatestSchemaCommitId(),
                                                    context_.new_segment_files[0]->GetPartitionId(),
                                                    context_.new_segment_files[0]->GetSegmentId());
    }
    for (auto& new_segment_file : context_.new_segment_files) {
        resource_->GetMappings().insert(new_segment_file->GetID());
    }
    AddStep(*resource_, false);
    return Status::OK();
}

Status
SegmentCommitOperation::PreCheck() {
    if (context_.new_segment_files.size() == 0) {
        return Status(SS_INVALID_CONTEX_ERROR, "Invalid SegmentCommitOperation Context");
    }
    return Status::OK();
}

SegmentFileOperation::SegmentFileOperation(const SegmentFileContext& sc, ScopedSnapshotT prev_ss)
    : BaseT(OperationContext(), prev_ss), context_(sc) {
}

Status
SegmentFileOperation::DoExecute(Store& store) {
    auto field_element_id = GetStartedSS()->GetFieldElementId(context_.field_name, context_.field_element_name);
    resource_ = std::make_shared<SegmentFile>(context_.partition_id, context_.segment_id, field_element_id);
    AddStep(*resource_, false);
    return Status::OK();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
