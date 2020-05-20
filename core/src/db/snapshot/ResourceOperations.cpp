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

#include "ResourceOperations.h"

namespace milvus {
namespace engine {
namespace snapshot {

bool
CollectionCommitOperation::DoExecute() {
    auto prev_resource = GetPrevResource();
    if (!prev_resource) return false;
    resource_ = std::make_shared<CollectionCommit>(*prev_resource);
    resource_->ResetStatus();
    if (context_.new_partition_commit) {
        auto prev_partition_commit = prev_ss_->GetPartitionCommitByPartitionId(
                context_.new_partition_commit->GetPartitionId());
        resource_->GetMappings().erase(prev_partition_commit->GetID());
        resource_->GetMappings().insert(context_.new_partition_commit->GetID());
    } else if (context_.new_schema_commit) {
        resource_->SetSchemaId(context_.new_schema_commit->GetID());
    }
    resource_->SetID(0);
    AddStep(*BaseT::resource_);
    return true;
}

bool
PartitionCommitOperation::DoExecute() {
    auto prev_resource = GetPrevResource();
    if (prev_resource) {
        resource_ = std::make_shared<PartitionCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        auto prev_segment_commit = prev_ss_->GetSegmentCommit(
                context_.new_segment_commit->GetSegmentId());
        if (prev_segment_commit)
            resource_->GetMappings().erase(prev_segment_commit->GetID());
        if (context_.stale_segments.size() > 0) {
            for (auto& stale_segment : context_.stale_segments) {
                auto stale_segment_commit = prev_ss_->GetSegmentCommit(stale_segment->GetID());
                resource_->GetMappings().erase(stale_segment_commit->GetID());
            }
        }
    } else {
        resource_ = std::make_shared<PartitionCommit>(prev_ss_->GetCollectionId(),
                                                      context_.new_segment_commit->GetPartitionId());
    }

    resource_->GetMappings().insert(context_.new_segment_commit->GetID());
    AddStep(*resource_);
    return true;
}


bool
SegmentCommitOperation::DoExecute() {
    auto prev_resource = GetPrevResource();

    if (prev_resource) {
        resource_ = std::make_shared<SegmentCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        if (context_.stale_segment_file) {
            resource_->GetMappings().erase(context_.stale_segment_file->GetID());
        }
    } else {
        resource_ = std::make_shared<SegmentCommit>(prev_ss_->GetLatestSchemaCommitId(),
                                                    context_.new_segment_files[0]->GetPartitionId(),
                                                    context_.new_segment_files[0]->GetSegmentId());
    }
    for(auto& new_segment_file : context_.new_segment_files) {
        resource_->GetMappings().insert(new_segment_file->GetID());
    }
    AddStep(*resource_);
    return true;
}

bool
SegmentFileOperation::DoExecute() {
    auto field_element_id = prev_ss_->GetFieldElementId(context_.field_name, context_.field_element_name);
    resource_ = std::make_shared<SegmentFile>(context_.partition_id, context_.segment_id, field_element_id);
    AddStep(*resource_);
    return true;
}

} // snapshot
} // engine
} // milvus
