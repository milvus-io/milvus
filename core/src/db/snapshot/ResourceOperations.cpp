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
CollectionCommitOperation::DoExecute(StorePtr store) {
    auto prev_resource = GetPrevResource();
    auto row_cnt = 0;
    auto size = 0;
    if (!prev_resource) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Cannot find prev collection commit resource";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    resource_ = std::make_shared<CollectionCommit>(*prev_resource);
    resource_->ResetStatus();
    row_cnt = resource_->GetRowCount();
    size = resource_->GetSize();

    auto handle_new_pc = [&](PartitionCommitPtr& pc) {
        auto prev_partition_commit = GetStartedSS()->GetPartitionCommitByPartitionId(pc->GetPartitionId());
        if (prev_partition_commit) {
            resource_->GetMappings().erase(prev_partition_commit->GetID());
            row_cnt -= prev_partition_commit->GetRowCount();
            size -= prev_partition_commit->GetSize();
        }
        resource_->GetMappings().insert(pc->GetID());
        row_cnt += pc->GetRowCount();
        size += pc->GetSize();
    };

    if (context_.stale_partition_commit) {
        resource_->GetMappings().erase(context_.stale_partition_commit->GetID());
        row_cnt -= context_.stale_partition_commit->GetRowCount();
        size -= context_.stale_partition_commit->GetSize();
    } else if (context_.new_partition_commit) {
        handle_new_pc(context_.new_partition_commit);
    } else if (context_.new_partition_commits.size() > 0) {
        for (auto& pc : context_.new_partition_commits) {
            handle_new_pc(pc);
        }
    }
    if (context_.new_schema_commit) {
        resource_->SetSchemaId(context_.new_schema_commit->GetID());
    }
    resource_->SetID(0);
    resource_->SetRowCount(row_cnt);
    resource_->SetSize(size);
    AddStep(*BaseT::resource_, nullptr, false);
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
PartitionOperation::DoExecute(StorePtr store) {
    auto status = CheckStale();
    if (!status.ok())
        return status;
    resource_ = std::make_shared<Partition>(context_.name, GetStartedSS()->GetCollection()->GetID());
    AddStep(*resource_, nullptr, false);
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
    if (context_.new_segment_commit) {
        return GetStartedSS()->GetPartitionCommitByPartitionId(context_.new_segment_commit->GetPartitionId());
    } else if (context_.new_segment_commits.size() > 0) {
        return GetStartedSS()->GetPartitionCommitByPartitionId(context_.new_segment_commits[0]->GetPartitionId());
    }
    return nullptr;
}

Status
PartitionCommitOperation::DoExecute(StorePtr store) {
    auto prev_resource = GetPrevResource();
    auto row_cnt = 0;
    auto size = 0;
    if (prev_resource) {
        resource_ = std::make_shared<PartitionCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        row_cnt = resource_->GetRowCount();
        size = resource_->GetSize();
        auto erase_sc = [&](SegmentCommitPtr& sc) {
            if (!sc)
                return;
            auto prev_sc = GetStartedSS()->GetSegmentCommitBySegmentId(sc->GetSegmentId());
            if (prev_sc) {
                resource_->GetMappings().erase(prev_sc->GetID());
                row_cnt -= prev_sc->GetRowCount();
                size -= prev_sc->GetSize();
            }
        };

        erase_sc(context_.new_segment_commit);
        for (auto& sc : context_.new_segment_commits) {
            erase_sc(sc);
        }

        if (context_.stale_segments.size() > 0) {
            for (auto& stale_segment : context_.stale_segments) {
                if (stale_segment->GetPartitionId() != prev_resource->GetPartitionId()) {
                    std::stringstream emsg;
                    emsg << GetRepr() << ". All stale segments should from partition ";
                    emsg << prev_resource->GetPartitionId();
                    return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
                }
                auto stale_segment_commit = GetStartedSS()->GetSegmentCommitBySegmentId(stale_segment->GetID());
                resource_->GetMappings().erase(stale_segment_commit->GetID());
                row_cnt -= stale_segment_commit->GetRowCount();
                size -= stale_segment_commit->GetSize();
            }
        }
    } else {
        if (!context_.new_partition) {
            std::stringstream emsg;
            emsg << GetRepr() << ". New partition is required";
            return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
        }
        resource_ =
            std::make_shared<PartitionCommit>(GetStartedSS()->GetCollectionId(), context_.new_partition->GetID());
    }

    if (context_.new_segment_commit) {
        resource_->GetMappings().insert(context_.new_segment_commit->GetID());
        row_cnt += context_.new_segment_commit->GetRowCount();
        size += context_.new_segment_commit->GetSize();
    } else if (context_.new_segment_commits.size() > 0) {
        for (auto& sc : context_.new_segment_commits) {
            resource_->GetMappings().insert(sc->GetID());
            row_cnt += sc->GetRowCount();
            size += sc->GetSize();
        }
    }
    resource_->SetRowCount(row_cnt);
    resource_->SetSize(size);
    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

SegmentOperation::SegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}

Status
SegmentOperation::PreCheck() {
    if (!context_.prev_partition) {
        std::stringstream emsg;
        emsg << GetRepr() << ". prev_partition should be specified in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
SegmentOperation::DoExecute(StorePtr store) {
    if (!context_.prev_partition) {
        std::stringstream emsg;
        emsg << GetRepr() << ". prev_partition should be specified in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    auto prev_num = GetStartedSS()->GetMaxSegmentNumByPartition(context_.prev_partition->GetID());
    resource_ = std::make_shared<Segment>(context_.prev_partition->GetCollectionId(), context_.prev_partition->GetID(),
                                          prev_num + 1);
    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

SegmentCommitOperation::SegmentCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

SegmentCommit::Ptr
SegmentCommitOperation::GetPrevResource() const {
    if (context_.new_segment) {
        return nullptr;
    }
    if (context_.new_segment_files.size() > 0) {
        return GetStartedSS()->GetSegmentCommitBySegmentId(context_.new_segment_files[0]->GetSegmentId());
    } else if (context_.stale_segment_files.size() > 0) {
        return GetStartedSS()->GetSegmentCommitBySegmentId(context_.stale_segment_files[0]->GetSegmentId());
    }
    return nullptr;
}

Status
SegmentCommitOperation::DoExecute(StorePtr store) {
    auto prev_resource = GetPrevResource();
    auto size = 0;
    if (prev_resource) {
        resource_ = std::make_shared<SegmentCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        size = resource_->GetSize();
        for (auto& stale_segment_file : context_.stale_segment_files) {
            resource_->GetMappings().erase(stale_segment_file->GetID());
            size -= stale_segment_file->GetSize();
        }
    } else if (context_.new_segment && GetStartedSS()->GetResource<Partition>(context_.new_segment->GetPartitionId())) {
        resource_ =
            std::make_shared<SegmentCommit>(GetStartedSS()->GetLatestSchemaCommitId(),
                                            context_.new_segment->GetPartitionId(), context_.new_segment->GetID());
    } else {
        return Status(SS_STALE_ERROR, "Stale Error");
    }
    for (auto& new_segment_file : context_.new_segment_files) {
        resource_->GetMappings().insert(new_segment_file->GetID());
        size += new_segment_file->GetSize();
    }
    resource_->SetSize(size);
    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

Status
SegmentCommitOperation::PreCheck() {
    if (context_.stale_segment_files.size() == 0 && context_.new_segment_files.size() == 0 && !context_.new_segment) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Segment files should not be empty in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    return Status::OK();
}

FieldCommitOperation::FieldCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

FieldCommit::Ptr
FieldCommitOperation::GetPrevResource() const {
    auto get_resource = [&](FieldElementPtr fe) -> FieldCommitPtr {
        auto& field_commits = GetStartedSS()->GetResources<FieldCommit>();
        for (auto& kv : field_commits) {
            if (kv.second->GetFieldId() == fe->GetFieldId()) {
                return kv.second.Get();
            }
        }
        return nullptr;
    };

    if (context_.new_field_elements.size() > 0) {
        return get_resource(context_.new_field_elements[0]);
    } else if (context_.stale_field_elements.size() > 0) {
        return get_resource(context_.stale_field_elements[0]);
    }
    return nullptr;
}

Status
FieldCommitOperation::DoExecute(StorePtr store) {
    auto prev_resource = GetPrevResource();

    if (prev_resource) {
        resource_ = std::make_shared<FieldCommit>(*prev_resource);
        resource_->SetID(0);
        resource_->ResetStatus();
        for (auto& fe : context_.stale_field_elements) {
            resource_->GetMappings().erase(fe->GetID());
        }
    } else {
        // TODO
    }

    for (auto& fe : context_.new_field_elements) {
        resource_->GetMappings().insert(fe->GetID());
    }

    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

SchemaCommitOperation::SchemaCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

SchemaCommit::Ptr
SchemaCommitOperation::GetPrevResource() const {
    return GetStartedSS()->GetSchemaCommit();
}

Status
SchemaCommitOperation::DoExecute(StorePtr store) {
    auto prev_resource = GetPrevResource();
    if (!prev_resource) {
        return Status(SS_INVALID_CONTEX_ERROR, "Cannot get schema commit");
    }

    resource_ = std::make_shared<SchemaCommit>(*prev_resource);
    resource_->SetID(0);
    resource_->ResetStatus();
    for (auto& fc : context_.stale_field_commits) {
        resource_->GetMappings().erase(fc->GetID());
    }

    for (auto& fc : context_.new_field_commits) {
        resource_->GetMappings().insert(fc->GetID());
    }

    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

SegmentFileOperation::SegmentFileOperation(const SegmentFileContext& sc, ScopedSnapshotT prev_ss)
    : BaseT(OperationContext(), prev_ss), context_(sc) {
}

Status
SegmentFileOperation::DoExecute(StorePtr store) {
    FieldElementPtr fe;
    STATUS_CHECK(GetStartedSS()->GetFieldElement(context_.field_name, context_.field_element_name, fe));
    resource_ = std::make_shared<SegmentFile>(context_.collection_id, context_.partition_id, context_.segment_id,
                                              fe->GetID(), fe->GetFtype());
    //    auto seg_ctx_p = ResourceContextBuilder<SegmentFile>().SetResource(resource_).SetOp(oAdd).CreatePtr();
    AddStep(*resource_, nullptr, false);
    return Status::OK();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
