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

#include "db/snapshot/CompoundOperations.h"
#include <memory>
#include <sstream>
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"

namespace milvus {
namespace engine {
namespace snapshot {

BuildOperation::BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}
BuildOperation::BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {
}

std::string
BuildOperation::OperationRepr() const {
    std::stringstream ss;
    ss << "<BO(SS=" << prev_ss_->GetID() << ",SEG=";
    if (context_.new_segment_files.size() == 0) {
        ss << "?";
    } else {
        ss << context_.new_segment_files[0]->GetSegmentId();
        ss << ",NSF=[";
        bool first = true;
        for (auto& f : context_.new_segment_files) {
            if (!first) {
                ss << ",";
            }
            ss << f->GetID();
            first = false;
        }
        ss << "]";
    }
    ss << ")>";
    return ss.str();
}

Status
BuildOperation::DoExecute(Store& store) {
    auto status = CheckStale(std::bind(&BuildOperation::CheckSegmentStale, this, std::placeholders::_1,
                                       context_.new_segment_files[0]->GetSegmentId()));
    if (!status.ok())
        return status;

    SegmentCommitOperation op(context_, prev_ss_);
    op(store);
    status = op.GetResource(context_.new_segment_commit);
    if (!status.ok())
        return status;
    AddStep(*context_.new_segment_commit);

    PartitionCommitOperation pc_op(context_, prev_ss_);
    pc_op(store);

    OperationContext cc_context;
    status = pc_op.GetResource(cc_context.new_partition_commit);
    if (!status.ok())
        return status;
    AddStep(*cc_context.new_partition_commit);

    PartitionCommitPtr pc;
    status = pc_op.GetResource(pc);
    if (!status.ok())
        return status;
    AddStep(*pc);

    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    cc_op(store);
    CollectionCommitPtr cc;
    status = cc_op.GetResource(cc);
    if (!status.ok())
        return status;
    AddStep(*cc);

    return status;
}

Status
BuildOperation::CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const {
    auto segment = latest_snapshot->GetResource<Segment>(segment_id);
    if (!segment) {
        return Status(SS_STALE_ERROR, "BuildOperation target segment is stale");
    }
    return Status::OK();
}

Status
BuildOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto status =
        CheckStale(std::bind(&BuildOperation::CheckSegmentStale, this, std::placeholders::_1, context.segment_id));
    if (!status.ok())
        return status;
    auto new_sf_op = std::make_shared<SegmentFileOperation>(context, prev_ss_);
    status = new_sf_op->Push();
    if (!status.ok())
        return status;
    status = new_sf_op->GetResource(created);
    if (!status.ok())
        return status;
    context_.new_segment_files.push_back(created);
    AddStep(*created);
    return status;
}

NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}
NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {
}

Status
NewSegmentOperation::DoExecute(Store& store) {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    /* auto status = PrevSnapshotRequried(); */
    /* if (!status.ok()) return status; */
    // TODO: Check Context
    SegmentCommitOperation op(context_, prev_ss_);
    auto status = op(store);
    if (!status.ok())
        return status;
    status = op.GetResource(context_.new_segment_commit);
    if (!status.ok())
        return status;
    AddStep(*context_.new_segment_commit);

    OperationContext cc_context;

    PartitionCommitOperation pc_op(context_, prev_ss_);
    status = pc_op(store);
    if (!status.ok())
        return status;
    status = pc_op.GetResource(cc_context.new_partition_commit);
    if (!status.ok())
        return status;
    AddStep(*cc_context.new_partition_commit);

    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    status = cc_op(store);
    if (!status.ok())
        return status;
    CollectionCommitPtr cc;
    status = cc_op.GetResource(cc);
    if (!status.ok())
        return status;
    AddStep(*cc);

    return status;
}

Status
NewSegmentOperation::CommitNewSegment(SegmentPtr& created) {
    auto op = std::make_shared<SegmentOperation>(context_, prev_ss_);
    auto status = op->Push();
    if (!status.ok())
        return status;
    status = op->GetResource(context_.new_segment);
    if (!status.ok())
        return status;
    created = context_.new_segment;
    AddStep(*created);
    return status;
}

Status
NewSegmentOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto c = context;
    c.segment_id = context_.new_segment->GetID();
    c.partition_id = context_.new_segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(c, prev_ss_);
    auto status = new_sf_op->Push();
    if (!status.ok())
        return status;
    status = new_sf_op->GetResource(created);
    if (!status.ok())
        return status;
    AddStep(*created);
    context_.new_segment_files.push_back(created);
    return status;
}

MergeOperation::MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}
MergeOperation::MergeOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {
}

std::string
MergeOperation::OperationRepr() const {
    std::stringstream ss;
    ss << "<MO(SS=" << prev_ss_->GetID() << ",SSEG=[";
    {
        bool first = true;
        for (auto& r : context_.stale_segments) {
            if (!first) {
                ss << ",";
            }
            ss << r->GetID();
            first = false;
        }
    }
    ss << "]";
    ss << ",NSEG=";
    if (context_.new_segment) {
        ss << context_.new_segment->GetID();
    } else {
        ss << "?";
    }
    ss << ",NSF=[";
    {
        bool first = true;
        for (auto& f : context_.new_segment_files) {
            if (!first) {
                ss << ",";
            }
            ss << f->GetID();
            first = false;
        }
    }
    ss << "]";

    ss << ")>";
    return ss.str();
}

Status
MergeOperation::CommitNewSegment(SegmentPtr& created) {
    Status status;
    if (context_.new_segment) {
        created = context_.new_segment;
        return status;
    }
    auto op = std::make_shared<SegmentOperation>(context_, prev_ss_);
    status = op->Push();
    if (!status.ok())
        return status;
    status = op->GetResource(context_.new_segment);
    if (!status.ok())
        return status;
    created = context_.new_segment;
    AddStep(*created);
    return status;
}

Status
MergeOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    // PXU TODO: Check element type and segment file mapping rules
    SegmentPtr new_segment;
    auto status = CommitNewSegment(new_segment);
    if (!status.ok())
        return status;
    auto c = context;
    c.segment_id = new_segment->GetID();
    c.partition_id = new_segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(c, prev_ss_);
    status = new_sf_op->Push();
    if (!status.ok())
        return status;
    status = new_sf_op->GetResource(created);
    if (!status.ok())
        return status;
    context_.new_segment_files.push_back(created);
    AddStep(*created);
    return status;
}

Status
MergeOperation::DoExecute(Store& store) {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    SegmentCommitOperation op(context_, prev_ss_);
    auto status = op(store);
    if (!status.ok())
        return status;

    status = op.GetResource(context_.new_segment_commit);
    if (!status.ok())
        return status;
    AddStep(*context_.new_segment_commit);

    // PXU TODO: Check stale segments

    PartitionCommitOperation pc_op(context_, prev_ss_);
    status = pc_op(store);
    if (!status.ok())
        return status;

    OperationContext cc_context;
    status = pc_op.GetResource(cc_context.new_partition_commit);
    if (!status.ok())
        return status;
    AddStep(*cc_context.new_partition_commit);

    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    status = cc_op(store);
    if (!status.ok())
        return status;
    CollectionCommitPtr cc;
    status = cc_op.GetResource(cc);
    if (!status.ok())
        return status;
    AddStep(*cc);

    return status;
}

GetSnapshotIDsOperation::GetSnapshotIDsOperation(ID_TYPE collection_id, bool reversed)
    : BaseT(OperationContext(), ScopedSnapshotT()), collection_id_(collection_id), reversed_(reversed) {
}

Status
GetSnapshotIDsOperation::DoExecute(Store& store) {
    ids_ = store.AllActiveCollectionCommitIds(collection_id_, reversed_);
    return Status::OK();
}

const IDS_TYPE&
GetSnapshotIDsOperation::GetIDs() const {
    return ids_;
}

GetCollectionIDsOperation::GetCollectionIDsOperation(bool reversed)
    : BaseT(OperationContext(), ScopedSnapshotT()), reversed_(reversed) {
}

Status
GetCollectionIDsOperation::DoExecute(Store& store) {
    ids_ = store.AllActiveCollectionIds(reversed_);
    return Status::OK();
}

const IDS_TYPE&
GetCollectionIDsOperation::GetIDs() const {
    return ids_;
}

DropPartitionOperation::DropPartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss)
    : BaseT(OperationContext(), prev_ss), context_(context) {
}

Status
DropPartitionOperation::DoExecute(Store& store) {
    Status status;
    PartitionPtr p;
    auto id = context_.id;
    if (id == 0) {
        status = prev_ss_->GetPartitionId(context_.name, id);
        context_.id = id;
    }
    if (!status.ok())
        return status;
    auto p_c = prev_ss_->GetPartitionCommitByPartitionId(id);
    if (!p_c)
        return Status(SS_NOT_FOUND_ERROR, "No partition commit found");

    OperationContext op_ctx;
    op_ctx.stale_partition_commit = p_c;
    auto op = CollectionCommitOperation(op_ctx, prev_ss_);
    status = op(store);
    if (!status.ok())
        return status;
    CollectionCommitPtr cc;
    status = op.GetResource(cc);
    if (!status.ok())
        return status;

    AddStep(*cc);
    return status;
}

CreatePartitionOperation::CreatePartitionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}
CreatePartitionOperation::CreatePartitionOperation(const OperationContext& context, ID_TYPE collection_id,
                                                   ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {
}

Status
CreatePartitionOperation::PreCheck() {
    Status status;
    if (!context_.new_partition) {
        status = Status(SS_INVALID_CONTEX_ERROR, "No partition specified before push partition");
    }
    return status;
}

Status
CreatePartitionOperation::CommitNewPartition(const PartitionContext& context, PartitionPtr& partition) {
    Status status;
    auto op = std::make_shared<PartitionOperation>(context, prev_ss_);
    status = op->Push();
    if (!status.ok())
        return status;
    status = op->GetResource(partition);
    if (!status.ok())
        return status;
    context_.new_partition = partition;
    AddStep(*partition);
    return status;
}

Status
CreatePartitionOperation::DoExecute(Store& store) {
    Status status;
    status = CheckStale();
    if (!status.ok())
        return status;

    auto collection = prev_ss_->GetCollection();
    auto partition = context_.new_partition;

    PartitionCommitPtr pc;
    OperationContext pc_context;
    pc_context.new_partition = partition;
    auto pc_op = PartitionCommitOperation(pc_context, prev_ss_);
    status = pc_op(store);
    if (!status.ok())
        return status;
    status = pc_op.GetResource(pc);
    if (!status.ok())
        return status;
    /* status = store.CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(), partition->GetID()), pc); */
    AddStep(*pc);
    OperationContext cc_context;
    cc_context.new_partition_commit = pc;
    auto cc_op = CollectionCommitOperation(cc_context, prev_ss_);
    status = cc_op(store);
    if (!status.ok())
        return status;
    CollectionCommitPtr cc;
    status = cc_op.GetResource(cc);
    if (!status.ok())
        return status;
    AddStep(*cc);

    return status;
}

CreateCollectionOperation::CreateCollectionOperation(const CreateCollectionContext& context)
    : BaseT(OperationContext(), ScopedSnapshotT()), context_(context) {
}

Status
CreateCollectionOperation::DoExecute(Store& store) {
    // TODO: Do some checks
    CollectionPtr collection;
    auto status = store.CreateCollection(Collection(context_.collection->GetName()), collection);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return status;
    }
    AddStep(*collection);
    MappingT field_commit_ids = {};
    auto field_idx = 0;
    for (auto& field_kv : context_.fields_schema) {
        field_idx++;
        auto& field_schema = field_kv.first;
        auto& field_elements = field_kv.second;
        FieldPtr field;
        status = store.CreateResource<Field>(Field(field_schema->GetName(), field_idx), field);
        AddStep(*field);
        MappingT element_ids = {};
        FieldElementPtr raw_element;
        status = store.CreateResource<FieldElement>(
            FieldElement(collection->GetID(), field->GetID(), "RAW", FieldElementType::RAW), raw_element);
        AddStep(*raw_element);
        element_ids.insert(raw_element->GetID());
        for (auto& element_schema : field_elements) {
            FieldElementPtr element;
            status =
                store.CreateResource<FieldElement>(FieldElement(collection->GetID(), field->GetID(),
                                                                element_schema->GetName(), element_schema->GetFtype()),
                                                   element);
            AddStep(*element);
            element_ids.insert(element->GetID());
        }
        FieldCommitPtr field_commit;
        status = store.CreateResource<FieldCommit>(FieldCommit(collection->GetID(), field->GetID(), element_ids),
                                                   field_commit);
        AddStep(*field_commit);
        field_commit_ids.insert(field_commit->GetID());
    }
    SchemaCommitPtr schema_commit;
    status = store.CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), field_commit_ids), schema_commit);
    AddStep(*schema_commit);
    PartitionPtr partition;
    status = store.CreateResource<Partition>(Partition("_default", collection->GetID()), partition);
    AddStep(*partition);
    PartitionCommitPtr partition_commit;
    status = store.CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(), partition->GetID()),
                                                   partition_commit);
    AddStep(*partition_commit);
    CollectionCommitPtr collection_commit;
    status = store.CreateResource<CollectionCommit>(
        CollectionCommit(collection->GetID(), schema_commit->GetID(), {partition_commit->GetID()}), collection_commit);
    AddStep(*collection_commit);
    context_.collection_commit = collection_commit;
    return Status::OK();
}

Status
CreateCollectionOperation::GetSnapshot(ScopedSnapshotT& ss) const {
    auto status = DoneRequired();
    if (!status.ok())
        return status;
    status = IDSNotEmptyRequried();
    if (!status.ok())
        return status;
    if (!context_.collection_commit)
        return Status(SS_CONSTRAINT_CHECK_ERROR, "No Snapshot is available");
    status = Snapshots::GetInstance().GetSnapshot(ss, context_.collection_commit->GetCollectionId());
    return status;
}

Status
SoftDeleteCollectionOperation::DoExecute(Store& store) {
    if (!context_.collection) {
        return Status(SS_INVALID_CONTEX_ERROR, "Invalid Context");
    }
    context_.collection->Deactivate();
    AddStep(*context_.collection);
    return Status::OK();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
