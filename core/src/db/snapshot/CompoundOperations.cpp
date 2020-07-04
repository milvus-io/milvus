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
#include <map>
#include <memory>
#include <sstream>
#include <vector>
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

BuildOperation::BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}

Status
BuildOperation::DoExecute(Store& store) {
    STATUS_CHECK(CheckStale(std::bind(&BuildOperation::CheckSegmentStale, this, std::placeholders::_1,
                                      context_.new_segment_files[0]->GetSegmentId())));

    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn);

    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    OperationContext cc_context;
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn);

    context_.new_partition_commit = cc_context.new_partition_commit;
    STATUS_CHECK(pc_op.GetResource(context_.new_partition_commit));
    AddStepWithLsn(*context_.new_partition_commit, context_.lsn);

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn);

    return Status::OK();
}

Status
BuildOperation::CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const {
    auto segment = latest_snapshot->GetResource<Segment>(segment_id);
    if (!segment) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Target segment " << segment_id << " is stale";
        return Status(SS_STALE_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
BuildOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    STATUS_CHECK(
        CheckStale(std::bind(&BuildOperation::CheckSegmentStale, this, std::placeholders::_1, context.segment_id)));

    auto segment = GetStartedSS()->GetResource<Segment>(context.segment_id);
    if (!segment) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Invalid segment " << context.segment_id << " in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }

    auto ctx = context;
    ctx.partition_id = segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    context_.new_segment_files.push_back(created);
    AddStepWithLsn(*created, context_.lsn);

    return Status::OK();
}

DropAllIndexOperation::DropAllIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
DropAllIndexOperation::PreCheck() {
    if (context_.stale_field_element == nullptr) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Stale field element is requried";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }

    if (!GetStartedSS()->GetResource<FieldElement>(context_.stale_field_element->GetID())) {
        std::stringstream emsg;
        emsg << GetRepr() << ".  Specified field element " << context_.stale_field_element->GetName();
        emsg << " is stale";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    // TODO: Check type
    return Status::OK();
}

Status
DropAllIndexOperation::DoExecute(Store& store) {
    auto& segment_files = GetAdjustedSS()->GetResources<SegmentFile>();

    OperationContext cc_context;
    {
        auto context = context_;
        context.stale_field_elements.push_back(context.stale_field_element);

        FieldCommitOperation fc_op(context, GetAdjustedSS());
        STATUS_CHECK(fc_op(store));
        FieldCommitPtr new_field_commit;
        STATUS_CHECK(fc_op.GetResource(new_field_commit));
        AddStepWithLsn(*new_field_commit, context.lsn);
        context.new_field_commits.push_back(new_field_commit);
        for (auto& kv : GetAdjustedSS()->GetResources<FieldCommit>()) {
            if (kv.second->GetFieldId() == new_field_commit->GetFieldId()) {
                context.stale_field_commits.push_back(kv.second.Get());
            }
        }

        SchemaCommitOperation sc_op(context, GetAdjustedSS());

        STATUS_CHECK(sc_op(store));
        STATUS_CHECK(sc_op.GetResource(cc_context.new_schema_commit));
        AddStepWithLsn(*cc_context.new_schema_commit, context.lsn);
    }

    std::map<ID_TYPE, std::vector<SegmentCommitPtr>> p_sc_map;
    for (auto& kv : segment_files) {
        if (kv.second->GetFieldElementId() != context_.stale_field_element->GetID()) {
            continue;
        }

        auto context = context_;
        context.stale_segment_file = kv.second.Get();
        SegmentCommitOperation sc_op(context, GetAdjustedSS());
        STATUS_CHECK(sc_op(store));
        STATUS_CHECK(sc_op.GetResource(context.new_segment_commit));
        AddStepWithLsn(*context.new_segment_commit, context.lsn);
        p_sc_map[context.new_segment_commit->GetPartitionId()].push_back(context.new_segment_commit);
    }

    for (auto& kv : p_sc_map) {
        auto& partition_id = kv.first;
        auto context = context_;
        context.new_segment_commits = kv.second;
        PartitionCommitOperation pc_op(context, GetAdjustedSS());
        STATUS_CHECK(pc_op(store));
        STATUS_CHECK(pc_op.GetResource(context.new_partition_commit));
        AddStepWithLsn(*context.new_partition_commit, context.lsn);
        cc_context.new_partition_commits.push_back(context.new_partition_commit);
    }

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn);

    return Status::OK();
}

DropIndexOperation::DropIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
DropIndexOperation::PreCheck() {
    if (context_.stale_segment_file == nullptr) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Stale segment is requried";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    // TODO: Check segment file type

    return Status::OK();
}

Status
DropIndexOperation::DoExecute(Store& store) {
    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn);

    OperationContext cc_context;
    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn);
    context_.new_partition_commit = cc_context.new_partition_commit;

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn);

    return Status::OK();
}

NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
NewSegmentOperation::DoExecute(Store& store) {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    /* auto status = PrevSnapshotRequried(); */
    /* if (!status.ok()) return status; */
    // TODO: Check Context
    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn);
    /* std::cout << GetRepr() << " POST_SC_MAP=("; */
    /* for (auto id : context_.new_segment_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    OperationContext cc_context;
    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn);
    context_.new_partition_commit = cc_context.new_partition_commit;
    /* std::cout << GetRepr() << " POST_PC_MAP=("; */
    /* for (auto id : cc_context.new_partition_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn);

    return Status::OK();
}

Status
NewSegmentOperation::CommitNewSegment(SegmentPtr& created) {
    auto op = std::make_shared<SegmentOperation>(context_, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(context_.new_segment));
    created = context_.new_segment;
    AddStepWithLsn(*created, context_.lsn);
    return Status::OK();
}

Status
NewSegmentOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto ctx = context;
    ctx.segment_id = context_.new_segment->GetID();
    ctx.partition_id = context_.new_segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    AddStepWithLsn(*created, context_.lsn);
    context_.new_segment_files.push_back(created);
    return Status::OK();
}

MergeOperation::MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
}

Status
MergeOperation::OnSnapshotStale() {
    for (auto& stale_seg : context_.stale_segments) {
        auto expect_sc = GetStartedSS()->GetSegmentCommitBySegmentId(stale_seg->GetID());
        auto latest_sc = GetAdjustedSS()->GetSegmentCommitBySegmentId(stale_seg->GetID());
        if (!latest_sc || (latest_sc->GetID() != expect_sc->GetID())) {
            std::stringstream emsg;
            emsg << GetRepr() << ". Stale segment " << stale_seg->GetID() << " in context";
            return Status(SS_STALE_ERROR, emsg.str());
        }
    }
    return Status::OK();
}

Status
MergeOperation::CommitNewSegment(SegmentPtr& created) {
    if (context_.new_segment) {
        created = context_.new_segment;
        return Status::OK();
    }
    auto op = std::make_shared<SegmentOperation>(context_, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(context_.new_segment));
    created = context_.new_segment;
    AddStepWithLsn(*created, context_.lsn);
    return Status::OK();
}

Status
MergeOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    // PXU TODO: Check element type and segment file mapping rules
    SegmentPtr new_segment;
    STATUS_CHECK(CommitNewSegment(new_segment));
    auto ctx = context;
    ctx.segment_id = new_segment->GetID();
    ctx.partition_id = new_segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    context_.new_segment_files.push_back(created);
    AddStepWithLsn(*created, context_.lsn);
    return Status::OK();
}

Status
MergeOperation::DoExecute(Store& store) {
    // PXU TODO:
    // 1. Check all required field elements have related segment files
    // 2. Check Stale and others
    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn);
    /* std::cout << GetRepr() << " POST_SC_MAP=("; */
    /* for (auto id : context_.new_segment_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    OperationContext cc_context;
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn);
    context_.new_partition_commit = cc_context.new_partition_commit;

    /* std::cout << GetRepr() << " POST_PC_MAP=("; */
    /* for (auto id : cc_context.new_partition_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn);

    return Status::OK();
}

GetSnapshotIDsOperation::GetSnapshotIDsOperation(ID_TYPE collection_id, bool reversed)
    : BaseT(OperationContext(), ScopedSnapshotT(), OperationsType::O_Compound),
      collection_id_(collection_id),
      reversed_(reversed) {
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
    : BaseT(OperationContext(), prev_ss), c_context_(context) {
}

std::string
DropPartitionOperation::GetRepr() const {
    std::stringstream ss;
    ss << "<" << GetName() << "(";
    if (GetAdjustedSS()) {
        ss << "SS=" << GetAdjustedSS()->GetID();
    }
    ss << "," << c_context_.ToString();
    ss << "," << context_.ToString();
    ss << ",LSN=" << GetContextLsn();
    ss << ")>";
    return ss.str();
}

Status
DropPartitionOperation::DoExecute(Store& store) {
    PartitionPtr p;
    auto id = c_context_.id;
    if (id == 0) {
        STATUS_CHECK(GetAdjustedSS()->GetPartitionId(c_context_.name, id));
        c_context_.id = id;
    }
    auto p_c = GetAdjustedSS()->GetPartitionCommitByPartitionId(id);
    if (!p_c) {
        std::stringstream emsg;
        emsg << GetRepr() << ". PartitionCommit " << id << " not found";
        return Status(SS_NOT_FOUND_ERROR, emsg.str());
    }
    context_.stale_partition_commit = p_c;

    OperationContext op_ctx;
    op_ctx.stale_partition_commit = p_c;
    auto cc_op = CollectionCommitOperation(op_ctx, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    AddStepWithLsn(*context_.new_collection_commit, c_context_.lsn);
    return Status::OK();
}

CreatePartitionOperation::CreatePartitionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
CreatePartitionOperation::PreCheck() {
    STATUS_CHECK(BaseT::PreCheck());
    if (!context_.new_partition) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Partition is missing";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
CreatePartitionOperation::CommitNewPartition(const PartitionContext& context, PartitionPtr& partition) {
    auto op = std::make_shared<PartitionOperation>(context, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(partition));
    context_.new_partition = partition;
    AddStepWithLsn(*partition, context_.lsn);
    return Status::OK();
}

Status
CreatePartitionOperation::DoExecute(Store& store) {
    STATUS_CHECK(CheckStale());

    auto collection = GetAdjustedSS()->GetCollection();
    auto partition = context_.new_partition;

    if (context_.new_partition) {
        if (GetAdjustedSS()->GetPartition(context_.new_partition->GetName())) {
            std::stringstream emsg;
            emsg << GetRepr() << ". Duplicate Partition \"" << context_.new_partition->GetName() << "\"";
            return Status(SS_DUPLICATED_ERROR, emsg.str());
        }
    }

    PartitionCommitPtr pc;
    OperationContext pc_context;
    pc_context.new_partition = partition;
    auto pc_op = PartitionCommitOperation(pc_context, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(pc));
    AddStepWithLsn(*pc, context_.lsn);

    OperationContext cc_context;
    cc_context.new_partition_commit = pc;
    context_.new_partition_commit = pc;
    auto cc_op = CollectionCommitOperation(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    CollectionCommitPtr cc;
    STATUS_CHECK(cc_op.GetResource(cc));
    AddStepWithLsn(*cc, context_.lsn);
    context_.new_collection_commit = cc;

    return Status::OK();
}

CreateCollectionOperation::CreateCollectionOperation(const CreateCollectionContext& context)
    : BaseT(OperationContext(), ScopedSnapshotT()), c_context_(context) {
}

Status
CreateCollectionOperation::PreCheck() {
    // TODO
    return Status::OK();
}

std::string
CreateCollectionOperation::GetRepr() const {
    std::stringstream ss;
    ss << "<" << GetName() << "(";
    if (GetAdjustedSS()) {
        ss << "SS=" << GetAdjustedSS()->GetID();
    }
    ss << c_context_.ToString();
    ss << "," << context_.ToString();
    ss << ",LSN=" << GetContextLsn();
    ss << ")>";
    return ss.str();
}

Status
CreateCollectionOperation::DoExecute(Store& store) {
    // TODO: Do some checks
    CollectionPtr collection;
    auto status = store.CreateCollection(Collection(c_context_.collection->GetName()), collection);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return status;
    }
    AddStepWithLsn(*collection, c_context_.lsn);
    context_.new_collection = collection;
    MappingT field_commit_ids = {};
    auto field_idx = 0;
    for (auto& field_kv : c_context_.fields_schema) {
        field_idx++;
        auto& field_schema = field_kv.first;
        auto& field_elements = field_kv.second;
        FieldPtr field;
        status =
            store.CreateResource<Field>(Field(field_schema->GetName(), field_idx, field_schema->GetFtype()), field);
        AddStepWithLsn(*field, c_context_.lsn);
        MappingT element_ids = {};
        FieldElementPtr raw_element;
        status = store.CreateResource<FieldElement>(
            FieldElement(collection->GetID(), field->GetID(), "RAW", FieldElementType::RAW), raw_element);
        AddStepWithLsn(*raw_element, c_context_.lsn);
        element_ids.insert(raw_element->GetID());
        for (auto& element_schema : field_elements) {
            FieldElementPtr element;
            status =
                store.CreateResource<FieldElement>(FieldElement(collection->GetID(), field->GetID(),
                                                                element_schema->GetName(), element_schema->GetFtype()),
                                                   element);
            AddStepWithLsn(*element, c_context_.lsn);
            element_ids.insert(element->GetID());
        }
        FieldCommitPtr field_commit;
        status = store.CreateResource<FieldCommit>(FieldCommit(collection->GetID(), field->GetID(), element_ids),
                                                   field_commit);
        AddStepWithLsn(*field_commit, c_context_.lsn);
        field_commit_ids.insert(field_commit->GetID());
    }
    SchemaCommitPtr schema_commit;
    status = store.CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), field_commit_ids), schema_commit);
    AddStepWithLsn(*schema_commit, c_context_.lsn);
    PartitionPtr partition;
    status = store.CreateResource<Partition>(Partition("_default", collection->GetID()), partition);
    AddStepWithLsn(*partition, c_context_.lsn);
    context_.new_partition = partition;
    PartitionCommitPtr partition_commit;
    status = store.CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(), partition->GetID()),
                                                   partition_commit);
    AddStepWithLsn(*partition_commit, c_context_.lsn);
    context_.new_partition_commit = partition_commit;
    CollectionCommitPtr collection_commit;
    status = store.CreateResource<CollectionCommit>(
        CollectionCommit(collection->GetID(), schema_commit->GetID(), {partition_commit->GetID()}), collection_commit);
    AddStepWithLsn(*collection_commit, c_context_.lsn);
    context_.new_collection_commit = collection_commit;
    c_context_.collection_commit = collection_commit;
    context_.new_collection_commit = collection_commit;
    return Status::OK();
}

Status
CreateCollectionOperation::GetSnapshot(ScopedSnapshotT& ss) const {
    STATUS_CHECK(CheckDone());
    STATUS_CHECK(CheckIDSNotEmpty());
    if (!c_context_.collection_commit) {
        std::stringstream emsg;
        emsg << GetRepr() << ". No snapshot is available";
        return Status(SS_CONSTRAINT_CHECK_ERROR, emsg.str());
    }
    /* status = Snapshots::GetInstance().GetSnapshot(ss, c_context_.collection_commit->GetCollectionId()); */
    ss = context_.latest_ss;
    return Status::OK();
}

Status
DropCollectionOperation::DoExecute(Store& store) {
    if (!context_.collection) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Collection is missing in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    context_.collection->Deactivate();
    AddStepWithLsn(*context_.collection, context_.lsn, false);
    return Status::OK();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
