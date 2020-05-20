#include "CompoundOperations.h"
#include "Snapshots.h"

BuildOperation::BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {};
BuildOperation::BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {};

bool
BuildOperation::PreExecute() {
    SegmentCommitOperation op(context_, prev_ss_);
    op();
    context_.new_segment_commit = op.GetResource();
    if (!context_.new_segment_commit) return false;

    PartitionCommitOperation pc_op(context_, prev_ss_);
    pc_op();

    OperationContext cc_context;
    cc_context.new_partition_commit = pc_op.GetResource();
    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    cc_op();

    for (auto& new_segment_file : context_.new_segment_files) {
        AddStep(*new_segment_file);
    }
    AddStep(*context_.new_segment_commit);
    AddStep(*pc_op.GetResource());
    AddStep(*cc_op.GetResource());
    return true;
}

bool
BuildOperation::DoExecute() {
    if (status_ != OP_PENDING) {
        return false;
    }
    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        return false;
    }
    /* if (!prev_ss_->HasFieldElement(context_.field_name, context_.field_element_name)) { */
    /*     status_ = OP_FAIL_INVALID_PARAMS; */
    /*     return false; */
    /* } */

    // PXU TODO: Temp comment below check for test
    /* if (prev_ss_->HasSegmentFile(context_.field_name, context_.field_element_name, context_.segment_id)) { */
    /*     status_ = OP_FAIL_DUPLICATED; */
    /*     return; */
    /* } */

    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        // PXU TODO: Produce cleanup job
        return false;
    }
    std::any_cast<SegmentFilePtr>(steps_[0])->Activate();
    std::any_cast<SegmentCommitPtr>(steps_[1])->Activate();
    std::any_cast<PartitionCommitPtr>(steps_[2])->Activate();
    std::any_cast<CollectionCommitPtr>(steps_[3])->Activate();
    return true;
}

SegmentFilePtr
BuildOperation::NewSegmentFile(const SegmentFileContext& context) {
    SegmentFileOperation new_sf_op(context, prev_ss_);
    new_sf_op();
    context_.new_segment_files.push_back(new_sf_op.GetResource());
    return new_sf_op.GetResource();
}

NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {};
NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {};

bool
NewSegmentOperation::DoExecute() {
    auto i = 0;
    for(; i<context_.new_segment_files.size(); ++i) {
        std::any_cast<SegmentFilePtr>(steps_[i])->Activate();
    }
    std::any_cast<SegmentPtr>(steps_[i++])->Activate();
    std::any_cast<SegmentCommitPtr>(steps_[i++])->Activate();
    std::any_cast<PartitionCommitPtr>(steps_[i++])->Activate();
    std::any_cast<CollectionCommitPtr>(steps_[i++])->Activate();
    return true;
}

bool
NewSegmentOperation::PreExecute() {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    SegmentCommitOperation op(context_, prev_ss_);
    op();
    context_.new_segment_commit = op.GetResource();
    if (!context_.new_segment_commit) return false;

    PartitionCommitOperation pc_op(context_, prev_ss_);
    pc_op();

    OperationContext cc_context;
    cc_context.new_partition_commit = pc_op.GetResource();
    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    cc_op();

    for (auto& new_segment_file : context_.new_segment_files) {
        AddStep(*new_segment_file);
    }
    AddStep(*context_.new_segment);
    AddStep(*context_.new_segment_commit);
    AddStep(*pc_op.GetResource());
    AddStep(*cc_op.GetResource());
    return true;
}

SegmentPtr
NewSegmentOperation::NewSegment() {
    SegmentOperation op(context_, prev_ss_);
    op();
    context_.new_segment = op.GetResource();
    return context_.new_segment;
}

SegmentFilePtr
NewSegmentOperation::NewSegmentFile(const SegmentFileContext& context) {
    auto c = context;
    c.segment_id = context_.new_segment->GetID();
    c.partition_id = context_.new_segment->GetPartitionId();
    SegmentFileOperation new_sf_op(c, prev_ss_);
    new_sf_op();
    context_.new_segment_files.push_back(new_sf_op.GetResource());
    return new_sf_op.GetResource();
}

MergeOperation::MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {};
MergeOperation::MergeOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id)
    : BaseT(context, collection_id, commit_id) {};

SegmentPtr
MergeOperation::NewSegment() {
    if (context_.new_segment) return context_.new_segment;
    SegmentOperation op(context_, prev_ss_);
    op();
    context_.new_segment = op.GetResource();
    return context_.new_segment;
}

SegmentFilePtr
MergeOperation::NewSegmentFile(const SegmentFileContext& context) {
    // PXU TODO: Check element type and segment file mapping rules
    auto new_segment = NewSegment();
    auto c = context;
    c.segment_id = new_segment->GetID();
    c.partition_id = new_segment->GetPartitionId();
    SegmentFileOperation new_sf_op(c, prev_ss_);
    new_sf_op();
    context_.new_segment_files.push_back(new_sf_op.GetResource());
    return new_sf_op.GetResource();
}

bool
MergeOperation::PreExecute() {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    SegmentCommitOperation op(context_, prev_ss_);
    op();
    context_.new_segment_commit = op.GetResource();
    if (!context_.new_segment_commit) return false;

    // PXU TODO: Check stale segments

    PartitionCommitOperation pc_op(context_, prev_ss_);
    pc_op();

    OperationContext cc_context;
    cc_context.new_partition_commit = pc_op.GetResource();
    CollectionCommitOperation cc_op(cc_context, prev_ss_);
    cc_op();

    for (auto& new_segment_file : context_.new_segment_files) {
        AddStep(*new_segment_file);
    }
    AddStep(*context_.new_segment);
    AddStep(*context_.new_segment_commit);
    AddStep(*pc_op.GetResource());
    AddStep(*cc_op.GetResource());
    return true;
}

bool
MergeOperation::DoExecute() {
    auto i = 0;
    for(; i<context_.new_segment_files.size(); ++i) {
        std::any_cast<SegmentFilePtr>(steps_[i])->Activate();
    }
    std::any_cast<SegmentPtr>(steps_[i++])->Activate();
    std::any_cast<SegmentCommitPtr>(steps_[i++])->Activate();
    std::any_cast<PartitionCommitPtr>(steps_[i++])->Activate();
    std::any_cast<CollectionCommitPtr>(steps_[i++])->Activate();
    return true;
}
