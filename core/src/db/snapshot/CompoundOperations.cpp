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

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/meta/MetaAdapter.h"
#include "db/meta/MetaResourceAttrs.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

MultiSegmentsOperation::MultiSegmentsOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
MultiSegmentsOperation::DoExecute(StorePtr store) {
    if (context_.new_segments.empty()) {
        return Status(SS_INVALID_CONTEX_ERROR, "Nothing to do");
    }

    for (auto& iter : context_.new_segment_file_map) {
        for (auto& new_file : iter.second) {
            auto update_ctx = ResourceContextBuilder<SegmentFile>(meta::oUpdate).AddAttr(SizeField::Name).CreatePtr();
            AddStepWithLsn(*new_file, context_.lsn, update_ctx);
        }
    }

    std::map<ID_TYPE, SegmentCommit::VecT> new_segment_commits;
    for (auto& iter : new_segments_) {
        for (auto& new_segment : iter.second) {
            size_t row_count = new_segment_counts_[new_segment->GetID()];
            OperationContext context;
            context.new_segment = new_segment;
            // TODO(yhz): Why here get adjusted ss
            context.new_segment_files = context_.new_segment_file_map[new_segment->GetID()];
            // Set segment file row count.
            for (auto& file : context.new_segment_files) {
                if (file->GetFEtype() == engine::FieldElementType::FET_RAW) {
                    file->SetRowCount(row_count);
                    auto sf_ctx_p =
                        ResourceContextBuilder<SegmentFile>(meta::oUpdate).AddAttr(RowCountField::Name).CreatePtr();
                    AddStepWithLsn(*file, context.lsn, sf_ctx_p);
                }
            }
            auto sc_op = SegmentCommitOperation(context, GetAdjustedSS());
            STATUS_CHECK(sc_op(store));
            SegmentCommit::Ptr sc;
            STATUS_CHECK(sc_op.GetResource(sc));

            sc->SetRowCount(row_count);

            if (new_segment_commits.find(new_segment->GetPartitionId()) == new_segment_commits.end()) {
                new_segment_commits[new_segment->GetPartitionId()] = SegmentCommit::VecT();
            }
            new_segment_commits[new_segment->GetPartitionId()].push_back(sc);
            auto sc_ctx_p =
                ResourceContextBuilder<SegmentCommit>(meta::oUpdate).AddAttr(RowCountField::Name).CreatePtr();
            AddStepWithLsn(*sc, context.lsn, sc_ctx_p);
        }
    }

    for (auto& iter : new_segment_commits) {
        auto& partition_id = iter.first;
        auto context = context_;
        context.new_segment_commits = iter.second;
        PartitionCommitOperation pc_op(context, GetAdjustedSS());
        STATUS_CHECK(pc_op(store));
        STATUS_CHECK(pc_op.GetResource(context.new_partition_commit));
        auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*context.new_partition_commit, context.lsn, pc_ctx_p);
        context_.new_partition_commits.push_back(context.new_partition_commit);
    }

    CollectionCommitOperation cc_op(context_, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

Status
MultiSegmentsOperation::CommitNewSegment(const OperationContext& context, SegmentPtr& created) {
    //    if (context_.new_segment) {
    //        return Status(SS_DUPLICATED_ERROR, "Only one new segment could be created");
    //    }
    //    GetAdjustedSS()->GetPa
    if (context.prev_partition == nullptr) {
        return Status(SS_INVALID_CONTEX_ERROR, "Unknown corresponding partition");
    }

    auto partition = GetStartedSS()->GetResource<Partition>(context.prev_partition->GetID());
    if (partition == nullptr || !partition->IsActive()) {
        return Status(SS_STALE_ERROR, "partition of segment has been staled");
    }

    auto op = std::make_shared<SegmentOperation>(context, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(created));
    new_segments_[context.prev_partition->GetID()].push_back(created);
    auto s_ctx_p = ResourceContextBuilder<Segment>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, s_ctx_p);

    context_.new_segments.push_back(created);
    context_.new_segment_file_map[created->GetID()] = SegmentFile::VecT();

    return Status::OK();
}

Status
MultiSegmentsOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto segment = GetStartedSS()->GetResource<Segment>(context.segment_id);
    // TODO(yhz): May not depend on context_.new_segment
    if (!segment) {
        for (auto& seg : context_.new_segments) {
            if (seg->GetID() == context.segment_id) {
                segment = seg;
                break;
            }
        }
    }

    if (!segment || segment->GetID() != context.segment_id) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Invalid segment " << context.segment_id << " in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }

    auto ctx = context;
    ctx.partition_id = segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    context_.new_segment_file_map[created->GetSegmentId()].push_back(created);
    auto sf_ctx_p = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, sf_ctx_p);

    return Status::OK();
}

Status
MultiSegmentsOperation::CommitRowCount(ID_TYPE segment_id, SIZE_TYPE delta) {
    // TODO(yhz): may need check if segment exists
    for (auto& seg : context_.new_segments) {
        if (seg->GetID() == segment_id) {
            new_segment_counts_[segment_id] = delta;
            return Status::OK();
        }
    }

    std::string err = "Invalid segment id " + std::to_string(segment_id) + ": segment not created";
    return Status(SS_NOT_FOUND_ERROR, err);
}

CompoundSegmentsOperation::CompoundSegmentsOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
    for (auto& stale_segment_file : context_.stale_segment_files) {
        stale_segment_files_[stale_segment_file->GetSegmentId()].push_back(stale_segment_file);
        modified_segments_.insert(stale_segment_file->GetSegmentId());
    }
}

Status
CompoundSegmentsOperation::CommitRowCountDelta(ID_TYPE segment_id, SIZE_TYPE delta, bool sub) {
    if (context_.new_segment && (context_.new_segment->GetID() == segment_id)) {
        delta_[segment_id] = {delta, sub};
    } else if (modified_segments_.find(segment_id) != modified_segments_.end()) {
        delta_[segment_id] = {delta, sub};
    } else {
        return Status(SS_ERROR, "Cannot commit row count delta for segment " + std::to_string(segment_id));
    }
    return Status::OK();
}

Status
CompoundSegmentsOperation::CommitNewSegment(const OperationContext& context, SegmentPtr& created) {
    if (context_.new_segment) {
        return Status(SS_DUPLICATED_ERROR, "Only one new segment could be created");
    }
    auto op = std::make_shared<SegmentOperation>(context, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(context_.new_segment));
    created = context_.new_segment;
    auto s_ctx_p = ResourceContextBuilder<Segment>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, s_ctx_p);
    return Status::OK();
}

Status
CompoundSegmentsOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto segment = GetStartedSS()->GetResource<Segment>(context.segment_id);
    if (!segment) {
        segment = context_.new_segment;
    }

    if (!segment || segment->GetID() != context.segment_id) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Invalid segment " << context.segment_id << " in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }

    auto ctx = context;
    ctx.partition_id = segment->GetPartitionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    new_segment_files_[created->GetSegmentId()].push_back(created);
    modified_segments_.insert(created->GetSegmentId());
    auto sf_ctx_p = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, sf_ctx_p);

    return Status::OK();
}

Status
CompoundSegmentsOperation::AddStaleSegmentFile(const SegmentFilePtr& stale_segment_file) {
    stale_segment_files_[stale_segment_file->GetSegmentId()].push_back(stale_segment_file);
    modified_segments_.insert(stale_segment_file->GetSegmentId());

    return Status::OK();
}

bool
CompoundSegmentsOperation::StaleSegmentFilesModified() {
    for (auto& kv : stale_segment_files_) {
        for (auto& file : kv.second) {
            auto segment_file = GetAdjustedSS()->GetResource<SegmentFile>(file->GetID());
            if (segment_file == nullptr || segment_file->IsDeactive()) {
                return true;
            }
        }
    }

    return false;
}

Status
CompoundSegmentsOperation::DoExecute(StorePtr store) {
    if (!context_.new_segment && stale_segment_files_.empty() && new_segment_files_.empty()) {
        return Status(SS_INVALID_CONTEX_ERROR, "Nothing to do");
    }
    if (context_.new_segment && context_.new_segment->IsActive()) {
        return Status(SS_INVALID_CONTEX_ERROR, "New segment should not be active");
    }

    if (StaleSegmentFilesModified()) {
        return Status(SS_STALE_ERROR, "Segment file has been stale");
    }

    auto update_size = [&](SegmentFilePtr& file) {
        auto update_ctx = ResourceContextBuilder<SegmentFile>(meta::oUpdate).AddAttr(SizeField::Name).CreatePtr();
        AddStepWithLsn(*file, context_.lsn, update_ctx);
    };

    for (auto& kv : new_segment_files_) {
        for (auto& new_file : kv.second) {
            update_size(new_file);
        }
    }

    if (context_.new_segment) {
        modified_segments_.insert(context_.new_segment->GetID());
    }

    std::map<ID_TYPE, SegmentCommit::VecT> new_sc_map;
    for (auto& m_seg_id : modified_segments_) {
        OperationContext context;
        context.lsn = context_.lsn;
        auto itstale = stale_segment_files_.find(m_seg_id);
        if (itstale != stale_segment_files_.end()) {
            context.stale_segment_files = std::move(itstale->second);
            stale_segment_files_.erase(itstale);
        }
        auto itnew = new_segment_files_.find(m_seg_id);
        if (itnew != new_segment_files_.end()) {
            context.new_segment_files = std::move(itnew->second);
            new_segment_files_.erase(itnew);
        }

        if (context_.new_segment && context_.new_segment->GetID() == m_seg_id) {
            context.new_segment = context_.new_segment;
        }

        SegmentCommitOperation sc_op(context, GetAdjustedSS());
        STATUS_CHECK(sc_op(store));
        SegmentCommitPtr new_sc;
        STATUS_CHECK(sc_op.GetResource(new_sc));
        auto segc_ctx_p = ResourceContextBuilder<SegmentCommit>(meta::oUpdate).CreatePtr();
        auto it_delta = delta_.find(m_seg_id);
        if (it_delta != delta_.end()) {
            auto delta = std::get<0>(it_delta->second);
            auto is_sub = std::get<1>(it_delta->second);
            if (delta != 0) {
                auto new_row_cnt = 0;
                if (is_sub && new_sc->GetRowCount() < delta) {
                    return Status(SS_ERROR, "Invalid row count delta for segment " + std::to_string(m_seg_id));
                } else if (is_sub) {
                    new_row_cnt = new_sc->GetRowCount() - delta;
                } else {
                    new_row_cnt = new_sc->GetRowCount() + delta;
                }
                new_sc->SetRowCount(new_row_cnt);
                segc_ctx_p->AddAttr(RowCountField::Name);
            }
        }

        AddStepWithLsn(*new_sc, context.lsn, segc_ctx_p);
        new_sc_map[new_sc->GetPartitionId()].push_back(new_sc);
    }

    for (auto& kv : new_sc_map) {
        auto& partition_id = kv.first;
        auto context = context_;
        context.new_segment_commits = kv.second;
        PartitionCommitOperation pc_op(context, GetAdjustedSS());
        STATUS_CHECK(pc_op(store));
        STATUS_CHECK(pc_op.GetResource(context.new_partition_commit));
        auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*context.new_partition_commit, context.lsn, pc_ctx_p);
        context_.new_partition_commits.push_back(context.new_partition_commit);
    }

    CollectionCommitOperation cc_op(context_, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

ChangeSegmentFileOperation::ChangeSegmentFileOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
ChangeSegmentFileOperation::DoExecute(StorePtr store) {
    STATUS_CHECK(CheckStale(std::bind(&ChangeSegmentFileOperation::CheckSegmentStale, this, std::placeholders::_1,
                                      context_.new_segment_files[0]->GetSegmentId())));

    ID_TYPE segment_id = 0;
    for (auto& stale_segment_file : context_.stale_segment_files) {
        if (segment_id == 0) {
            segment_id = stale_segment_file->GetSegmentId();
        } else if (segment_id != stale_segment_file->GetSegmentId()) {
            return Status(SS_INVALID_CONTEX_ERROR, "All segment files should be of same segment");
        }
    }

    auto update_size = [&](SegmentFilePtr& file) {
        auto update_ctx = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
        update_ctx->AddAttr(SizeField::Name);
        AddStepWithLsn(*file, context_.lsn, update_ctx);
    };

    for (auto& new_file : context_.new_segment_files) {
        if (segment_id == 0) {
            segment_id = new_file->GetSegmentId();
        } else if (segment_id != new_file->GetSegmentId()) {
            return Status(SS_INVALID_CONTEX_ERROR, "All segment files should be of same segment");
        }
        update_size(new_file);
    }

    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    auto seg_commit_ctx_p =
        ResourceContextBuilder<SegmentCommit>(meta::oUpdate).SetResource(context_.new_segment_commit).CreatePtr();
    if (delta_ != 0) {
        auto new_row_cnt = 0;
        if (sub_ && context_.new_segment_commit->GetRowCount() < delta_) {
            return Status(SS_ERROR, "Invalid row count delta");
        } else if (sub_) {
            new_row_cnt = context_.new_segment_commit->GetRowCount() - delta_;
        } else {
            new_row_cnt = context_.new_segment_commit->GetRowCount() + delta_;
        }
        context_.new_segment_commit->SetRowCount(new_row_cnt);
        seg_commit_ctx_p->AddAttr(RowCountField::Name);
    }
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn, seg_commit_ctx_p);

    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    OperationContext cc_context;
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    auto par_commit_ctx_p =
        ResourceContextBuilder<PartitionCommit>(meta::oUpdate).SetResource(cc_context.new_partition_commit).CreatePtr();
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn, par_commit_ctx_p);

    context_.new_partition_commit = cc_context.new_partition_commit;
    //    STATUS_CHECK(pc_op.GetResource(context_.new_partition_commit));
    //    AddStepWithLsn(*context_.new_partition_commit, context_.lsn);

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto c_commit_ctx_p =
        ResourceContextBuilder<CollectionCommit>(meta::oUpdate).SetResource(context_.new_collection_commit).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, c_commit_ctx_p);

    return Status::OK();
}

Status
ChangeSegmentFileOperation::CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const {
    auto segment = latest_snapshot->GetResource<Segment>(segment_id);
    if (!segment) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Target segment " << segment_id << " is stale";
        return Status(SS_STALE_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
ChangeSegmentFileOperation::CommitRowCountDelta(SIZE_TYPE delta, bool sub) {
    delta_ = delta;
    sub_ = sub;
    return Status::OK();
}

Status
ChangeSegmentFileOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    STATUS_CHECK(CheckStale(
        std::bind(&ChangeSegmentFileOperation::CheckSegmentStale, this, std::placeholders::_1, context.segment_id)));

    auto segment = GetStartedSS()->GetResource<Segment>(context.segment_id);
    if (!segment || (context_.new_segment_files.size() > 0 &&
                     (context_.new_segment_files[0]->GetSegmentId() != context.segment_id))) {
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
    auto sf_ctx_p = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, sf_ctx_p);

    return Status::OK();
}

AddFieldElementOperation::AddFieldElementOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
AddFieldElementOperation::PreCheck() {
    if (context_.stale_field_elements.size() > 0 || context_.new_field_elements.size() == 0) {
        return Status(SS_INVALID_CONTEX_ERROR, "No new field element or at least one stale field element");
    }

    return Status::OK();
}

Status
AddFieldElementOperation::DoExecute(StorePtr store) {
    OperationContext cc_context;
    {
        auto context = context_;
        context.new_field_elements.clear();
        for (auto& new_fe : context_.new_field_elements) {
            if (new_fe->GetCollectionId() != GetAdjustedSS()->GetCollectionId()) {
                std::stringstream emsg;
                emsg << GetRepr() << ". Invalid collection id " << new_fe->GetCollectionId();
                return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
            }
            auto field = GetAdjustedSS()->GetResource<Field>(new_fe->GetFieldId());
            if (!field) {
                std::stringstream emsg;
                emsg << GetRepr() << ". Invalid field id " << new_fe->GetFieldId();
                return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
            }
            FieldElementPtr field_element;
            auto status = GetAdjustedSS()->GetFieldElement(field->GetName(), new_fe->GetName(), field_element);
            if (status.ok()) {
                std::stringstream emsg;
                emsg << GetRepr() << ". Duplicate field element name " << new_fe->GetName();
                return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
            }

            STATUS_CHECK(store->CreateResource<FieldElement>(FieldElement(*new_fe), field_element));
            auto fe_ctx_p = ResourceContextBuilder<FieldElement>(meta::oUpdate).CreatePtr();
            AddStepWithLsn(*field_element, context.lsn, fe_ctx_p);

            context.new_field_elements.push_back(field_element);
        }

        FieldCommitOperation fc_op(context, GetAdjustedSS());
        STATUS_CHECK(fc_op(store));
        FieldCommitPtr new_field_commit;
        STATUS_CHECK(fc_op.GetResource(new_field_commit));
        auto fc_ctx_p = ResourceContextBuilder<FieldCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*new_field_commit, context.lsn, fc_ctx_p);
        context.new_field_commits.push_back(new_field_commit);
        for (auto& kv : GetAdjustedSS()->GetResources<FieldCommit>()) {
            if (kv.second->GetFieldId() == new_field_commit->GetFieldId()) {
                context.stale_field_commits.push_back(kv.second.Get());
            }
        }

        SchemaCommitOperation sc_op(context, GetAdjustedSS());

        STATUS_CHECK(sc_op(store));
        STATUS_CHECK(sc_op.GetResource(cc_context.new_schema_commit));
        auto sc_ctx_p = ResourceContextBuilder<SchemaCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*cc_context.new_schema_commit, context.lsn, sc_ctx_p);
    }

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

DropAllIndexOperation::DropAllIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
DropAllIndexOperation::PreCheck() {
    if (context_.stale_field_elements.size() == 0) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Stale field element is requried";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }

    for (const auto& stale_fe : context_.stale_field_elements) {
        if (!GetStartedSS()->GetResource<FieldElement>(stale_fe->GetID())) {
            std::stringstream emsg;
            emsg << GetRepr() << ".  Specified field element " << stale_fe->GetName();
            emsg << " is stale";
            return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
        }
    }

    // TODO: Check type
    return Status::OK();
}

Status
DropAllIndexOperation::DoExecute(StorePtr store) {
    auto& segment_files = GetAdjustedSS()->GetResources<SegmentFile>();

    OperationContext cc_context;
    {
        auto context = context_;

        FieldCommitOperation fc_op(context, GetAdjustedSS());
        STATUS_CHECK(fc_op(store));
        FieldCommitPtr new_field_commit;
        STATUS_CHECK(fc_op.GetResource(new_field_commit));
        auto fc_ctx_p = ResourceContextBuilder<FieldCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*new_field_commit, context.lsn, fc_ctx_p);
        context.new_field_commits.push_back(new_field_commit);
        for (auto& kv : GetAdjustedSS()->GetResources<FieldCommit>()) {
            if (kv.second->GetFieldId() == new_field_commit->GetFieldId()) {
                context.stale_field_commits.push_back(kv.second.Get());
            }
        }

        SchemaCommitOperation sc_op(context, GetAdjustedSS());

        STATUS_CHECK(sc_op(store));
        STATUS_CHECK(sc_op.GetResource(cc_context.new_schema_commit));
        auto sc_ctx_p = ResourceContextBuilder<SchemaCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*cc_context.new_schema_commit, context.lsn, sc_ctx_p);
    }

    std::map<ID_TYPE, std::vector<SegmentCommitPtr>> p_sc_map;

    std::set<ID_TYPE> stale_fe_ids;
    for (auto& fe : context_.stale_field_elements) {
        stale_fe_ids.insert(fe->GetID());
    }

    auto seg_executor = [&](const SegmentPtr& segment, SegmentIterator* handler) -> Status {
        auto sf_ids = handler->ss_->GetSegmentFileIds(segment->GetID());
        if (sf_ids.size() == 0) {
            return Status::OK();
        }
        auto context = context_;
        for (auto& sf_id : sf_ids) {
            auto sf = handler->ss_->GetResource<SegmentFile>(sf_id);
            if (stale_fe_ids.find(sf->GetFieldElementId()) == stale_fe_ids.end()) {
                continue;
            }
            context.stale_segment_files.push_back(sf);
        }
        if (context.stale_segment_files.size() == 0) {
            return Status::OK();
        }
        SegmentCommitOperation sc_op(context, GetAdjustedSS());
        STATUS_CHECK(sc_op(store));
        STATUS_CHECK(sc_op.GetResource(context.new_segment_commit));
        auto segc_ctx_p = ResourceContextBuilder<SegmentCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*context.new_segment_commit, context.lsn, segc_ctx_p);
        p_sc_map[context.new_segment_commit->GetPartitionId()].push_back(context.new_segment_commit);
        return Status::OK();
    };

    auto segment_iter = std::make_shared<SegmentIterator>(GetAdjustedSS(), seg_executor);
    segment_iter->Iterate();
    STATUS_CHECK(segment_iter->GetStatus());

    for (auto& kv : p_sc_map) {
        auto& partition_id = kv.first;
        auto context = context_;
        context.new_segment_commits = kv.second;
        PartitionCommitOperation pc_op(context, GetAdjustedSS());
        STATUS_CHECK(pc_op(store));
        STATUS_CHECK(pc_op.GetResource(context.new_partition_commit));
        auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*context.new_partition_commit, context.lsn, pc_ctx_p);
        cc_context.new_partition_commits.push_back(context.new_partition_commit);
    }

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

DropIndexOperation::DropIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
DropIndexOperation::PreCheck() {
    if (context_.stale_segment_files.size() == 0) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Stale segment is requried";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    // TODO: Check segment file type

    return Status::OK();
}

Status
DropIndexOperation::DoExecute(StorePtr store) {
    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    auto sc_ctx_p = ResourceContextBuilder<SegmentCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn, sc_ctx_p);

    OperationContext cc_context;
    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn, pc_ctx_p);
    context_.new_partition_commit = cc_context.new_partition_commit;

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

NewSegmentOperation::NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
NewSegmentOperation::CommitRowCount(SIZE_TYPE row_cnt) {
    row_cnt_ = row_cnt;
    return Status::OK();
}

DropSegmentOperation::DropSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
    : BaseT(context, prev_ss) {
}

Status
DropSegmentOperation::DoExecute(StorePtr store) {
    OperationContext pc_context;
    // create a empty segment commit
    pc_context.stale_segments.push_back(context_.prev_segment);
    PartitionCommitOperation pc_op(pc_context, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(pc_context.new_partition_commit));
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*pc_context.new_partition_commit, context_.lsn, pc_ctx_p);

    auto cc_context = OperationContext();
    cc_context.new_partition_commits.push_back(pc_context.new_partition_commit);

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

Status
NewSegmentOperation::DoExecute(StorePtr store) {
    // PXU TODO:
    // 1. Check all requried field elements have related segment files
    // 2. Check Stale and others
    /* auto status = PrevSnapshotRequried(); */
    /* if (!status.ok()) return status; */
    // TODO: Check Context
    for (auto& new_file : context_.new_segment_files) {
        auto update_ctx = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
        update_ctx->AddAttr(SizeField::Name);
        AddStepWithLsn(*new_file, context_.lsn, update_ctx);
    }

    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    context_.new_segment_commit->SetRowCount(row_cnt_);
    auto sc_ctx_p = ResourceContextBuilder<SegmentCommit>(meta::oUpdate).CreatePtr();
    sc_ctx_p->AddAttr(RowCountField::Name);
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn, sc_ctx_p);
    /* std::cout << GetRepr() << " POST_SC_MAP=("; */
    /* for (auto id : context_.new_segment_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    OperationContext cc_context;
    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn, pc_ctx_p);
    context_.new_partition_commit = cc_context.new_partition_commit;
    /* std::cout << GetRepr() << " POST_PC_MAP=("; */
    /* for (auto id : cc_context.new_partition_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

Status
NewSegmentOperation::CommitNewSegment(SegmentPtr& created) {
    auto op = std::make_shared<SegmentOperation>(context_, GetStartedSS());
    STATUS_CHECK(op->Push());
    STATUS_CHECK(op->GetResource(context_.new_segment));
    created = context_.new_segment;
    auto s_ctx_p = ResourceContextBuilder<Segment>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, s_ctx_p);
    return Status::OK();
}

Status
NewSegmentOperation::CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created) {
    auto ctx = context;
    ctx.segment_id = context_.new_segment->GetID();
    ctx.partition_id = context_.new_segment->GetPartitionId();
    ctx.collection_id = GetStartedSS()->GetCollectionId();
    auto new_sf_op = std::make_shared<SegmentFileOperation>(ctx, GetStartedSS());
    STATUS_CHECK(new_sf_op->Push());
    STATUS_CHECK(new_sf_op->GetResource(created));
    auto sf_ctx_p = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, sf_ctx_p);
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
    auto seg_ctx_p = ResourceContextBuilder<Segment>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, seg_ctx_p);
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
    auto sf_ctx_p = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*created, context_.lsn, sf_ctx_p);
    return Status::OK();
}

Status
MergeOperation::DoExecute(StorePtr store) {
    auto row_cnt = 0;
    for (auto& stale_seg : context_.stale_segments) {
        row_cnt += GetStartedSS()->GetSegmentCommitBySegmentId(stale_seg->GetID())->GetRowCount();
    }

    auto update_size = [&](SegmentFilePtr& file) {
        auto update_ctx = ResourceContextBuilder<SegmentFile>(meta::oUpdate).CreatePtr();
        update_ctx->AddAttr(SizeField::Name);
        AddStepWithLsn(*file, context_.lsn, update_ctx);
    };

    for (auto& new_file : context_.new_segment_files) {
        update_size(new_file);
    }

    // PXU TODO:
    // 1. Check all required field elements have related segment files
    // 2. Check Stale and others
    SegmentCommitOperation sc_op(context_, GetAdjustedSS());
    STATUS_CHECK(sc_op(store));
    STATUS_CHECK(sc_op.GetResource(context_.new_segment_commit));
    auto sc_ctx_p = ResourceContextBuilder<SegmentCommit>(meta::oUpdate).CreatePtr();
    context_.new_segment_commit->SetRowCount(row_cnt);
    sc_ctx_p->AddAttr(RowCountField::Name);
    AddStepWithLsn(*context_.new_segment_commit, context_.lsn, sc_ctx_p);
    /* std::cout << GetRepr() << " POST_SC_MAP=("; */
    /* for (auto id : context_.new_segment_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    PartitionCommitOperation pc_op(context_, GetAdjustedSS());
    STATUS_CHECK(pc_op(store));
    OperationContext cc_context;
    STATUS_CHECK(pc_op.GetResource(cc_context.new_partition_commit));
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*cc_context.new_partition_commit, context_.lsn, pc_ctx_p);
    context_.new_partition_commit = cc_context.new_partition_commit;

    /* std::cout << GetRepr() << " POST_PC_MAP=("; */
    /* for (auto id : cc_context.new_partition_commit->GetMappings()) { */
    /*     std::cout << id << ","; */
    /* } */
    /* std::cout << ")" << std::endl; */

    CollectionCommitOperation cc_op(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    STATUS_CHECK(cc_op.GetResource(context_.new_collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, context_.lsn, cc_ctx_p);

    return Status::OK();
}

GetSnapshotIDsOperation::GetSnapshotIDsOperation(ID_TYPE collection_id, bool reversed)
    : BaseT(OperationContext(), ScopedSnapshotT(), OperationsType::O_Compound),
      collection_id_(collection_id),
      reversed_(reversed) {
}

Status
GetSnapshotIDsOperation::DoExecute(StorePtr store) {
    CollectionPtr collection;
    STATUS_CHECK(store->GetResource<Collection>(collection_id_, collection));
    if (!collection || !collection->IsActive()) {
        return Status::OK();
    }
    ids_ = store->AllActiveCollectionCommitIds(collection_id_, reversed_);
    return Status::OK();
}

const IDS_TYPE&
GetSnapshotIDsOperation::GetIDs() const {
    return ids_;
}

GetAllActiveSnapshotIDsOperation::GetAllActiveSnapshotIDsOperation(const RangeContext& context)
    : BaseT(OperationContext(), ScopedSnapshotT(), OperationsType::O_Compound), updated_time_range_(context) {
}

Status
GetAllActiveSnapshotIDsOperation::DoExecute(StorePtr store) {
    std::vector<CollectionCommitPtr> ccs;
    /* LOG_ENGINE_WARNING_ << "Upper=" << updated_time_range_.upper_bound_ << " Lower=" << updated_time_range_.low_bound_; */
    STATUS_CHECK(store->GetActiveResourcesByAttrs<CollectionCommit>(ccs, {meta::F_ID, meta::F_COLLECTON_ID},
                updated_time_range_.upper_bound_,
                updated_time_range_.low_bound_));

    for (auto& cc : ccs) {
        auto cid = cc->GetCollectionId();
        auto it = cid_ccid_.find(cid);
        if (it == cid_ccid_.end()) {
            cid_ccid_[cid] = cc->GetID();
        } else {
            cid_ccid_[cid] = std::max(it->second, cc->GetID());
        }
        latest_update_ = std::max(latest_update_, cc->GetUpdatedTime());
    }
    return Status::OK();
}

const std::map<ID_TYPE, ID_TYPE>&
GetAllActiveSnapshotIDsOperation::GetIDs() const {
    return cid_ccid_;
}

GetCollectionIDsOperation::GetCollectionIDsOperation(bool reversed)
    : BaseT(OperationContext(), ScopedSnapshotT()), reversed_(reversed) {
}

Status
GetCollectionIDsOperation::DoExecute(StorePtr store) {
    ids_ = store->AllActiveCollectionIds(reversed_);
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
DropPartitionOperation::DoExecute(StorePtr store) {
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
    auto cc_ctx_p =
        ResourceContextBuilder<CollectionCommit>(meta::oUpdate).SetResource(context_.new_collection_commit).CreatePtr();
    AddStepWithLsn(*context_.new_collection_commit, c_context_.lsn, cc_ctx_p);
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
    auto par_ctx_p = ResourceContextBuilder<Partition>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*partition, context_.lsn, par_ctx_p);
    return Status::OK();
}

Status
CreatePartitionOperation::DoExecute(StorePtr store) {
    STATUS_CHECK(CheckStale());

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
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*pc, context_.lsn, pc_ctx_p);

    OperationContext cc_context;
    cc_context.new_partition_commit = pc;
    context_.new_partition_commit = pc;
    auto cc_op = CollectionCommitOperation(cc_context, GetAdjustedSS());
    STATUS_CHECK(cc_op(store));
    CollectionCommitPtr cc;
    STATUS_CHECK(cc_op.GetResource(cc));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*cc, context_.lsn, cc_ctx_p);
    context_.new_collection_commit = cc;

    return Status::OK();
}

CreateCollectionOperation::CreateCollectionOperation(const CreateCollectionContext& context)
    : BaseT(OperationContext(), ScopedSnapshotT()), c_context_(context) {
    SetContextLsn(c_context_.lsn);
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
CreateCollectionOperation::DoExecute(StorePtr store) {
    CollectionPtr collection;
    ScopedSnapshotT ss;
    Snapshots::GetInstance().GetSnapshot(ss, c_context_.collection->GetName());
    if (ss) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Duplicated collection " << c_context_.collection->GetName();
        return Status(SS_DUPLICATED_ERROR, emsg.str());
    }

    auto status = store->CreateResource<Collection>(
        Collection(c_context_.collection->GetName(), c_context_.collection->GetParams()), collection);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.ToString();
        return status;
    }
    auto c_ctx_p = ResourceContextBuilder<Collection>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*collection, c_context_.lsn, c_ctx_p);
    context_.new_collection = collection;
    MappingT field_commit_ids = {};
    ID_TYPE result_id;
    auto field_idx = 0;
    for (auto& field_kv : c_context_.fields_schema) {
        field_idx++;
        auto& field_schema = field_kv.first;
        auto& field_elements = field_kv.second;
        FieldPtr field;
        STATUS_CHECK(store->CreateResource<Field>(
            Field(field_schema->GetName(), field_idx, field_schema->GetFtype(), field_schema->GetParams()), field));
        auto f_ctx_p = ResourceContextBuilder<Field>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*field, c_context_.lsn, f_ctx_p);
        MappingT element_ids = {};
        FieldElementPtr raw_element;
        STATUS_CHECK(
            store->CreateResource<FieldElement>(FieldElement(collection->GetID(), field->GetID(), ELEMENT_RAW_DATA,
                                                             FieldElementType::FET_RAW, ELEMENT_RAW_DATA),
                                                raw_element));
        auto fe_ctx_p = ResourceContextBuilder<FieldElement>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*raw_element, c_context_.lsn, fe_ctx_p);
        element_ids.insert(raw_element->GetID());
        for (auto& element_schema : field_elements) {
            FieldElementPtr element;
            STATUS_CHECK(store->CreateResource<FieldElement>(
                FieldElement(collection->GetID(), field->GetID(), element_schema->GetName(),
                             element_schema->GetFEtype(), element_schema->GetTypeName()),
                element));
            auto t_fe_ctx_p = ResourceContextBuilder<FieldElement>(meta::oUpdate).CreatePtr();
            AddStepWithLsn(*element, c_context_.lsn, t_fe_ctx_p);
            element_ids.insert(element->GetID());
        }
        FieldCommitPtr field_commit;
        STATUS_CHECK(store->CreateResource<FieldCommit>(FieldCommit(collection->GetID(), field->GetID(), element_ids),
                                                        field_commit));
        auto fc_ctx_p = ResourceContextBuilder<FieldCommit>(meta::oUpdate).CreatePtr();
        AddStepWithLsn(*field_commit, c_context_.lsn, fc_ctx_p);
        field_commit_ids.insert(field_commit->GetID());
    }
    SchemaCommitPtr schema_commit;
    STATUS_CHECK(
        store->CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), field_commit_ids), schema_commit));
    auto sc_ctx_p = ResourceContextBuilder<SchemaCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*schema_commit, c_context_.lsn, sc_ctx_p);
    PartitionPtr partition;
    STATUS_CHECK(store->CreateResource<Partition>(Partition("_default", collection->GetID()), partition));
    auto p_ctx_p = ResourceContextBuilder<Partition>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*partition, c_context_.lsn, p_ctx_p);
    context_.new_partition = partition;
    PartitionCommitPtr partition_commit;
    PartitionCommit temp_pc(collection->GetID(), partition->GetID());
    temp_pc.UpdateFlushIds();
    auto base_pc_path = GetResPath<Partition>(store->GetRootPath(), partition);
    temp_pc.FlushIds(base_pc_path);

    STATUS_CHECK(store->CreateResource<PartitionCommit>(std::move(temp_pc), partition_commit));
    auto pc_ctx_p = ResourceContextBuilder<PartitionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*partition_commit, c_context_.lsn, pc_ctx_p);
    context_.new_partition_commit = partition_commit;
    CollectionCommitPtr collection_commit;

    CollectionCommit temp_cc(collection->GetID(), schema_commit->GetID());
    temp_cc.UpdateFlushIds();
    temp_cc.GetMappings().insert(partition_commit->GetID());
    auto base_path = GetResPath<Collection>(store->GetRootPath(), collection);
    temp_cc.FlushIds(base_path);

    STATUS_CHECK(store->CreateResource<CollectionCommit>(std::move(temp_cc), collection_commit));
    auto cc_ctx_p = ResourceContextBuilder<CollectionCommit>(meta::oUpdate).CreatePtr();
    AddStepWithLsn(*collection_commit, c_context_.lsn, cc_ctx_p);
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
DropCollectionOperation::DoExecute(StorePtr store) {
    if (!context_.collection) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Collection is missing in context";
        return Status(SS_INVALID_CONTEX_ERROR, emsg.str());
    }
    context_.collection->Deactivate();
    auto c_ctx_p = ResourceContextBuilder<Collection>(meta::oUpdate).SetResource(context_.collection).CreatePtr();
    c_ctx_p->AddAttr(StateField::Name);
    AddStepWithLsn(*context_.collection, context_.lsn, c_ctx_p, false);
    return Status::OK();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
