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

#pragma once

#include <map>
#include <set>
#include <string>
#include <utility>
#include "ResourceOperations.h"
#include "Snapshot.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename DerivedT>
class CompoundBaseOperation : public Operations {
 public:
    using BaseT = Operations;

    CompoundBaseOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss, OperationsType::W_Compound) {
    }

    std::string
    GetRepr() const override {
        std::stringstream ss;
        ss << "<" << GetName() << "(";
        if (GetAdjustedSS()) {
            ss << "SS=" << GetAdjustedSS()->GetID();
        }
        ss << "," << context_.ToString();
        ss << ",LSN=" << GetContextLsn();
        ss << ")>";
        return ss.str();
    }

    Status
    PreCheck() override {
        // TODO
        if (GetContextLsn() == 0) {
            SetContextLsn(GetStartedSS()->GetMaxLsn());
        } else if (GetContextLsn() < GetStartedSS()->GetMaxLsn()) {
            return Status(SS_INVALID_CONTEX_ERROR, "Invalid LSN found in operation");
        }
        return Status::OK();
    }

    std::string
    GetName() const override {
        return DerivedT::Name;
    }
};

class ChangeSegmentFileOperation : public CompoundBaseOperation<ChangeSegmentFileOperation> {
 public:
    using BaseT = CompoundBaseOperation<ChangeSegmentFileOperation>;
    static constexpr const char* Name = "CSF";

    ChangeSegmentFileOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    Status
    CommitRowCountDelta(SIZE_TYPE delta, bool sub = true);

 protected:
    Status
    CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const;

    SIZE_TYPE delta_ = 0;
    bool sub_;
};

class MultiSegmentsOperation : public CompoundBaseOperation<MultiSegmentsOperation> {
 public:
    using BaseT = CompoundBaseOperation<MultiSegmentsOperation>;
    static constexpr const char* Name = "MS";

    MultiSegmentsOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    CommitNewSegment(const OperationContext& context, SegmentPtr&);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    Status
    CommitRowCount(ID_TYPE segment_id, SIZE_TYPE delta);

 protected:
    std::map<ID_TYPE, Segment::VecT> new_segments_;
    std::map<ID_TYPE, SIZE_TYPE> new_segment_counts_;
};

class CompoundSegmentsOperation : public CompoundBaseOperation<CompoundSegmentsOperation> {
 public:
    using BaseT = CompoundBaseOperation<CompoundSegmentsOperation>;
    static constexpr const char* Name = "CS";

    CompoundSegmentsOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    AddStaleSegmentFile(const SegmentFilePtr&);

    Status
    CommitNewSegment(const OperationContext& context, SegmentPtr&);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    Status
    CommitRowCountDelta(ID_TYPE segment_id, SIZE_TYPE delta, bool sub = true);

 protected:
    bool
    StaleSegmentFilesModified();

 protected:
    std::map<ID_TYPE, std::pair<SIZE_TYPE, bool>> delta_;
    std::map<ID_TYPE, SegmentFile::VecT> stale_segment_files_;
    std::map<ID_TYPE, SegmentFile::VecT> new_segment_files_;
    std::set<ID_TYPE> modified_segments_;
};

class AddFieldElementOperation : public CompoundBaseOperation<AddFieldElementOperation> {
 public:
    using BaseT = CompoundBaseOperation<AddFieldElementOperation>;
    static constexpr const char* Name = "AFE";

    AddFieldElementOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    PreCheck() override;

    Status DoExecute(StorePtr) override;
};

class DropIndexOperation : public CompoundBaseOperation<DropIndexOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropIndexOperation>;
    static constexpr const char* Name = "DI";

    DropIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    PreCheck() override;

    Status DoExecute(StorePtr) override;
};

class DropAllIndexOperation : public CompoundBaseOperation<DropAllIndexOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropAllIndexOperation>;
    static constexpr const char* Name = "DAI";

    DropAllIndexOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    PreCheck() override;

    Status DoExecute(StorePtr) override;
};

class NewSegmentOperation : public CompoundBaseOperation<NewSegmentOperation> {
 public:
    using BaseT = CompoundBaseOperation<NewSegmentOperation>;
    static constexpr const char* Name = "NS";

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    CommitNewSegment(SegmentPtr& created);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    Status
    CommitRowCount(SIZE_TYPE row_cnt);

 protected:
    SIZE_TYPE row_cnt_ = 0;
};

class DropSegmentOperation : public CompoundBaseOperation<DropSegmentOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropSegmentOperation>;
    static constexpr const char* Name = "DS";

    DropSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    //    Status
    //    AddStaleSegment();
};

class MergeOperation : public CompoundBaseOperation<MergeOperation> {
 public:
    using BaseT = CompoundBaseOperation<MergeOperation>;
    static constexpr const char* Name = "M";

    MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    CommitNewSegment(SegmentPtr&);
    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr&);

    Status
    OnSnapshotStale() override;
};

class CreateCollectionOperation : public CompoundBaseOperation<CreateCollectionOperation> {
 public:
    using BaseT = CompoundBaseOperation<CreateCollectionOperation>;
    static constexpr const char* Name = "CC";

    explicit CreateCollectionOperation(const CreateCollectionContext& context);

    Status DoExecute(StorePtr) override;

    Status
    GetSnapshot(ScopedSnapshotT& ss) const override;

    Status
    PreCheck() override;

    const LSN_TYPE&
    GetContextLsn() const override {
        return c_context_.lsn;
    }

    std::string
    GetRepr() const override;

 private:
    CreateCollectionContext c_context_;
};

class CreatePartitionOperation : public CompoundBaseOperation<CreatePartitionOperation> {
 public:
    using BaseT = CompoundBaseOperation<CreatePartitionOperation>;
    static constexpr const char* Name = "CP";

    CreatePartitionOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    CommitNewPartition(const PartitionContext& context, PartitionPtr& partition);

    Status DoExecute(StorePtr) override;

    Status
    PreCheck() override;
};

class DropPartitionOperation : public CompoundBaseOperation<DropPartitionOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropPartitionOperation>;
    static constexpr const char* Name = "DP";
    DropPartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    const LSN_TYPE&
    GetContextLsn() const override {
        return c_context_.lsn;
    }

    std::string
    GetRepr() const override;

 protected:
    PartitionContext c_context_;
};

class GetSnapshotIDsOperation : public Operations {
 public:
    using BaseT = Operations;

    explicit GetSnapshotIDsOperation(ID_TYPE collection_id, bool reversed = true);

    Status DoExecute(StorePtr) override;

    const IDS_TYPE&
    GetIDs() const;

 protected:
    ID_TYPE collection_id_;
    bool reversed_;
    IDS_TYPE ids_;
};

class GetCollectionIDsOperation : public Operations {
 public:
    using BaseT = Operations;

    explicit GetCollectionIDsOperation(bool reversed = true);

    Status DoExecute(StorePtr) override;

    const IDS_TYPE&
    GetIDs() const;

 protected:
    bool reversed_ = true;
    IDS_TYPE ids_;
};

class GetAllActiveSnapshotIDsOperation : public Operations {
 public:
    using BaseT = Operations;

    GetAllActiveSnapshotIDsOperation(const RangeContext& context);

    Status DoExecute(StorePtr) override;

    const std::map<ID_TYPE, ID_TYPE>&
    GetIDs() const;

    TS_TYPE
    GetLatestUpdatedTime() const {
        return latest_update_;
    }

 protected:
    std::map<ID_TYPE, ID_TYPE> cid_ccid_;
    RangeContext updated_time_range_;
    TS_TYPE latest_update_ = std::numeric_limits<TS_TYPE>::min();
};

class DropCollectionOperation : public CompoundBaseOperation<DropCollectionOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropCollectionOperation>;
    static constexpr const char* Name = "DC";

    explicit DropCollectionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {
    }

    Status DoExecute(StorePtr) override;

 private:
    ID_TYPE collection_id_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
