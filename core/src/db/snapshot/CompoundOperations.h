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

#include <string>
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
        /* if (GetContextLsn() <= GetStartedSS()->GetMaxLsn()) { */
        /*     return Status(SS_INVALID_CONTEX_ERROR, "Invalid LSN found in operation"); */
        /* } */
        return Status::OK();
    }

    std::string
    GetName() const override {
        return DerivedT::Name;
    }
};

class BuildOperation : public CompoundBaseOperation<BuildOperation> {
 public:
    using BaseT = CompoundBaseOperation<BuildOperation>;
    static constexpr const char* Name = "B";

    BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

 protected:
    Status
    CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const;
};

class NewSegmentOperation : public CompoundBaseOperation<NewSegmentOperation> {
 public:
    using BaseT = CompoundBaseOperation<NewSegmentOperation>;
    static constexpr const char* Name = "NS";

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegment(SegmentPtr& created);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);
};

class MergeOperation : public CompoundBaseOperation<MergeOperation> {
 public:
    using BaseT = CompoundBaseOperation<MergeOperation>;
    static constexpr const char* Name = "M";

    MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

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

    Status
    DoExecute(Store&) override;

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

    Status
    DoExecute(Store&) override;

    Status
    PreCheck() override;
};

class DropPartitionOperation : public CompoundBaseOperation<DropPartitionOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropPartitionOperation>;
    static constexpr const char* Name = "DP";
    DropPartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

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

    Status
    DoExecute(Store& store) override;

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

    Status
    DoExecute(Store& store) override;

    const IDS_TYPE&
    GetIDs() const;

 protected:
    bool reversed_;
    IDS_TYPE ids_;
};

class DropCollectionOperation : public CompoundBaseOperation<DropCollectionOperation> {
 public:
    using BaseT = CompoundBaseOperation<DropCollectionOperation>;
    static constexpr const char* Name = "DC";

    explicit DropCollectionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {
    }

    Status
    DoExecute(Store& store) override;

 private:
    ID_TYPE collection_id_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
