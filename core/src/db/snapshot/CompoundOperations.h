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

class CompoundBaseOperation : public Operations {
 public:
    using BaseT = Operations;

    CompoundBaseOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    CompoundBaseOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    std::string
    GetRepr() const override;

    Status
    PreCheck() override;
};

class BuildOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "BO";

    BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    std::string
    GetName() const override {
        return Name;
    }

 protected:
    Status
    CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const;
};

class NewSegmentOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "NSO";

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegment(SegmentPtr& created);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    std::string
    GetName() const override {
        return Name;
    }
};

class MergeOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "MO";

    MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    MergeOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegment(SegmentPtr&);
    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr&);

    std::string
    GetName() const override {
        return Name;
    }
};

class CreateCollectionOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "CCO";

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

    std::string
    GetName() const override {
        return Name;
    }

 private:
    CreateCollectionContext c_context_;
};

class CreatePartitionOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "CPO";

    CreatePartitionOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    CreatePartitionOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    CommitNewPartition(const PartitionContext& context, PartitionPtr& partition);

    Status
    DoExecute(Store&) override;

    Status
    PreCheck() override;

    std::string
    GetName() const override {
        return Name;
    }
};

class DropPartitionOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "DPO";
    DropPartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

    const LSN_TYPE&
    GetContextLsn() const override {
        return c_context_.lsn;
    }

    std::string
    GetRepr() const override;

    std::string
    GetName() const override {
        return Name;
    }

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

class SoftDeleteCollectionOperation : public CompoundBaseOperation {
 public:
    using BaseT = CompoundBaseOperation;
    static constexpr const char* Name = "DCO";

    explicit SoftDeleteCollectionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {
    }

    Status
    DoExecute(Store& store) override;

    std::string
    GetName() const override {
        return Name;
    }

 private:
    ID_TYPE collection_id_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
