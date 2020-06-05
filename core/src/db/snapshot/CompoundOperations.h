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

class CommonOperation : public Operations {
 public:
    using BaseT = Operations;

    CommonOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    CommonOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    PreCheck() override;
};

class BuildOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;

    BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);

    std::string
    OperationRepr() const override;

 protected:
    Status
    CheckSegmentStale(ScopedSnapshotT& latest_snapshot, ID_TYPE segment_id) const;
};

class NewSegmentOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegment(SegmentPtr& created);

    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr& created);
};

class MergeOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;

    MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    MergeOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    DoExecute(Store&) override;

    Status
    CommitNewSegment(SegmentPtr&);
    Status
    CommitNewSegmentFile(const SegmentFileContext& context, SegmentFilePtr&);

    std::string
    OperationRepr() const override;
};

class CreateCollectionOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;
    explicit CreateCollectionOperation(const CreateCollectionContext& context);

    Status
    DoExecute(Store&) override;

    Status
    GetSnapshot(ScopedSnapshotT& ss) const override;

    Status
    PreCheck() override;

    const LSN_TYPE&
    GetContextLsn() const override {
        return context_.lsn;
    }

 private:
    CreateCollectionContext context_;
};

class CreatePartitionOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;
    CreatePartitionOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    CreatePartitionOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    Status
    CommitNewPartition(const PartitionContext& context, PartitionPtr& partition);

    Status
    DoExecute(Store&) override;

    Status
    PreCheck() override;

    std::string
    OperationRepr() const override;
};

class DropPartitionOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;
    DropPartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss);

    Status
    DoExecute(Store&) override;

    const LSN_TYPE&
    GetContextLsn() const override {
        return context_.lsn;
    }

    std::string
    OperationRepr() const override;

 protected:
    PartitionContext context_;
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

class SoftDeleteCollectionOperation : public CommonOperation {
 public:
    using BaseT = CommonOperation;
    explicit SoftDeleteCollectionOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
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
