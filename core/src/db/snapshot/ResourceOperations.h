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

#include "db/snapshot/Operations.h"

namespace milvus {
namespace engine {
namespace snapshot {

class CollectionCommitOperation : public CommitOperation<CollectionCommit> {
 public:
    using BaseT = CommitOperation<CollectionCommit>;
    CollectionCommitOperation(OperationContext context, ScopedSnapshotT prev_ss) : BaseT(context, prev_ss) {
    }

    CollectionCommitPtr
    GetPrevResource() const override {
        return prev_ss_->GetCollectionCommit();
    }

    Status DoExecute(StorePtr) override;
};

class PartitionCommitOperation : public CommitOperation<PartitionCommit> {
 public:
    using BaseT = CommitOperation<PartitionCommit>;
    PartitionCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    PartitionCommitPtr
    GetPrevResource() const override;

    Status DoExecute(StorePtr) override;

    Status
    PreCheck() override;
};

class PartitionOperation : public CommitOperation<Partition> {
 public:
    using BaseT = CommitOperation<Partition>;
    PartitionOperation(const PartitionContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    PreCheck() override;

 protected:
    PartitionContext context_;
};

class SegmentCommitOperation : public CommitOperation<SegmentCommit> {
 public:
    using BaseT = CommitOperation<SegmentCommit>;
    SegmentCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    SegmentCommit::Ptr
    GetPrevResource() const override;

    Status DoExecute(StorePtr) override;

    Status
    PreCheck() override;
};

class SegmentOperation : public CommitOperation<Segment> {
 public:
    using BaseT = CommitOperation<Segment>;
    SegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

    Status
    PreCheck() override;
};

class SegmentFileOperation : public CommitOperation<SegmentFile> {
 public:
    using BaseT = CommitOperation<SegmentFile>;
    SegmentFileOperation(const SegmentFileContext& sc, ScopedSnapshotT prev_ss);

    Status DoExecute(StorePtr) override;

 protected:
    SegmentFileContext context_;
};

class FieldCommitOperation : public CommitOperation<FieldCommit> {
 public:
    using BaseT = CommitOperation<FieldCommit>;
    FieldCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    FieldCommit::Ptr
    GetPrevResource() const override;

    Status DoExecute(StorePtr) override;
};

class SchemaCommitOperation : public CommitOperation<SchemaCommit> {
 public:
    using BaseT = CommitOperation<SchemaCommit>;
    SchemaCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss);

    SchemaCommit::Ptr
    GetPrevResource() const override;

    Status DoExecute(StorePtr) override;
};

template <>
class LoadOperation<Collection> : public Operations {
 public:
    explicit LoadOperation(const LoadOperationContext& context)
        : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::O_Leaf), context_(context) {
    }

    const Status&
    ApplyToStore(StorePtr store) override {
        if (done_) {
            Done(store);
            return status_;
        }
        Status status;
        if (context_.id == 0 && context_.name != "") {
            status = store->GetCollection(context_.name, resource_);
        } else {
            status = store->GetResource<Collection>(context_.id, resource_);
        }
        SetStatus(status);
        Done(store);
        return status_;
    }

    Status
    GetResource(CollectionPtr& res, bool wait = true) {
        if (wait) {
            WaitToFinish();
        }
        STATUS_CHECK(CheckDone());
        if (!resource_) {
            return Status(SS_NOT_FOUND_ERROR, "No specified resource");
        }
        res = resource_;
        return Status::OK();
    }

 protected:
    LoadOperationContext context_;
    CollectionPtr resource_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
