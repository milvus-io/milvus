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

#include <memory>
#include <mutex>

#include "db/snapshot/Snapshot.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename T>
struct IterateHandler : public std::enable_shared_from_this<IterateHandler<T>> {
    using ResourceT = T;
    using ThisT = IterateHandler<ResourceT>;
    using Ptr = std::shared_ptr<ThisT>;
    using ExecutorT = std::function<Status(const typename T::Ptr&, ThisT*)>;

    explicit IterateHandler(ScopedSnapshotT ss, const ExecutorT& executor = {}) : ss_(ss), executor_(executor) {
    }

    virtual Status
    PreIterate() {
        return Status::OK();
    }
    virtual Status
    Handle(const typename ResourceT::Ptr& resource) {
        if (executor_) {
            return executor_(resource, this);
        }
        return Status::OK();
    }

    virtual Status
    PostIterate() {
        return Status::OK();
    }

    void
    SetStatus(Status status) {
        std::unique_lock<std::mutex> lock(mtx_);
        status_ = status;
    }
    Status
    GetStatus() const {
        std::unique_lock<std::mutex> lock(mtx_);
        return status_;
    }

    virtual void
    Iterate() {
        ss_->IterateResources<ThisT>(this->shared_from_this());
    }

    ScopedSnapshotT ss_;
    ExecutorT executor_;
    Status status_;
    mutable std::mutex mtx_;
};

using CollectionIterator = IterateHandler<Collection>;
using PartitionIterator = IterateHandler<Partition>;
using SegmentCommitIterator = IterateHandler<SegmentCommit>;
using SegmentIterator = IterateHandler<Segment>;
using SegmentFileIterator = IterateHandler<SegmentFile>;
using FieldIterator = IterateHandler<Field>;
using FieldElementIterator = IterateHandler<FieldElement>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
