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

    explicit IterateHandler(ScopedSnapshotT ss) : ss_(ss) {
    }

    virtual Status
    PreIterate() {
        return Status::OK();
    }
    virtual Status
    Handle(const typename ResourceT::Ptr& resource) = 0;

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
    Status status_;
    mutable std::mutex mtx_;
};

using SegmentExecutorT = std::function<Status(const Segment::Ptr&, IterateHandler<Segment>*)>;
struct SegmentCollector : public IterateHandler<Segment> {
    using ResourceT = Segment;
    using BaseT = IterateHandler<Segment>;

    explicit SegmentCollector(ScopedSnapshotT ss, const SegmentExecutorT& executor) : BaseT(ss), executor_(executor) {
    }

    Status
    Handle(const typename ResourceT::Ptr& segment) override {
        return executor_(segment, this);
    }

    SegmentExecutorT executor_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
