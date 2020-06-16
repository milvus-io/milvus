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

#include <assert.h>
#include <any>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "Context.h"
#include "db/snapshot/Snapshot.h"
#include "db/snapshot/Store.h"
#include "utils/Error.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

using StepsT = std::vector<std::any>;
using CheckStaleFunc = std::function<Status(ScopedSnapshotT&)>;

enum OperationsType { Invalid, W_Leaf, O_Leaf, W_Compound, O_Compound };

class Operations : public std::enable_shared_from_this<Operations> {
 public:
    Operations(const OperationContext& context, ScopedSnapshotT prev_ss,
               const OperationsType& type = OperationsType::Invalid);

    const ScopedSnapshotT&
    GetStartedSS() const {
        return prev_ss_;
    }

    const ScopedSnapshotT&
    GetAdjustedSS() const {
        return context_.prev_ss;
    }

    virtual const LSN_TYPE&
    GetContextLsn() const {
        return context_.lsn;
    }

    virtual Status
    CheckStale(const CheckStaleFunc& checker = nullptr) const;
    virtual Status
    DoCheckStale(ScopedSnapshotT& latest_snapshot) const;

    template <typename StepT>
    void
    AddStep(const StepT& step, bool activate = true);
    template <typename StepT>
    void
    AddStepWithLsn(const StepT& step, const LSN_TYPE& lsn, bool activate = true);
    void
    SetStepResult(ID_TYPE id) {
        ids_.push_back(id);
    }

    StepsT&
    GetSteps() {
        return steps_;
    }

    ID_TYPE
    GetID() const;

    virtual const OperationsType&
    GetType() const {
        return type_;
    }

    virtual Status
    OnExecute(Store&);
    virtual Status
    PreExecute(Store&);
    virtual Status
    DoExecute(Store&);
    virtual Status
    PostExecute(Store&);

    virtual Status
    GetSnapshot(ScopedSnapshotT& ss) const;

    virtual Status
    operator()(Store& store);
    virtual Status
    Push(bool sync = true);

    virtual Status
    PreCheck();

    virtual Status
    ApplyToStore(Store& store);

    Status
    WaitToFinish();

    void
    Done(Store& store);

    void
    SetStatus(const Status& status);

    Status
    GetStatus() const {
        return status_;
    }

    virtual std::string
    GetName() const {
        return typeid(*this).name();
    }
    virtual std::string
    GetRepr() const;

    virtual std::string
    ToString() const;

    Status
    RollBack();

    virtual Status
    OnSnapshotStale();
    virtual Status
    OnSnapshotDropped();

    virtual ~Operations();

    friend std::ostream&
    operator<<(std::ostream& out, const Operations& operation);

 protected:
    virtual std::string
    SuccessString() const;
    virtual std::string
    FailureString() const;

    Status
    DoneRequired() const;
    Status
    IDSNotEmptyRequried() const;
    Status
    PrevSnapshotRequried() const;

    Status
    ApplyRollBack(Store&);

    OperationContext context_;
    ScopedSnapshotT prev_ss_;
    StepsT steps_;
    std::vector<ID_TYPE> ids_;
    bool done_ = false;
    Status status_;
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;
    ID_TYPE uid_;
    OperationsType type_;
};

template <typename StepT>
void
Operations::AddStep(const StepT& step, bool activate) {
    auto s = std::make_shared<StepT>(step);
    if (activate)
        s->Activate();
    steps_.push_back(s);
}

template <typename StepT>
void
Operations::AddStepWithLsn(const StepT& step, const LSN_TYPE& lsn, bool activate) {
    auto s = std::make_shared<StepT>(step);
    if (activate)
        s->Activate();
    s->SetLsn(lsn);
    steps_.push_back(s);
}

template <typename ResourceT>
class CommitOperation : public Operations {
 public:
    using BaseT = Operations;
    CommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss, OperationsType::W_Leaf) {
    }

    virtual typename ResourceT::Ptr
    GetPrevResource() const {
        return nullptr;
    }

    Status
    GetResource(typename ResourceT::Ptr& res, bool wait = false) {
        if (!status_.ok())
            return status_;
        if (wait) {
            WaitToFinish();
        }
        auto status = DoneRequired();
        if (!status.ok())
            return status;
        status = IDSNotEmptyRequried();
        if (!status.ok())
            return status;
        resource_->SetID(ids_[0]);
        res = resource_;
        return status;
    }

 protected:
    Status
    ResourceNotNullRequired() const {
        Status status;
        if (!resource_)
            return Status(SS_CONSTRAINT_CHECK_ERROR, "No specified resource");
        return status;
    }

    typename ResourceT::Ptr resource_;
};

template <typename ResourceT>
class LoadOperation : public Operations {
 public:
    explicit LoadOperation(const LoadOperationContext& context)
        : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::O_Leaf), context_(context) {
    }

    Status
    ApplyToStore(Store& store) override {
        if (done_) {
            Done(store);
            return status_;
        }
        auto status = store.GetResource<ResourceT>(context_.id, resource_);
        SetStatus(status);
        Done(store);
        return status_;
    }

    Status
    GetResource(typename ResourceT::Ptr& res, bool wait = false) {
        if (!status_.ok())
            return status_;
        if (wait) {
            WaitToFinish();
        }
        auto status = DoneRequired();
        if (!status.ok())
            return status;
        status = ResourceNotNullRequired();
        if (!status.ok())
            return status;
        res = resource_;
        return status;
    }

 protected:
    Status
    ResourceNotNullRequired() const {
        Status status;
        if (!resource_)
            return Status(SS_CONSTRAINT_CHECK_ERROR, "No specified resource");
        return status;
    }

    LoadOperationContext context_;
    typename ResourceT::Ptr resource_;
};

template <typename ResourceT>
class HardDeleteOperation : public Operations {
 public:
    explicit HardDeleteOperation(ID_TYPE id)
        : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::W_Leaf), id_(id) {
    }

    Status
    ApplyToStore(Store& store) override {
        if (done_)
            return status_;
        auto status = store.RemoveResource<ResourceT>(id_);
        SetStatus(status);
        Done(store);
        return status_;
    }

 protected:
    ID_TYPE id_;
};

template <>
class HardDeleteOperation<Collection> : public Operations {
 public:
    explicit HardDeleteOperation(ID_TYPE id)
        : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::W_Leaf), id_(id) {
    }

    Status
    ApplyToStore(Store& store) override {
        if (done_) {
            Done(store);
            return status_;
        }
        auto status = store.RemoveCollection(id_);
        SetStatus(status);
        Done(store);
        return status_;
    }

 protected:
    ID_TYPE id_;
};

using OperationsPtr = std::shared_ptr<Operations>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
