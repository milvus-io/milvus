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
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "db/snapshot/Context.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/Snapshot.h"
#include "db/snapshot/Store.h"
#include "utils/Error.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

using CheckStaleFunc = std::function<Status(ScopedSnapshotT&)>;
template <typename ResourceT>
using StepsContextSet = std::set<typename ResourceContext<ResourceT>::Ptr>;
using StepsHolderT =
    std::tuple<StepsContextSet<CollectionCommit>, StepsContextSet<Collection>, StepsContextSet<SchemaCommit>,
               StepsContextSet<FieldCommit>, StepsContextSet<Field>, StepsContextSet<FieldElement>,
               StepsContextSet<PartitionCommit>, StepsContextSet<Partition>, StepsContextSet<SegmentCommit>,
               StepsContextSet<Segment>, StepsContextSet<SegmentFile>>;

enum OperationsType { Invalid, W_Leaf, O_Leaf, W_Compound, O_Compound };

class Operations : public std::enable_shared_from_this<Operations> {
 public:
    using TimeoutCBT = std::function<Status(const Status&)>;

    Operations(const OperationContext& context, ScopedSnapshotT prev_ss,
               const OperationsType& type = OperationsType::Invalid);

    const OperationContext&
    GetContext() const {
        return context_;
    }

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

    void
    SetContextLsn(LSN_TYPE lsn) {
        context_.lsn = lsn;
    }

    virtual Status
    CheckStale(const CheckStaleFunc& checker = nullptr) const;
    virtual Status
    DoCheckStale(ScopedSnapshotT& latest_snapshot) const;

    template <typename StepT>
    void
    AddStep(const StepT& step, ResourceContextPtr<StepT> step_context = nullptr, bool activate = true);
    template <typename StepT>
    void
    AddStepWithLsn(const StepT& step, const LSN_TYPE& lsn, ResourceContextPtr<StepT> step_context = nullptr,
                   bool activate = true);
    void
    SetStepResult(ID_TYPE id) {
        ids_.push_back(id);
    }

    const size_t
    GetPos() const {
        return last_pos_;
    }

    StepsHolderT&
    GetStepHolders() {
        return holders_;
    }

    ID_TYPE
    GetID() const;

    virtual const OperationsType&
    GetType() const {
        return type_;
    }

    virtual Status OnExecute(StorePtr);
    virtual Status PreExecute(StorePtr);
    virtual Status DoExecute(StorePtr);
    virtual Status PostExecute(StorePtr);

    virtual Status
    GetSnapshot(ScopedSnapshotT& ss) const;

    virtual Status
    operator()(StorePtr store);

    Status
    Push(bool sync = true);

    virtual Status
    PreCheck();

    virtual const Status&
    ApplyToStore(StorePtr);

    const Status&
    WaitToFinish();

    void
    Done(StorePtr store);

    void
    SetStatus(const Status& status);

    const Status&
    GetStatus() const {
        std::unique_lock<std::mutex> lock(finish_mtx_);
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

    virtual Status
    OnSnapshotStale();
    virtual Status
    OnSnapshotDropped();

    void
    Abort() {
        aborted_ = true;
    }
    bool
    HasAborted() const {
        return aborted_;
    }

    virtual ~Operations();

    friend std::ostream&
    operator<<(std::ostream& out, const Operations& operation);

 protected:
    virtual Status
    OnApplySuccessCallback(ID_TYPE result_id);
    virtual Status OnApplyErrorCallback(Status);
    virtual Status OnApplyTimeoutCallback(StorePtr);

    virtual std::string
    SuccessString() const;
    virtual std::string
    FailureString() const;

    Status
    CheckDone() const;
    Status
    CheckIDSNotEmpty() const;
    Status
    CheckPrevSnapshot() const;

    void
    RollBack();

    OperationContext context_;
    ScopedSnapshotT prev_ss_;
    StepsHolderT holders_;
    size_t last_pos_;
    std::vector<ID_TYPE> ids_;
    bool done_ = false;
    Status status_;
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;
    ID_TYPE uid_;
    OperationsType type_;
    double execution_time_ = 0;
    std::atomic_bool aborted_ = false;
};

template <typename StepT>
void
Operations::AddStep(const StepT& step, ResourceContextPtr<StepT> step_context, bool activate) {
    if (step_context == nullptr) {
        step_context = ResourceContextBuilder<StepT>().SetOp(meta::oAdd).CreatePtr();
    }

    auto s = std::make_shared<StepT>(step);
    step_context->AddResource(s);
    if (activate) {
        s->Activate();
        step_context->AddAttr(StateField::Name);
    }

    last_pos_ = Index<StepsContextSet<StepT>, StepsHolderT>::value;
    auto& holder = std::get<Index<StepsContextSet<StepT>, StepsHolderT>::value>(holders_);
    holder.insert(step_context);
}

template <typename StepT>
void
Operations::AddStepWithLsn(const StepT& step, const LSN_TYPE& lsn, ResourceContextPtr<StepT> step_context,
                           bool activate) {
    if (step_context == nullptr) {
        step_context = ResourceContextBuilder<StepT>().SetOp(meta::oAdd).CreatePtr();
    }

    auto s = std::make_shared<StepT>(step);
    step_context->AddResource(s);
    if (activate) {
        s->Activate();
        step_context->AddAttr(StateField::Name);
    }
    s->SetLsn(lsn);
    step_context->AddAttr(LsnField::Name);

    last_pos_ = Index<StepsContextSet<StepT>, StepsHolderT>::value;
    auto& holder = std::get<Index<StepsContextSet<StepT>, StepsHolderT>::value>(holders_);
    holder.insert(step_context);
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
        STATUS_CHECK(CheckDone());
        STATUS_CHECK(CheckIDSNotEmpty());
        resource_->SetID(ids_[0]);
        res = resource_;
        return Status::OK();
    }

 protected:
    Status
    CheckResource() const {
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

    const Status&
    ApplyToStore(StorePtr store) override {
        if (done_) {
            Done(store);
            return status_;
        }
        auto status = store->GetResource<ResourceT>(context_.id, resource_);
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
        STATUS_CHECK(CheckDone());
        STATUS_CHECK(CheckResource());
        res = resource_;
        return Status::OK();
    }

 protected:
    Status
    CheckResource() const {
        Status status;
        if (!resource_)
            return Status(SS_CONSTRAINT_CHECK_ERROR, "No specified resource");
        return status;
    }

    LoadOperationContext context_;
    typename ResourceT::Ptr resource_;
};

template <typename ResourceT>
class SoftDeleteOperation : public Operations {
 public:
    using BaseT = Operations;
    explicit SoftDeleteOperation(ID_TYPE id) : BaseT(OperationContext(), ScopedSnapshotT()), id_(id) {
    }

    Status
    GetResource(typename ResourceT::Ptr& res, bool wait = false) {
        if (!status_.ok())
            return status_;
        if (wait) {
            WaitToFinish();
        }
        STATUS_CHECK(CheckDone());
        STATUS_CHECK(CheckIDSNotEmpty());
        res = resource_;
        return Status::OK();
    }

    Status
    DoExecute(StorePtr store) override {
        auto status = store->GetResource<ResourceT>(id_, resource_);
        if (!status.ok()) {
            return status;
        }
        if (!resource_) {
            std::stringstream emsg;
            emsg << "Specified " << typeid(ResourceT).name() << " id=" << id_ << " not found";
            return Status(SS_NOT_FOUND_ERROR, emsg.str());
        }
        resource_->Deactivate();
        auto r_ctx_p = ResourceContextBuilder<ResourceT>().SetResource(resource_).SetOp(meta::oUpdate).CreatePtr();
        r_ctx_p->AddAttr(StateField::Name);
        AddStep(*resource_, r_ctx_p, false);
        return status;
    }

 protected:
    ID_TYPE id_;
    typename ResourceT::Ptr resource_;
};

template <typename ResourceT>
class HardDeleteOperation : public Operations {
 public:
    explicit HardDeleteOperation(ID_TYPE id)
        : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::W_Leaf), id_(id) {
    }

    const Status&
    ApplyToStore(StorePtr store) override {
        if (done_)
            return status_;
        auto status = store->RemoveResource<ResourceT>(id_);
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
