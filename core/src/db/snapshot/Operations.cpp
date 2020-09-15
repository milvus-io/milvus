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

#include "db/snapshot/Operations.h"

#include <chrono>
#include <sstream>

#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ResourceGCEvent.h"
#include "db/snapshot/Snapshots.h"

namespace milvus::engine::snapshot {

static ID_TYPE UID = 1;

std::ostream&
operator<<(std::ostream& out, const Operations& operation) {
    out << operation.ToString();
    return out;
}

Operations::Operations(const OperationContext& context, ScopedSnapshotT prev_ss, const OperationsType& type)
    : context_(context),
      prev_ss_(prev_ss),
      uid_(UID++),
      status_(SS_OPERATION_PENDING, "Operation Pending"),
      type_(type) {
    if (prev_ss_ && context_.lsn == 0) {
        context_.lsn = prev_ss_->GetMaxLsn();
    }
}

std::string
Operations::SuccessString() const {
    return status_.ToString();
}

std::string
Operations::FailureString() const {
    return status_.ToString();
}

std::string
Operations::GetRepr() const {
    std::stringstream ss;
    ss << "<" << GetName() << ":" << GetID() << ">";
    return ss.str();
}

std::string
Operations::ToString() const {
    std::stringstream ss;
    ss << GetRepr();
    ss << (done_ ? " | DONE" : " | PENDING");
    if (done_) {
        if (status_.ok()) {
            ss << " | " << SuccessString();
        } else {
            ss << " | " << FailureString();
        }
    }
    ss << " | " << execution_time_ / 1000 << " ms";
    return ss.str();
}

ID_TYPE
Operations::GetID() const {
    return uid_;
}

Status
Operations::operator()(StorePtr store) {
    STATUS_CHECK(PreCheck());
    return ApplyToStore(store);
}

void
Operations::SetStatus(const Status& status) {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    status_ = status;
}

const Status&
Operations::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });
    return status_;
}

void
Operations::Done(StorePtr store) {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    done_ = true;
    if (GetType() == OperationsType::W_Compound) {
        if (!context_.latest_ss && ids_.size() > 0 && context_.new_collection_commit) {
            Snapshots::GetInstance().LoadSnapshot(store, context_.latest_ss,
                                                  context_.new_collection_commit->GetCollectionId(), ids_.back());
        }
        /* if (!context_.latest_ss && context_.new_collection_commit) { */
        /*     auto& holder = std::get<ConstPos(last_pos_)>(holders_); */
        /*     if (holder.size() > 0) */
        /*         Snapshots::GetInstance().LoadSnapshot(store, context_.latest_ss, */
        /*                 context_.new_collection_commit->GetCollectionId(), holder.rbegin()->GetID()); */
        /*     } */
        /* } */
        LOG_ENGINE_DEBUG_ << ToString();
    }
    finish_cond_.notify_all();
}

Status
Operations::PreCheck() {
    return Status::OK();
}

Status
Operations::Push(bool sync) {
    STATUS_CHECK(PreCheck());
    return OperationExecutor::GetInstance().Submit(shared_from_this(), sync);
}

Status
Operations::DoCheckStale(ScopedSnapshotT& latest_snapshot) const {
    return Status::OK();
}

Status
Operations::CheckStale(const CheckStaleFunc& checker) const {
    decltype(prev_ss_) latest_ss;
    STATUS_CHECK(Snapshots::GetInstance().GetSnapshot(latest_ss, prev_ss_->GetCollection()->GetID()));
    if (prev_ss_->GetID() != latest_ss->GetID()) {
        if (checker) {
            STATUS_CHECK(checker(latest_ss));
        } else {
            STATUS_CHECK(DoCheckStale(latest_ss));
        }
    }
    return Status::OK();
}

Status
Operations::CheckDone() const {
    if (!done_) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Should be done";
        return Status(SS_CONSTRAINT_CHECK_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
Operations::CheckIDSNotEmpty() const {
    if (ids_.size() == 0) {
        std::stringstream emsg;
        emsg << GetRepr() << ". No resource available";
        return Status(SS_CONSTRAINT_CHECK_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
Operations::CheckPrevSnapshot() const {
    if (!prev_ss_) {
        std::stringstream emsg;
        emsg << GetRepr() << ". Previous snapshot required";
        return Status(SS_CONSTRAINT_CHECK_ERROR, emsg.str());
    }
    return Status::OK();
}

Status
Operations::GetSnapshot(ScopedSnapshotT& ss) const {
    STATUS_CHECK(CheckPrevSnapshot());
    STATUS_CHECK(CheckDone());
    STATUS_CHECK(CheckIDSNotEmpty());
    ss = context_.latest_ss;
    return Status::OK();
}

const Status&
Operations::ApplyToStore(StorePtr store) {
    if (GetType() == OperationsType::W_Compound) {
        /* std::cout << ToString() << std::endl; */
    }
    if (done_) {
        Done(store);
        return status_;
    }
    auto start_time = std::chrono::high_resolution_clock::now();
    auto status = OnExecute(store);
    execution_time_ =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_time)
            .count();
    SetStatus(status);
    Done(store);
    return status_;
}

Status
Operations::OnSnapshotDropped() {
    std::stringstream msg;
    msg << "Collection " << GetStartedSS()->GetCollection()->GetID() << " was dropped before" << std::endl;
    LOG_ENGINE_WARNING_ << msg.str();
    return Status(SS_COLLECTION_DROPPED, msg.str());
}

Status
Operations::OnSnapshotStale() {
    /* std::cout << GetRepr() << " Stale SS " << prev_ss_->GetID() << " RefCnt=" << prev_ss_->ref_count() \ */
    /*     << " Curr SS " << context_.prev_ss->GetID() << " RefCnt=" << context_.prev_ss->ref_count() << std::endl; */
    return Status::OK();
}

Status
Operations::OnExecute(StorePtr store) {
    STATUS_CHECK(PreExecute(store));
    STATUS_CHECK(DoExecute(store));
    STATUS_CHECK(PostExecute(store));
    return Status::OK();
}

Status
Operations::PreExecute(StorePtr store) {
    if (GetStartedSS() && type_ == OperationsType::W_Compound) {
        STATUS_CHECK(Snapshots::GetInstance().GetSnapshot(context_.prev_ss, GetStartedSS()->GetCollectionId()));
        if (!context_.prev_ss) {
            STATUS_CHECK(OnSnapshotDropped());
        } else if (!prev_ss_->GetCollection()->IsActive()) {
            STATUS_CHECK(OnSnapshotDropped());
        } else if (prev_ss_->GetID() != context_.prev_ss->GetID()) {
            STATUS_CHECK(OnSnapshotStale());
        }
    }
    return Status::OK();
}

Status
Operations::DoExecute(StorePtr store) {
    return Status::OK();
}

Status
Operations::OnApplyTimeoutCallback(StorePtr store) {
    ApplyContext context;
    context.on_succes_cb = std::bind(&Operations::OnApplySuccessCallback, this, std::placeholders::_1);
    context.on_error_cb = std::bind(&Operations::OnApplyErrorCallback, this, std::placeholders::_1);

    auto try_times = 0;

    auto status = store->ApplyOperation(*this, context);
    while (status.code() == SS_TIMEOUT && !HasAborted()) {
        LOG_ENGINE_WARNING_ << GetName() << " Timeout! Try " << ++try_times;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        status = store->ApplyOperation(*this, context);
    }
    return status;
}

Status
Operations::OnApplySuccessCallback(ID_TYPE result_id) {
    SetStepResult(result_id);
    return Status::OK();
}

Status
Operations::OnApplyErrorCallback(Status status) {
    return status;
}

Status
Operations::PostExecute(StorePtr store) {
    ApplyContext context;
    context.on_succes_cb = std::bind(&Operations::OnApplySuccessCallback, this, std::placeholders::_1);
    context.on_error_cb = std::bind(&Operations::OnApplyErrorCallback, this, std::placeholders::_1);
    context.on_timeout_cb = std::bind(&Operations::OnApplyTimeoutCallback, this, std::placeholders::_1);

    return store->ApplyOperation(*this, context);
}

template <typename ResourceT>
void
ApplyRollBack(std::set<std::shared_ptr<ResourceContext<ResourceT>>>& step_context_set) {
    for (auto& step_context : step_context_set) {
        auto res = step_context->Resource();
        auto evt_ptr = std::make_shared<ResourceGCEvent<ResourceT>>(res);
        EventExecutor::GetInstance().Submit(evt_ptr);
        LOG_ENGINE_DEBUG_ << "Rollback " << typeid(ResourceT).name() << ": " << res->GetID();
    }
}

void
Operations::RollBack() {
    std::apply([&](auto&... step_context_set) { ((ApplyRollBack(step_context_set)), ...); }, GetStepHolders());
}

Operations::~Operations() {
    if ((!status_.ok() || !done_) && status_.code() != SS_TIMEOUT) {
        RollBack();
    }
}

}  // namespace milvus::engine::snapshot
