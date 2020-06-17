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
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"

namespace milvus {
namespace engine {
namespace snapshot {

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
    return ss.str();
}

ID_TYPE
Operations::GetID() const {
    return uid_;
}

Status
Operations::operator()(Store& store) {
    auto status = PreCheck();
    if (!status.ok())
        return status;
    return ApplyToStore(store);
}

void
Operations::SetStatus(const Status& status) {
    status_ = status;
}

Status
Operations::WaitToFinish() {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });
    return status_;
}

void
Operations::Done(Store& store) {
    std::unique_lock<std::mutex> lock(finish_mtx_);
    done_ = true;
    if (GetType() == OperationsType::W_Compound) {
        if (!context_.latest_ss && ids_.size() > 0 && context_.new_collection_commit) {
            Snapshots::GetInstance().LoadSnapshot(store, context_.latest_ss,
                                                  context_.new_collection_commit->GetCollectionId(), ids_.back());
        }
        std::cout << ToString() << std::endl;
    }
    finish_cond_.notify_all();
}

Status
Operations::PreCheck() {
    return Status::OK();
}

Status
Operations::Push(bool sync) {
    auto status = PreCheck();
    if (!status.ok())
        return status;
    return OperationExecutor::GetInstance().Submit(shared_from_this(), sync);
}

Status
Operations::DoCheckStale(ScopedSnapshotT& latest_snapshot) const {
    return Status::OK();
}

Status
Operations::CheckStale(const CheckStaleFunc& checker) const {
    decltype(prev_ss_) latest_ss;
    auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, prev_ss_->GetCollection()->GetID());
    if (!status.ok())
        return status;
    if (prev_ss_->GetID() != latest_ss->GetID()) {
        if (checker) {
            status = checker(latest_ss);
        } else {
            status = DoCheckStale(latest_ss);
        }
    }
    return status;
}

Status
Operations::DoneRequired() const {
    Status status;
    if (!done_) {
        status = Status(SS_CONSTRAINT_CHECK_ERROR, "Operation is expected to be done");
    }
    return status;
}

Status
Operations::IDSNotEmptyRequried() const {
    Status status;
    if (ids_.size() == 0)
        status = Status(SS_CONSTRAINT_CHECK_ERROR, "No Snapshot is available");
    return status;
}

Status
Operations::PrevSnapshotRequried() const {
    Status status;
    if (!prev_ss_) {
        status = Status(SS_CONSTRAINT_CHECK_ERROR, "Prev snapshot is requried");
    }
    return status;
}

Status
Operations::GetSnapshot(ScopedSnapshotT& ss) const {
    auto status = PrevSnapshotRequried();
    if (!status.ok())
        return status;
    status = DoneRequired();
    if (!status.ok())
        return status;
    status = IDSNotEmptyRequried();
    if (!status.ok())
        return status;
    /* status = Snapshots::GetInstance().GetSnapshot(ss, prev_ss_->GetCollectionId(), ids_.back()); */
    ss = context_.latest_ss;
    return status;
}

Status
Operations::ApplyToStore(Store& store) {
    if (GetType() == OperationsType::W_Compound) {
        /* std::cout << ToString() << std::endl; */
    }
    if (done_) {
        Done(store);
        return status_;
    }
    auto status = OnExecute(store);
    SetStatus(status);
    Done(store);
    return status_;
}

Status
Operations::OnSnapshotDropped() {
    return Status::OK();
}

Status
Operations::OnSnapshotStale() {
    /* std::cout << GetRepr() << " Stale SS " << prev_ss_->GetID() << " RefCnt=" << prev_ss_->RefCnt() \ */
    /*     << " Curr SS " << context_.prev_ss->GetID() << " RefCnt=" << context_.prev_ss->RefCnt() << std::endl; */
    return Status::OK();
}

Status
Operations::OnExecute(Store& store) {
    auto status = PreExecute(store);
    if (!status.ok()) {
        return status;
    }
    status = DoExecute(store);
    if (!status.ok()) {
        return status;
    }
    return PostExecute(store);
}

Status
Operations::PreExecute(Store& store) {
    Status status;
    if (GetStartedSS() && type_ == OperationsType::W_Compound) {
        Snapshots::GetInstance().GetSnapshot(context_.prev_ss, GetStartedSS()->GetCollectionId());
        if (!context_.prev_ss) {
            status = OnSnapshotDropped();
        } else if (prev_ss_->GetID() != context_.prev_ss->GetID()) {
            status = OnSnapshotStale();
        }
    }
    return status;
}

Status
Operations::DoExecute(Store& store) {
    return Status::OK();
}

Status
Operations::PostExecute(Store& store) {
    return store.DoCommitOperation(*this);
}

Status
Operations::RollBack() {
    // TODO: Implement here
    // Spwarn a rollback operation or re-use this operation
    return Status::OK();
}

Status
Operations::ApplyRollBack(Store& store) {
    // TODO: Implement rollback to remove all resources in steps_
    return Status::OK();
}

Operations::~Operations() {
    // TODO: Prefer to submit a rollback operation if status is not ok
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
