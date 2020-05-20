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

#include "Operations.h"
#include "Snapshots.h"

namespace milvus {
namespace engine {
namespace snapshot {

Operations::Operations(const OperationContext& context, ScopedSnapshotT prev_ss)
    : context_(context), prev_ss_(prev_ss) {}

Operations::Operations(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id) :
    context_(context), prev_ss_(Snapshots::GetInstance().GetSnapshot(collection_id, commit_id)) {
}

void
Operations::operator()() {
    return OnExecute();
}

bool
Operations::IsStale() const {
    auto curr_ss = Snapshots::GetInstance().GetSnapshot(prev_ss_->GetCollectionId());
    if (prev_ss_->GetID() == curr_ss->GetID()) {
        return false;
    }

    return true;
}

ScopedSnapshotT
Operations::GetSnapshot() const {
    //PXU TODO: Check is result ready or valid
    if (ids_.size() == 0) return ScopedSnapshotT();
    return Snapshots::GetInstance().GetSnapshot(prev_ss_->GetCollectionId(), ids_.back());
}

void
Operations::OnExecute() {
    auto r = PreExecute();
    if (!r) {
        status_ = OP_FAIL_FLUSH_META;
        return;
    }
    r = DoExecute();
    if (!r) {
        status_ = OP_FAIL_FLUSH_META;
        return;
    }
    PostExecute();
}

bool
Operations::PreExecute() {
    return true;
}

bool
Operations::DoExecute() {
    return true;
}

bool
Operations::PostExecute() {
    /* std::cout << "Operations " << Name << " is OnExecute with " << steps_.size() << " steps" << std::endl; */
    auto& store = Store::GetInstance();
    auto ok = store.DoCommitOperation(*this);
    if (!ok) status_ = OP_FAIL_FLUSH_META;
    return ok;
}

} // snapshot
} // engine
} // milvus
