#include "Operations.h"
#include "Snapshots.h"

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
