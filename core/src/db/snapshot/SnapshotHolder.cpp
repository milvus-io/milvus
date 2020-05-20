#include "SnapshotHolder.h"
#include "Store.h"
#include "ResourceHolders.h"

SnapshotHolder::SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler, size_t num_versions)
    : collection_id_(collection_id),
      num_versions_(num_versions),
      gc_handler_(gc_handler),
      done_(false) {
}

ScopedSnapshotT
SnapshotHolder::GetSnapshot(ID_TYPE id, bool scoped) {
    std::unique_lock<std::mutex> lock(mutex_);
    /* std::cout << "Holder " << collection_id_ << " actives num=" << active_.size() */
    /*     << " latest=" << active_[max_id_]->GetID() << " RefCnt=" << active_[max_id_]->RefCnt() <<  std::endl; */
    if (id == 0 || id == max_id_) {
        auto ss = active_[max_id_];
        return ScopedSnapshotT(ss, scoped);
    }
    if (id < min_id_) {
        return ScopedSnapshotT();
    }

    if (id > max_id_) {
        LoadNoLock(id);
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        return ScopedSnapshotT();
    }
    return ScopedSnapshotT(it->second, scoped);
}

bool SnapshotHolder::Add(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return AddNoLock(id);
}

bool
SnapshotHolder::AddNoLock(ID_TYPE id) {
    {
        if (active_.size() > 0 && id < max_id_) {
            return false;
        }
    }
    Snapshot::Ptr oldest_ss;
    {
        auto ss = std::make_shared<Snapshot>(id);

        if (done_) { return false; };
        ss->RegisterOnNoRefCB(std::bind(&Snapshot::UnRefAll, ss));
        ss->Ref();
        auto it = active_.find(id);
        if (it != active_.end()) {
            return false;
        }

        if (min_id_ > id) {
            min_id_ = id;
        }

        if (max_id_ < id) {
            max_id_ = id;
        }

        active_[id] = ss;
        if (active_.size() <= num_versions_)
            return true;

        auto oldest_it = active_.find(min_id_);
        oldest_ss = oldest_it->second;
        active_.erase(oldest_it);
        min_id_ = active_.begin()->first;
    }
    ReadyForRelease(oldest_ss); // TODO: Use different mutex
    return true;
}

void
SnapshotHolder::NotifyDone() {
    std::unique_lock<std::mutex> lock(gcmutex_);
    done_ = true;
    cv_.notify_all();
}

void
SnapshotHolder::BackgroundGC() {
    while (true) {
        if (done_.load(std::memory_order_acquire)) {
            break;
        }
        std::vector<Snapshot::Ptr> sss;
        {
            std::unique_lock<std::mutex> lock(gcmutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {return to_release_.size() > 0;});
            if (to_release_.size() > 0) {
                /* std::cout << "size = " << to_release_.size() << std::endl; */
                sss = to_release_;
                to_release_.clear();
            }
        }

        for (auto& ss : sss) {
            ss->UnRef();
            /* std::cout << "BG Handling " << ss->GetID() << " RefCnt=" << ss->RefCnt() << std::endl; */
        }

    }
}

void
SnapshotHolder::LoadNoLock(ID_TYPE collection_commit_id) {
    assert(collection_commit_id > max_id_);
    auto entry = Store::GetInstance().GetResource<CollectionCommit>(collection_commit_id);
    if (!entry) return;
    AddNoLock(collection_commit_id);
}
