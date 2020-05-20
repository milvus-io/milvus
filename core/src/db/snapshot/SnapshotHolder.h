#pragma once
#include "Snapshot.h"

class SnapshotHolder {
public:
    using ScopedPtr = std::shared_ptr<ScopedSnapshotT>;

    SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler = nullptr, size_t num_versions = 1);

    ID_TYPE GetID() const { return collection_id_; }
    bool Add(ID_TYPE id);

    void BackgroundGC();

    void NotifyDone();

    ScopedSnapshotT GetSnapshot(ID_TYPE id = 0, bool scoped = true);

    void GCHandlerTestCallBack(Snapshot::Ptr ss) {
        std::unique_lock<std::mutex> lock(gcmutex_);
        to_release_.push_back(ss);
        lock.unlock();
        cv_.notify_one();
    }

    bool SetGCHandler(GCHandler gc_handler) {
        gc_handler_ = gc_handler;
    }

private:
    void LoadNoLock(ID_TYPE collection_commit_id);
    bool AddNoLock(ID_TYPE id);

    void ReadyForRelease(Snapshot::Ptr ss) {
        if (gc_handler_) {
            gc_handler_(ss);
        }
    }

    std::mutex mutex_;
    std::mutex gcmutex_;
    std::condition_variable cv_;
    ID_TYPE collection_id_;
    ID_TYPE min_id_ = std::numeric_limits<ID_TYPE>::max();
    ID_TYPE max_id_ = std::numeric_limits<ID_TYPE>::min();
    std::map<ID_TYPE, Snapshot::Ptr> active_;
    std::vector<Snapshot::Ptr> to_release_;
    size_t num_versions_ = 1;
    GCHandler gc_handler_;
    std::atomic<bool> done_;
};

using SnapshotHolderPtr = std::shared_ptr<SnapshotHolder>;
