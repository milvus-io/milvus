// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include "mmap/TakeContext.h"

#include <algorithm>
#include <bit>
#include <deque>
#include <memory_resource>
#include <unordered_map>

namespace milvus {
namespace {
constexpr size_t kDirectoryBudget = 1UL << 20;

template <typename Left, typename Right>
bool
SameOwner(const Left& left, const Right& right) {
    return !left.owner_before(right) && !right.owner_before(left);
}

// Only directory allocation REQUEST bytes are charged here, not malloc
// headers/RSS, retained payload or the list of physical cohort owners.
class DirectoryResource final : public std::pmr::memory_resource {
 public:
    explicit DirectoryResource(size_t object_bytes) : used_(object_bytes) {
        AssertInfo(object_bytes < kDirectoryBudget,
                   "take directory object is too large");
    }

 private:
    void*
    do_allocate(size_t bytes, size_t alignment) override {
        if (bytes > kDirectoryBudget - used_) {
            throw std::bad_alloc();
        }
        auto* result =
            std::pmr::new_delete_resource()->allocate(bytes, alignment);
        used_ += bytes;
        return result;
    }

    void
    do_deallocate(void* pointer, size_t bytes, size_t alignment) override {
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
        used_ -= bytes;
    }

    bool
    do_is_equal(
        const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

    size_t used_;
};
}  // namespace

struct TakeContext::Entry {
    std::shared_ptr<const void> source;
    std::shared_ptr<const TakeCellCohort> cohort;
};

void
TakeBatchOwners::ReportNullOwner() {
    AssertInfo(false, "null take batch owner");
}

// Lookup metadata only. Slots never own an Entry, generation or payload.
// A complete directory miss proves absence; incomplete indexing falls back
// to the existing FIFO owner list rather than causing another physical pin.
class TakeContext::Directory final {
 public:
    using OwnerQueue = std::deque<Entry>;

    struct Slot {
        const Entry* entry{nullptr};
        void* cell{nullptr};
    };

    struct Result {
        Slot slot;
        bool complete{true};
    };

    Directory() : resource_(sizeof(Directory)), sources_(&resource_) {
    }

    const OwnerQueue*
    Owners() const {
        return owners_ ? &*owners_ : nullptr;
    }

    OwnerQueue*
    Owners() {
        return owners_ ? &*owners_ : nullptr;
    }

    OwnerQueue&
    EnsureOwners() {
        if (!owners_) {
            owners_.emplace();
        }
        return *owners_;
    }

    Result
    Find(const TakeCellSource& source, int64_t id) const {
        const SourceDirectory* selected = nullptr;
        if (last_source_ != nullptr &&
            last_identity_ == source.identity.get() &&
            SameOwner(last_source_->epoch, source.identity)) {
            selected = last_source_;
        } else {
            auto found = sources_.find(source.identity.get());
            if (found == sources_.end()) {
                return {{}, !unindexed_sources_};
            }
            if (!SameOwner(found->second.epoch, source.identity)) {
                // An old weak epoch at a reused address is never a hit.
                return {{}, false};
            }
            selected = &found->second;
            last_identity_ = source.identity.get();
            last_source_ = selected;
        }
        if (id >= 0 && static_cast<uint64_t>(id) < selected->dense.size()) {
            return {selected->dense[id], selected->complete};
        }
        auto found = selected->sparse.find(id);
        return {found == selected->sparse.end() ? Slot{} : found->second,
                selected->complete};
    }

    void
    Publish(const TakeCellSource& source,
            const TakeCellCohort& cohort,
            const Entry* entry) {
        SourceDirectory* selected = nullptr;
        auto found = sources_.find(source.identity.get());
        if (found != sources_.end()) {
            selected = &found->second;
            if (!SameOwner(selected->epoch, source.identity)) {
                // Weak epochs allow true ABA without retaining the old source.
                // A control-block collision also makes old entries searchable
                // through the authoritative owner list, never via stale slots.
                unindexed_sources_ = true;
                selected->Reset(source.identity);
                // A/B/A control epochs can still have retained entries whose
                // slots were cleared during the intervening publication.
                // Keep this source conservative until the Directory is reset.
                selected->complete = false;
            }
        } else {
            if (!can_grow_) {
                unindexed_sources_ = true;
                return;
            }
            try {
                selected =
                    &sources_
                         .try_emplace(
                             source.identity.get(), source.identity, &resource_)
                         .first->second;
            } catch (const std::bad_alloc&) {
                can_grow_ = false;
                unindexed_sources_ = true;
                return;
            }
        }
        last_identity_ = source.identity.get();
        last_source_ = selected;
        for (const auto& [id, cell] : cohort.cells) {
            if (!Index(*selected, id, Slot{entry, cell})) {
                selected->complete = false;
            }
        }
    }

    void
    Erase(const TakeCellSource& source,
          const TakeCellCohort& cohort,
          const Entry* entry) {
        auto found = sources_.find(source.identity.get());
        if (found == sources_.end() ||
            !SameOwner(found->second.epoch, source.identity)) {
            return;
        }
        auto& selected = found->second;
        for (const auto& [id, cell] : cohort.cells) {
            Slot* slot = nullptr;
            if (id >= 0 && static_cast<uint64_t>(id) < selected.dense.size()) {
                slot = &selected.dense[id];
            } else {
                auto sparse = selected.sparse.find(id);
                if (sparse != selected.sparse.end()) {
                    slot = &sparse->second;
                }
            }
            if (slot != nullptr && slot->entry == entry) {
                *slot = {};
            }
        }
    }

    void
    Clear() {
        // Borrowers hold cohort owners, never these directory addresses.
        // Release nodes/epochs in place without allocating a second Directory
        // or keeping old entries. Top-level hash buckets may remain.
        last_source_ = nullptr;
        last_identity_ = nullptr;
        sources_.clear();
        can_grow_ = true;
        unindexed_sources_ = false;
        // Lookup/oversized pins never construct this lazy queue. Clear
        // destroys all retained owners and its bookkeeping without allocating
        // a fresh empty deque. Active batches own cohorts, not Entry pointers.
        // A lease deleter's read-only Find also sees a complete empty index.
        owners_.reset();
    }

 private:
    struct SourceDirectory {
        SourceDirectory(const std::shared_ptr<const void>& identity,
                        std::pmr::memory_resource* resource)
            : epoch(identity), dense(resource), sparse(resource) {
        }

        void
        Reset(const std::shared_ptr<const void>& identity) {
            dense.clear();
            sparse.clear();
            epoch = identity;
            complete = true;
        }

        std::weak_ptr<const void> epoch;
        std::pmr::vector<Slot> dense;
        std::pmr::unordered_map<int64_t, Slot> sparse;
        bool complete{true};
    };

    static void
    Assign(Slot& destination, Slot value) {
        AssertInfo(
            destination.cell == nullptr || (destination.entry == value.entry &&
                                            destination.cell == value.cell),
            "duplicate retained take directory cell");
        destination = value;
    }

    bool
    Index(SourceDirectory& source, int64_t id, Slot slot) {
        // A directory acceleration threshold, not a physical ID-domain cap.
        // Larger exact IDs use sparse storage or the owner-list fallback.
        constexpr size_t kDenseSlots = kDirectoryBudget / (8 * sizeof(Slot));
        if (id >= 0 && static_cast<uint64_t>(id) < kDenseSlots) {
            if (static_cast<uint64_t>(id) >= source.dense.size()) {
                const auto required = std::max<size_t>(
                    64, std::bit_ceil(static_cast<size_t>(id) + 1));
                if (required <= source.dense.capacity() || can_grow_) {
                    try {
                        source.dense.resize(required);
                    } catch (const std::bad_alloc&) {
                        can_grow_ = false;
                    }
                }
            }
            if (static_cast<uint64_t>(id) < source.dense.size()) {
                Assign(source.dense[id], slot);
                return true;
            }
        }
        auto found = source.sparse.find(id);
        if (found != source.sparse.end()) {
            Assign(found->second, slot);
            return true;
        }
        if (!can_grow_) {
            return false;
        }
        try {
            auto [position, inserted] = source.sparse.try_emplace(id, slot);
            if (!inserted) {
                Assign(position->second, slot);
            }
            return true;
        } catch (const std::bad_alloc&) {
            can_grow_ = false;
            return false;
        }
    }

    DirectoryResource resource_;
    std::pmr::unordered_map<const void*, SourceDirectory> sources_;
    // Dynamic owner bookkeeping retains the original list's budget scope:
    // blocks/maps use the ordinary allocator, not the lookup PMR1MiB cap.
    // The optional itself is included in sizeof(Directory). End insertions
    // preserve Entry addresses stored by lookup slots even when its map grows.
    // Lazy construction avoids an empty deque's block/map on index-only paths.
    std::optional<OwnerQueue> owners_;
    // unordered_map element addresses survive rehash. Reset changes only the
    // epoch/data at that address; Clear resets these pointers before erasing
    // source entries in place.
    mutable const SourceDirectory* last_source_{nullptr};
    mutable const void* last_identity_{nullptr};
    bool can_grow_{true};
    bool unindexed_sources_{false};
};

TakeContext::TakeContext(TakeContextOptions options)
    : options_(options), directory_(std::make_unique<Directory>()) {
}

TakeContext::~TakeContext() = default;

inline std::pair<const TakeContext::Entry*, void*>
TakeContext::Lookup(const TakeCellSource& source, int64_t id) const {
    const auto indexed = directory_->Find(source, id);
    if (indexed.slot.cell != nullptr) {
        return {indexed.slot.entry, indexed.slot.cell};
    }
    if (indexed.complete) {
        return {};
    }
    const auto* owners = directory_->Owners();
    if (owners == nullptr) {
        return {};
    }
    for (const auto& entry : *owners) {
        if (entry.source.get() != source.identity.get() ||
            !SameOwner(entry.source, source.identity)) {
            continue;
        }
        for (const auto& [cell_id, cell] : entry.cohort->cells) {
            if (cell_id == id) {
                return {&entry, cell};
            }
        }
    }
    return {};
}

std::optional<TakeContext::PinnedCell>
TakeContext::Find(const TakeCellSource& source, int64_t id) {
    const auto [entry, cell] = Lookup(source, id);
    if (cell == nullptr) {
        return std::nullopt;
    }
    // Preserve the original optimized reader's FIFO: reads do not promote.
    ++stats_.cache_hits;
    return PinnedCell{entry->cohort, cell};
}

std::optional<void*>
TakeContext::FindBorrowedCached(const TakeCellSource& source,
                                int64_t id,
                                TakeBatchOwners& owners,
                                BorrowedCellCache& cached) {
    if (cached.context == this && cached.epoch == borrow_epoch_ &&
        cached.id == id && cached.entry != nullptr &&
        cached.entry->source.get() == source.identity.get() &&
        SameOwner(cached.entry->source, source.identity)) {
        owners.Add(cached.entry->cohort);
        ++stats_.cache_hits;
        return cached.cell;
    }
    const auto [entry, cell] = Lookup(source, id);
    if (cell == nullptr) {
        cached = {};
        return std::nullopt;
    }
    owners.Add(entry->cohort);
    cached = {this, borrow_epoch_, id, entry, cell};
    ++stats_.cache_hits;
    return cell;
}

std::optional<void*>
TakeContext::FindBorrowed(const TakeCellSource& source,
                          int64_t id,
                          TakeBatchOwners& owners) {
    const auto [entry, cell] = Lookup(source, id);
    if (cell == nullptr) {
        return std::nullopt;
    }
    owners.Add(entry->cohort);
    ++stats_.cache_hits;
    return cell;
}

std::vector<TakeContext::PinnedCell>
TakeContext::Acquire(const TakeCellSource& source,
                     const std::vector<int64_t>& ids) {
    AssertInfo(source.identity && source.pin && source.project,
               "incomplete take cell source");
    std::vector<PinnedCell> result(ids.size());
    std::vector<int64_t> missing;
    missing.reserve(ids.size());
    std::unordered_map<int64_t, size_t> missing_position;
    missing_position.reserve(ids.size());
    for (size_t i = 0; i < ids.size(); ++i) {
        AssertInfo(ids[i] >= 0, "negative take cell id {}", ids[i]);
        const auto [entry, cell] = Lookup(source, ids[i]);
        if (cell != nullptr) {
            result[i] = {entry->cohort, cell};
            ++stats_.cache_hits;
        } else {
            auto [position, inserted] =
                missing_position.emplace(ids[i], missing.size());
            if (inserted) {
                missing.push_back(ids[i]);
            }
        }
    }
    if (missing.empty()) {
        return result;
    }
    auto cohort = PinAndPublish(source, missing);
    for (size_t i = 0; i < ids.size(); ++i) {
        if (!result[i].cohort) {
            result[i] = {cohort,
                         cohort->cells[missing_position.at(ids[i])].second};
        }
    }
    return result;
}

std::shared_ptr<const TakeCellCohort>
TakeContext::AcquireUniqueMissing(const TakeCellSource& source,
                                  const std::vector<int64_t>& ids) {
    AssertInfo(source.identity && source.pin && source.project,
               "incomplete take cell source");
    if (ids.empty()) {
        return nullptr;
    }
    folly::small_vector<int64_t, 64> sorted(ids.begin(), ids.end());
    std::sort(sorted.begin(), sorted.end());
    AssertInfo(sorted.front() >= 0, "negative take cell id {}", sorted.front());
    AssertInfo(std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end(),
               "duplicate unique-missing take cell id");
    for (const auto id : sorted) {
        AssertInfo(Lookup(source, id).second == nullptr,
                   "unique-missing take cell {} is already retained",
                   id);
    }
    return PinAndPublish(source, ids);
}

std::shared_ptr<const TakeCellCohort>
TakeContext::PinAndPublish(const TakeCellSource& source,
                           const std::vector<int64_t>& ids) {
    // Same-worker reentrant publication is fail-closed, not concurrent-context
    // support. Keep inner valid work; this outer cohort has not mutated state.
    const auto publications_before = stats_.bulk_pins;
    auto cohort = std::make_shared<TakeCellCohort>(source.pin(ids));
    AssertInfo(stats_.bulk_pins == publications_before,
               "take source pin callback published into its context");
    ++stats_.bulk_pins;
    stats_.missing_cells += ids.size();
    AssertInfo(cohort->lease != nullptr,
               "take source returned a null cohort lease");
    AssertInfo(cohort->cells.size() == ids.size(),
               "take source returned an incomplete cell cohort");
    for (size_t i = 0; i < ids.size(); ++i) {
        AssertInfo(cohort->cells[i].first == ids[i] &&
                       cohort->cells[i].second != nullptr,
                   "take source returned a mismatched cell cohort");
    }
    if (cohort->bytes == 0 || cohort->bytes > options_.retained_bytes) {
        return cohort;
    }
    while (stats_.retained_bytes > options_.retained_bytes - cohort->bytes) {
        EvictBack();
    }
    auto& owners = directory_->EnsureOwners();
    owners.push_front({source.identity, cohort});
    const auto* entry = &owners.front();
    try {
        directory_->Publish(source, *cohort, entry);
    } catch (...) {
        directory_->Erase(source, *cohort, entry);
        // Publication has no backend callbacks. Only its new front entry is
        // rolled back; never retain an iterator or erase a middle deque node.
        AssertInfo(!owners.empty() && &owners.front() == entry,
                   "take owner queue changed during publication");
        owners.pop_front();
        throw;
    }
    stats_.retained_bytes += cohort->bytes;
    stats_.peak_retained_bytes =
        std::max(stats_.peak_retained_bytes, stats_.retained_bytes);
    return cohort;
}

void
TakeContext::Clear() {
    ++borrow_epoch_;
    directory_->Clear();
    stats_.retained_bytes = 0;
}

void
TakeContext::EvictBack() {
    auto* owners = directory_->Owners();
    AssertInfo(owners != nullptr && !owners->empty(),
               "take owner queue is unexpectedly empty");
    const auto& entry = owners->back();
    ++borrow_epoch_;
    TakeCellSource source;
    source.identity = entry.source;
    directory_->Erase(source, *entry.cohort, &entry);
    stats_.retained_bytes -= entry.cohort->bytes;
    ++stats_.evicted_cohorts;
    owners->pop_back();
}
}  // namespace milvus
